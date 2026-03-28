"""Notion データベース同期モジュール

SQLiteのアクティブ掲載データをNotionデータベースに同期する。
- 新規掲載 → Notionページ作成
- 既存掲載（家賃等変更） → ページ更新
- 掲載終了 → ステータスを「Inactive」に更新

Usage:
    python notion_sync.py
"""

import logging
import os
import sys
import time
from pathlib import Path

import httpx
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import NOTION_API_KEY, NOTION_DATABASE_ID
from models.database import get_db, get_all_active_listings, get_evaluation_scores_dict

logger = logging.getLogger(__name__)

# 評価エンジンのパスを追加
EVAL_MODULE_DIR = Path(__file__).resolve().parent.parent / "07_property_evaluation"

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
RATE_LIMIT_DELAY = 0.35  # Notion API: 3 requests/second


def load_evaluation_scores() -> dict[str, dict]:
    """評価スコアを読み込み、{listing_url: score_data} を返す。

    優先順位:
    1. rental_search.db の evaluation_scores テーブル（高速・最新）
    2. Excel フォールバック（DBにデータが無い場合）
    """
    # ── 1. DBから読み込み（優先） ──
    try:
        with get_db() as conn:
            scores = get_evaluation_scores_dict(conn)
        if scores:
            logger.info(f"評価スコアをDBから {len(scores)} 件読み込みました。")
            return scores
    except Exception as e:
        logger.debug(f"DB読み込みスキップ: {e}")

    # ── 2. Excelフォールバック ──
    scores: dict[str, dict] = {}
    try:
        eval_output = EVAL_MODULE_DIR / "output"
        if not eval_output.exists():
            logger.info("評価出力ディレクトリが見つかりません。スコアなしで同期します。")
            return scores

        eval_excels = sorted(eval_output.glob("evaluation_*.xlsx"))
        if not eval_excels:
            import subprocess
            eval_script = str(EVAL_MODULE_DIR / "evaluate.py")
            if Path(eval_script).exists():
                logger.info("評価Excel未発見 → evaluate.py を実行")
                subprocess.run(
                    [sys.executable, eval_script],
                    capture_output=True, timeout=120,
                    cwd=str(EVAL_MODULE_DIR),
                )
                eval_excels = sorted(eval_output.glob("evaluation_*.xlsx"))

        if not eval_excels:
            logger.info("評価Excelがありません。スコアなしで同期します。")
            return scores

        df = pd.read_excel(eval_excels[-1], sheet_name="総合ランキング")

        col_map = {
            "総合スコア": "total_score",
            "収益性": "score_profitability",
            "立地": "score_location",
            "需要安定": "score_demand",
            "物件適合": "score_quality",
            "リスク": "score_risk",
            "年間利益": "annual_profit",
            "ROI(%)": "annual_roi",
            "平均ADR": "weighted_avg_adr",
        }

        for _, row in df.iterrows():
            url = row.get("掲載URL", "")
            if not url or pd.isna(url):
                continue
            score_data = {}
            for excel_col, code_key in col_map.items():
                val = row.get(excel_col)
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    score_data[code_key] = float(val)
            scores[str(url)] = score_data

        logger.info(f"評価スコアをExcelから {len(scores)} 件読み込みました（フォールバック）。")
    except Exception as e:
        logger.warning(f"評価スコアの読み込みに失敗しました: {e}")
    return scores


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _rate_limit():
    time.sleep(RATE_LIMIT_DELAY)


def query_all_pages(client: httpx.Client) -> dict[str, dict]:
    """Notionデータベースの全ページを取得し、{listing_url: {page_id, status, rent}} を返す"""
    pages = {}
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        _rate_limit()
        resp = client.post(
            f"{NOTION_API_URL}/databases/{NOTION_DATABASE_ID}/query",
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            props = page["properties"]
            # URL プロパティから listing_url を取得
            url_prop = props.get("userDefined:URL", {})
            listing_url = url_prop.get("url", "")
            if listing_url:
                # 家賃を取得（変更検知用）
                rent_prop = props.get("家賃（円）", {})
                rent = rent_prop.get("number")
                # ステータスを取得
                status_prop = props.get("ステータス", {})
                status_select = status_prop.get("select")
                status = status_select.get("name", "") if status_select else ""
                # 総合スコアを取得（変更検知用）
                score_prop = props.get("総合スコア", {})
                total_score = score_prop.get("number")

                pages[listing_url] = {
                    "page_id": page["id"],
                    "status": status,
                    "rent": rent,
                    "total_score": total_score,
                }

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return pages


def _build_properties(row, score_data: dict | None = None) -> dict:
    """SQLite行からNotionプロパティを構築"""
    props = {
        "建物名": {"title": [{"text": {"content": row["building_name"] or ""}}]},
        "ステータス": {"select": {"name": "Active"}},
    }

    if row["ward"]:
        props["区"] = {"select": {"name": row["ward"]}}

    if row["address_base"]:
        props["住所"] = {"rich_text": [{"text": {"content": row["address_base"]}}]}

    if row["site_name"]:
        props["サイト"] = {"select": {"name": row["site_name"]}}

    if row["rent_price"] is not None:
        props["家賃（円）"] = {"number": row["rent_price"]}

    if row["management_fee"] is not None:
        props["管理費（円）"] = {"number": row["management_fee"]}

    if row["floor_plan"]:
        props["間取り"] = {"select": {"name": row["floor_plan"]}}

    if row["area_sqm"] is not None:
        props["面積（㎡）"] = {"number": row["area_sqm"]}

    if row["nearest_station"]:
        props["最寄駅"] = {"rich_text": [{"text": {"content": row["nearest_station"]}}]}

    if row["walk_minutes"] is not None:
        props["徒歩（分）"] = {"number": row["walk_minutes"]}

    if row["listing_url"]:
        props["userDefined:URL"] = {"url": row["listing_url"]}

    if row["listing_title"]:
        props["物件タイトル"] = {"rich_text": [{"text": {"content": str(row["listing_title"])[:2000]}}]}

    if row["deposit"]:
        props["敷金"] = {"rich_text": [{"text": {"content": str(row["deposit"])}}]}

    if row["key_money"]:
        props["礼金"] = {"rich_text": [{"text": {"content": str(row["key_money"])}}]}

    if row["building_age"]:
        props["築年数"] = {"rich_text": [{"text": {"content": str(row["building_age"])}}]}

    if row["floor_number"]:
        props["階数"] = {"rich_text": [{"text": {"content": str(row["floor_number"])}}]}

    if row["first_seen_at"]:
        props["初回検出日"] = {"date": {"start": row["first_seen_at"][:10]}}

    if row["last_seen_at"]:
        props["最終確認日"] = {"date": {"start": row["last_seen_at"][:10]}}

    # 評価スコアを追加（score_dataがある場合のみ）
    if score_data:
        score_property_map = {
            "total_score": "総合スコア",
            "score_profitability": "収益性スコア",
            "score_location": "立地スコア",
            "score_demand": "需要スコア",
            "score_quality": "品質スコア",
            "score_risk": "リスクスコア",
            "annual_profit": "年間利益（円）",
            "annual_roi": "年間ROI（%）",
            "weighted_avg_adr": "推定ADR（円）",
        }
        for key, notion_name in score_property_map.items():
            val = score_data.get(key)
            if val is not None:
                props[notion_name] = {"number": round(val, 2)}

    return props


def create_page(client: httpx.Client, row, score_data: dict | None = None) -> str:
    """Notionに新規ページを作成"""
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": _build_properties(row, score_data),
    }

    _rate_limit()
    resp = client.post(f"{NOTION_API_URL}/pages", json=payload)
    if resp.status_code != 200:
        logger.error(f"Notion create_page failed: {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json()["id"]


def update_page(client: httpx.Client, page_id: str, row, score_data: dict | None = None) -> None:
    """既存のNotionページを更新"""
    payload = {"properties": _build_properties(row, score_data)}

    _rate_limit()
    resp = client.patch(f"{NOTION_API_URL}/pages/{page_id}", json=payload)
    if resp.status_code != 200:
        logger.error(f"Notion update_page failed: {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()


# 評価スコア用のNotionプロパティ定義
SCORE_PROPERTIES = {
    "総合スコア": "number",
    "収益性スコア": "number",
    "立地スコア": "number",
    "需要スコア": "number",
    "品質スコア": "number",
    "リスクスコア": "number",
    "年間利益（円）": "number",
    "年間ROI（%）": "number",
    "推定ADR（円）": "number",
}


def ensure_score_properties(client: httpx.Client) -> None:
    """Notionデータベースに評価スコア用プロパティが無ければ追加する。"""
    _rate_limit()
    resp = client.get(f"{NOTION_API_URL}/databases/{NOTION_DATABASE_ID}")
    if resp.status_code != 200:
        logger.warning(f"DB schema取得失敗: {resp.status_code}")
        return

    existing_props = set(resp.json().get("properties", {}).keys())
    missing = {k: v for k, v in SCORE_PROPERTIES.items() if k not in existing_props}

    if not missing:
        logger.info("評価スコアプロパティは既に存在します")
        return

    # 不足プロパティを一括追加
    props_payload = {}
    for name, prop_type in missing.items():
        props_payload[name] = {prop_type: {}}

    _rate_limit()
    resp = client.patch(
        f"{NOTION_API_URL}/databases/{NOTION_DATABASE_ID}",
        json={"properties": props_payload},
    )
    if resp.status_code == 200:
        logger.info(f"評価スコアプロパティを追加: {list(missing.keys())}")
    else:
        logger.error(f"プロパティ追加失敗: {resp.status_code} {resp.text[:300]}")


def mark_inactive(client: httpx.Client, page_id: str) -> None:
    """ページのステータスを Inactive に変更"""
    payload = {
        "properties": {
            "ステータス": {"select": {"name": "Inactive"}},
        }
    }

    _rate_limit()
    resp = client.patch(f"{NOTION_API_URL}/pages/{page_id}", json=payload)
    resp.raise_for_status()


def sync():
    """メイン同期処理"""
    if not NOTION_API_KEY or not NOTION_DATABASE_ID:
        logger.warning("NOTION_API_KEY または NOTION_DATABASE_ID が未設定。Notion同期をスキップ")
        return

    logger.info("=== Notion同期開始 ===")

    # 評価スコアを読み込み
    try:
        eval_scores = load_evaluation_scores()
    except Exception as e:
        logger.warning(f"評価スコア読み込みでエラー: {e}。スコアなしで同期します。")
        eval_scores = {}

    # DBからアクティブ掲載を取得
    with get_db() as conn:
        active_listings = get_all_active_listings(conn)
    logger.info(f"アクティブ掲載数: {len(active_listings)}")

    client = httpx.Client(headers=_headers(), timeout=30)

    try:
        # 評価スコアプロパティがDBに無ければ自動追加
        if eval_scores:
            try:
                ensure_score_properties(client)
            except Exception as e:
                logger.warning(f"スコアプロパティ追加失敗: {e}。スコアなしで同期します。")
                eval_scores = {}

        # Notionの既存ページを取得
        logger.info("Notionデータベースをクエリ中...")
        existing_pages = query_all_pages(client)
        logger.info(f"Notion既存ページ数: {len(existing_pages)}")

        # アクティブ掲載のURLセット
        active_urls = set()
        created = 0
        updated = 0

        errors = 0
        for row in active_listings:
            url = row["listing_url"]
            active_urls.add(url)

            score_data = eval_scores.get(url)
            try:
                if url in existing_pages:
                    page_info = existing_pages[url]
                    # ステータスが Inactive だった場合、または家賃が変わった場合に更新
                    needs_update = (
                        page_info["status"] != "Active"
                        or page_info["rent"] != row["rent_price"]
                    )
                    # スコア変更の検知: スコアが未設定、または1点以上変化した場合
                    if score_data and "total_score" in score_data:
                        new_score = score_data["total_score"]
                        existing_score = page_info.get("total_score")
                        if existing_score is None:
                            needs_update = True
                        elif abs(new_score - existing_score) > 1.0:
                            needs_update = True
                    if needs_update:
                        update_page(client, page_info["page_id"], row, score_data)
                        updated += 1
                        logger.debug(f"更新: {row['building_name']} ({url})")
                else:
                    create_page(client, row, score_data)
                    created += 1
                    logger.debug(f"作成: {row['building_name']} ({url})")
            except Exception as e:
                errors += 1
                logger.warning(f"スキップ: {row['building_name']} - {e}")

        # 掲載が消えたページを Inactive に
        deactivated = 0
        for url, page_info in existing_pages.items():
            if url not in active_urls and page_info["status"] == "Active":
                try:
                    mark_inactive(client, page_info["page_id"])
                    deactivated += 1
                except Exception as e:
                    errors += 1
                    logger.warning(f"非アクティブ化失敗: {url} - {e}")

        logger.info(
            f"=== Notion同期完了: 作成={created}, 更新={updated}, "
            f"非アクティブ化={deactivated}, エラー={errors} ==="
        )
    finally:
        client.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    sync()


if __name__ == "__main__":
    main()
