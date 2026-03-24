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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import NOTION_API_KEY, NOTION_DATABASE_ID
from models.database import get_db, get_all_active_listings

logger = logging.getLogger(__name__)

NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
RATE_LIMIT_DELAY = 0.35  # Notion API: 3 requests/second


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

                pages[listing_url] = {
                    "page_id": page["id"],
                    "status": status,
                    "rent": rent,
                }

        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return pages


def _build_properties(row) -> dict:
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

    return props


def create_page(client: httpx.Client, row) -> str:
    """Notionに新規ページを作成"""
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": _build_properties(row),
    }

    _rate_limit()
    resp = client.post(f"{NOTION_API_URL}/pages", json=payload)
    if resp.status_code != 200:
        logger.error(f"Notion create_page failed: {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json()["id"]


def update_page(client: httpx.Client, page_id: str, row) -> None:
    """既存のNotionページを更新"""
    payload = {"properties": _build_properties(row)}

    _rate_limit()
    resp = client.patch(f"{NOTION_API_URL}/pages/{page_id}", json=payload)
    if resp.status_code != 200:
        logger.error(f"Notion update_page failed: {resp.status_code} {resp.text[:300]}")
        resp.raise_for_status()


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

    # DBからアクティブ掲載を取得
    with get_db() as conn:
        active_listings = get_all_active_listings(conn)
    logger.info(f"アクティブ掲載数: {len(active_listings)}")

    client = httpx.Client(headers=_headers(), timeout=30)

    try:
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

            try:
                if url in existing_pages:
                    page_info = existing_pages[url]
                    # ステータスが Inactive だった場合、または家賃が変わった場合に更新
                    needs_update = (
                        page_info["status"] != "Active"
                        or page_info["rent"] != row["rent_price"]
                    )
                    if needs_update:
                        update_page(client, page_info["page_id"], row)
                        updated += 1
                        logger.debug(f"更新: {row['building_name']} ({url})")
                else:
                    create_page(client, row)
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
