"""SUUMO検索結果 検証プログラム

SUUMOフリーワード検索で取得した掲載が、本当に検索対象の建物かを検証する。
各掲載の詳細ページを訪問し、実際の建物名・住所と期待値を比較して判定する。

Usage:
    python verify_listings.py                    # 全アクティブSUUMO掲載を検証
    python verify_listings.py --ward 中央区      # 区で絞り込み
    python verify_listings.py --limit 10         # テスト用に件数制限
    python verify_listings.py --building "グランド"  # 建物名で絞り込み
    python verify_listings.py --update-db        # 結果をDBに保存
    python verify_listings.py --skip-verified    # 検証済みをスキップ
"""

import argparse
import asyncio
import csv
import difflib
import logging
import re
import sys
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# PYTHONPATH に自身のディレクトリを追加
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import OUTPUT_DIR, LOG_DIR
from models.database import (
    init_db, get_db,
    get_active_suumo_listings, get_unverified_suumo_listings,
    upsert_verification,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


# =============================================================================
# データクラス
# =============================================================================

@dataclass
class VerificationResult:
    """検証結果"""
    listing_id: int
    building_id: int
    ward: str
    expected_name: str
    actual_name: str
    name_score: float
    expected_address: str
    actual_address: str
    ward_match: bool
    address_match: bool
    status: str        # match / suspicious / mismatch / error
    reason: str
    listing_url: str


# =============================================================================
# 漢数字→算用数字 変換
# =============================================================================

_KANJI_DIGIT = {
    "〇": 0, "零": 0,
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
}


def _kanji_to_int(kanji: str) -> Optional[int]:
    """漢数字文字列を整数に変換する（百の位まで対応）

    2つの表記法に対応:
    - 位取り式: 十二→12, 二十四→24, 百二十三→123
    - 桁並び式: 一二→12, 二〇→20, 一二三→123（SUUMOなどで使用）
    """
    if not kanji:
        return None

    has_positional = "十" in kanji or "百" in kanji

    if has_positional:
        # 位取り式: 十、百を位取りとして解釈
        result = 0
        current = 0
        for ch in kanji:
            if ch in _KANJI_DIGIT:
                current = _KANJI_DIGIT[ch]
            elif ch == "十":
                if current == 0:
                    current = 1
                result += current * 10
                current = 0
            elif ch == "百":
                if current == 0:
                    current = 1
                result += current * 100
                current = 0
            else:
                return None
        result += current
        return result if result > 0 else None
    else:
        # 桁並び式: 各文字を1桁として連結（一二→12, 二〇→20）
        digits = []
        for ch in kanji:
            if ch in _KANJI_DIGIT:
                digits.append(str(_KANJI_DIGIT[ch]))
            else:
                return None
        if not digits:
            return None
        return int("".join(digits))


def _kanji_number_replacer(match: re.Match) -> str:
    """正規表現マッチから漢数字部分を算用数字に変換"""
    kanji_part = match.group(1)
    suffix = match.group(2)
    num = _kanji_to_int(kanji_part)
    if num is not None:
        return f"{num}{suffix}"
    return match.group(0)  # 変換できない場合はそのまま


def normalize_address(address: str) -> str:
    """住所を正規化する（漢数字→算用数字変換を含む）

    変換対象: 条、丁目、番地、番、号 の前の漢数字
    例: 南一条西五丁目 → 南1条西5丁目
        北二十四条 → 北24条
    """
    if not address:
        return ""

    # NFKC正規化（全角数字→半角数字など）
    address = unicodedata.normalize("NFKC", address)

    # 漢数字→算用数字 (条・丁目・番地・番・号の前)
    address = re.sub(
        r"([一二三四五六七八九十百〇零]+)(条|丁目|番地|番|号)",
        _kanji_number_replacer,
        address,
    )

    # 末尾や区切り位置の漢数字も変換（例: 「西一二」→「西12」）
    address = re.sub(
        r"(?<=[^\d一二三四五六七八九十百〇零])([一二三四五六七八九十百〇零]+)$",
        lambda m: str(_kanji_to_int(m.group(1))) if _kanji_to_int(m.group(1)) else m.group(0),
        address,
    )

    # 連続スペースを統一
    address = re.sub(r"\s+", " ", address).strip()

    return address


# =============================================================================
# 名前比較ロジック
# =============================================================================

def normalize_name(name: str) -> str:
    """建物名を正規化（比較用）"""
    if not name:
        return ""
    # NFKC正規化
    name = unicodedata.normalize("NFKC", name)
    # 空白・記号を除去
    name = re.sub(r"[\s　・\-\(\)（）]", "", name)
    return name.lower()


def compare_names(expected: str, actual: str) -> float:
    """建物名の類似度を算出 (0.0 - 1.0)"""
    if not expected or not actual:
        return 0.0

    ne = normalize_name(expected)
    na = normalize_name(actual)

    # 完全一致
    if ne == na:
        return 1.0

    # 包含チェック（短い方が4文字以上かつ長い方の60%以上）
    shorter = min(len(ne), len(na))
    longer = max(len(ne), len(na))
    if shorter >= 4 and shorter / longer >= 0.6 and (ne in na or na in ne):
        return 0.95

    # SequenceMatcher で類似度
    return difflib.SequenceMatcher(None, ne, na).ratio()


# =============================================================================
# 住所比較ロジック
# =============================================================================

def extract_ward(address: str) -> Optional[str]:
    """住所から区名を抽出（例: "中央区", "北区"）"""
    # 札幌市の10区に限定してマッチ
    m = re.search(r"(中央区|北区|東区|白石区|豊平区|南区|西区|厚別区|手稲区|清田区)", address)
    return m.group(1) if m else None


def extract_area(address: str) -> Optional[str]:
    """住所から区より後の町域部分を抽出

    例: "北海道札幌市中央区南1条西5丁目1-10" → "南1条西5丁目"
         "北海道札幌市中央区南3条西12" → "南3条西12"
    """
    # 区の後ろを取得
    m = re.search(r"区(.+)", address)
    if not m:
        return None
    rest = m.group(1).strip()
    # 番地・番号部分を除去（数字-数字 のパターン以降を除く）
    rest = re.sub(r"\d+[-ー]\d+.*$", "", rest)
    return rest.strip() if rest.strip() else None


def compare_addresses(expected_ward: str, expected_address: str,
                      actual_address: str) -> tuple[bool, bool]:
    """住所を比較して (ward_match, area_match) を返す

    比較前に normalize_address を適用する。
    """
    if not actual_address:
        return False, False

    norm_expected = normalize_address(expected_address)
    norm_actual = normalize_address(actual_address)

    # 区の比較
    actual_ward = extract_ward(norm_actual)
    ward_match = False
    if actual_ward and expected_ward:
        ward_match = actual_ward == expected_ward

    # 町域の比較
    expected_area = extract_area(norm_expected)
    actual_area = extract_area(norm_actual)
    area_match = False
    if expected_area and actual_area:
        ea = re.sub(r"\s", "", expected_area)
        aa = re.sub(r"\s", "", actual_area)
        # 完全包含チェック
        if ea in aa or aa in ea:
            area_match = True
        else:
            # 「条」部分の一致チェック（例: 南3条 が両方に含まれる）
            ea_jou = re.search(r"(.+?\d+条)", ea)
            aa_jou = re.search(r"(.+?\d+条)", aa)
            if ea_jou and aa_jou and ea_jou.group(1) == aa_jou.group(1):
                area_match = True

    return ward_match, area_match


# =============================================================================
# 判定ロジック
# =============================================================================

def determine_status(name_score: float, ward_match: bool,
                     area_match: bool) -> tuple[str, str]:
    """検証ステータスと理由を判定"""
    # 区が不一致 → mismatch
    if not ward_match:
        return "mismatch", f"区不一致 (name_score={name_score:.2f})"

    # 名前スコアが高い + 住所も一致
    if name_score >= 0.8 and area_match:
        return "match", f"名前・住所一致 (name_score={name_score:.2f})"

    # 名前スコアが高いが住所は区のみ一致
    if name_score >= 0.8:
        return "suspicious", f"名前一致だが町域不一致 (name_score={name_score:.2f})"

    # 名前スコアが中程度
    if name_score >= 0.5:
        return "suspicious", f"名前類似 (name_score={name_score:.2f})"

    # 名前スコアが低い
    return "mismatch", f"名前不一致 (name_score={name_score:.2f})"


# =============================================================================
# SUUMO詳細ページスクレイパー
# =============================================================================

class VerificationScraper(BaseScraper):
    """SUUMO掲載詳細ページから建物名・住所を取得するスクレイパー"""

    site_name = "suumo"

    async def search(self, building_name: str, ward: str) -> list:
        """検証スクレイパーでは未使用（BaseScraper の抽象メソッド実装）"""
        return []

    async def scrape_listing_detail(self, url: str) -> Optional[dict]:
        """SUUMO詳細ページから建物名と住所を抽出"""
        page = await self._new_page()
        try:
            if not await self._safe_goto(page, url):
                logger.warning(f"Failed to load: {url}")
                return None

            await page.wait_for_timeout(2000)

            # 404 / リダイレクトチェック
            body_text = await page.inner_text("body")
            if "ページが見つかりません" in body_text or "削除されました" in body_text:
                return None

            actual_name = ""
            actual_address = ""

            # --- 建物名の取得 ---
            # 優先順位: section_h1 > h1 > title
            for selector in [
                ".section_h1-header-title",
                ".property_view_main-title",
                "h1",
            ]:
                el = await page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    if text and len(text) < 100:
                        actual_name = text
                        break

            if not actual_name:
                title = await page.title()
                if title:
                    # "物件名 - SUUMO" → "物件名"
                    actual_name = re.sub(r"\s*[-|｜].*(SUUMO|suumo).*$", "", title).strip()

            # --- 住所の取得 ---
            # パターン1: テーブルの「所在地」行
            th_elements = await page.query_selector_all("th")
            for th in th_elements:
                th_text = (await th.inner_text()).strip()
                if "所在地" in th_text:
                    td = await th.evaluate_handle(
                        "el => el.closest('tr')?.querySelector('td') || el.nextElementSibling"
                    )
                    if td:
                        actual_address = (await td.inner_text()).strip()
                        break

            # パターン2: detailbox内のテキスト
            if not actual_address:
                for selector in [
                    ".detailbox-property-col",
                    ".property_view_detail-text",
                ]:
                    els = await page.query_selector_all(selector)
                    for el in els:
                        text = (await el.inner_text()).strip()
                        if re.search(r"札幌市.+区", text):
                            actual_address = text
                            break
                    if actual_address:
                        break

            # パターン3: ページ全体から住所パターンを探す
            if not actual_address:
                m = re.search(r"北海道札幌市[\u4e00-\u9fff]+区[^\n\r<]{3,50}", body_text)
                if m:
                    actual_address = m.group(0).strip()

            # 改行やタブを除去
            actual_name = re.sub(r"[\n\r\t]+", " ", actual_name).strip()
            actual_address = re.sub(r"[\n\r\t]+", " ", actual_address).strip()

            return {
                "actual_name": actual_name,
                "actual_address": actual_address,
            }

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
        finally:
            await page.close()


# =============================================================================
# メイン検証処理
# =============================================================================

async def verify_all(listings: list, scraper: VerificationScraper) -> list[VerificationResult]:
    """全掲載を検証"""
    results = []

    for i, row in enumerate(listings):
        listing_id = row["listing_id"]
        url = row["listing_url"]
        expected_name = row["building_name"]
        expected_address = row["address_base"]
        expected_ward = row["ward"]

        logger.info(
            f"[{i+1}/{len(listings)}] 検証中: {expected_name} ({expected_ward}) - {url}"
        )

        detail = await scraper.scrape_listing_detail(url)

        if detail is None:
            results.append(VerificationResult(
                listing_id=listing_id,
                building_id=row["building_id"],
                ward=expected_ward,
                expected_name=expected_name,
                actual_name="",
                name_score=0.0,
                expected_address=expected_address,
                actual_address="",
                ward_match=False,
                address_match=False,
                status="error",
                reason="ページ取得失敗または削除済み",
                listing_url=url,
            ))
            continue

        actual_name = detail["actual_name"]
        actual_address = detail["actual_address"]

        # 名前比較
        name_score = compare_names(expected_name, actual_name)

        # 住所比較
        ward_match, area_match = compare_addresses(
            expected_ward, expected_address, actual_address
        )

        # 判定
        status, reason = determine_status(name_score, ward_match, area_match)

        result = VerificationResult(
            listing_id=listing_id,
            building_id=row["building_id"],
            ward=expected_ward,
            expected_name=expected_name,
            actual_name=actual_name,
            name_score=round(name_score, 3),
            expected_address=expected_address,
            actual_address=actual_address,
            ward_match=ward_match,
            address_match=area_match,
            status=status,
            reason=reason,
            listing_url=url,
        )
        results.append(result)

        if status != "match":
            logger.warning(
                f"  [{status.upper()}] {reason} | "
                f"expected={expected_name} actual={actual_name}"
            )
        else:
            logger.info(f"  [MATCH] {reason}")

    return results


# =============================================================================
# 出力
# =============================================================================

def export_csv(results: list[VerificationResult], output_dir: Path) -> Path:
    """検証結果をCSVに出力"""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"verification_{timestamp}.csv"

    fieldnames = [
        "listing_id", "building_id", "ward",
        "expected_name", "actual_name", "name_score",
        "expected_address", "actual_address",
        "ward_match", "address_match",
        "status", "reason", "listing_url",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    return csv_path


def print_summary(results: list[VerificationResult]):
    """検証結果サマリーをコンソールに出力"""
    total = len(results)
    if total == 0:
        logger.info("検証対象の掲載がありません")
        return

    counts = {"match": 0, "suspicious": 0, "mismatch": 0, "error": 0}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    logger.info("=" * 50)
    logger.info("=== 検証完了 ===")
    logger.info(
        f"検証件数: {total} | "
        f"match: {counts['match']} | "
        f"suspicious: {counts['suspicious']} | "
        f"mismatch: {counts['mismatch']} | "
        f"error: {counts['error']}"
    )
    logger.info("=" * 50)

    # mismatch/suspicious の詳細を表示
    problems = [r for r in results if r.status in ("mismatch", "suspicious")]
    if problems:
        logger.info("")
        logger.info("--- 要確認リスト ---")
        for r in problems:
            logger.info(
                f"  [{r.status.upper()}] {r.expected_name} ({r.ward})"
            )
            logger.info(
                f"    期待: {r.expected_name} / {r.expected_address}"
            )
            logger.info(
                f"    実際: {r.actual_name} / {r.actual_address}"
            )
            logger.info(
                f"    スコア: {r.name_score:.3f} | {r.reason}"
            )
            logger.info(f"    URL: {r.listing_url}")
            logger.info("")


# =============================================================================
# CLI
# =============================================================================

def setup_logging():
    """ロギング設定"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"verify_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_file), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="SUUMO検索結果 検証プログラム")
    parser.add_argument("--ward", type=str, default=None, help="区名で絞り込み")
    parser.add_argument("--limit", type=int, default=None, help="検証件数の上限（テスト用）")
    parser.add_argument("--building", type=str, default=None, help="建物名で絞り込み（部分一致）")
    parser.add_argument("--update-db", action="store_true", help="検証結果をDBに保存")
    parser.add_argument("--skip-verified", action="store_true", help="検証済みをスキップ")
    args = parser.parse_args()

    setup_logging()
    logger.info("=== SUUMO検索結果 検証開始 ===")

    # DB初期化（テーブル作成）
    init_db()

    # 検証対象の取得
    with get_db() as conn:
        if args.skip_verified:
            listings = get_unverified_suumo_listings(
                conn, ward=args.ward, building_name=args.building
            )
        else:
            listings = get_active_suumo_listings(
                conn, ward=args.ward, building_name=args.building
            )

    logger.info(f"検証対象: {len(listings)} 件")

    if args.limit:
        listings = listings[:args.limit]
        logger.info(f"上限適用: {len(listings)} 件")

    if not listings:
        logger.info("検証対象がありません。終了します。")
        return

    # 検証実行
    async def run():
        scraper = VerificationScraper()
        async with scraper:
            return await verify_all(listings, scraper)

    results = asyncio.run(run())

    # CSV出力
    csv_path = export_csv(results, OUTPUT_DIR)
    logger.info(f"CSV出力: {csv_path}")

    # DB保存（オプション）
    if args.update_db:
        with get_db() as conn:
            for r in results:
                upsert_verification(
                    conn, r.listing_id,
                    r.actual_name, r.actual_address,
                    r.name_score, r.address_match,
                    r.status, r.reason,
                )
        logger.info("検証結果をDBに保存しました")

    # サマリー表示
    print_summary(results)
    logger.info("=== 検証完了 ===")


if __name__ == "__main__":
    main()
