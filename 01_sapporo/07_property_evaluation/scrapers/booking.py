"""Booking.comスクレイパー — 札幌市のバケーションレンタル物件データを区別に収集

Booking.comの検索結果からアパートメント・バケーションホームの情報
（価格、評価、レビュー数等）を取得し、SQLiteに保存する。
収益推計の比較データとして使用する。

使用方法:
    python scrapers/booking.py                    # 全エリア検索
    python scrapers/booking.py --max-pages 2      # ページ数制限
    python scrapers/booking.py --detail           # 詳細スクレイピング
    python scrapers/booking.py --headless false    # ブラウザ表示
"""

import argparse
import asyncio
import logging
import random
import sys
from pathlib import Path

from playwright.async_api import async_playwright

# ── パス設定（直接実行・パッケージ両対応） ──
_SCRAPERS_DIR = Path(__file__).resolve().parent
_EVAL_DIR = _SCRAPERS_DIR.parent
_SAPPORO_DIR = _EVAL_DIR.parent

# shared モジュールへのパス
sys.path.insert(0, str(_SAPPORO_DIR))
from shared.browser import create_browser_context, new_stealth_page

# scrapers パッケージとしても直接実行としても動くようにする
if __name__ == "__main__" or __package__ is None:
    sys.path.insert(0, str(_EVAL_DIR))
    from scrapers.booking_db import BookingListing, init_db, save_listings, get_listings_without_details  # noqa: E402
    from scrapers.booking_utils import parse_price, parse_booking_rating, parse_review_count  # noqa: E402
    from scrapers.booking_search import SEARCH_AREAS, build_search_url, scrape_area, extract_from_json_ld, parse_json_ld_item, extract_from_dom, parse_property_card  # noqa: E402
    from scrapers.booking_detail import scrape_details, extract_detail_from_json_ld, extract_detail_from_dom  # noqa: E402
else:
    from .booking_db import BookingListing, init_db, save_listings, get_listings_without_details
    from .booking_utils import parse_price, parse_booking_rating, parse_review_count
    from .booking_search import SEARCH_AREAS, build_search_url, scrape_area, extract_from_json_ld, parse_json_ld_item, extract_from_dom, parse_property_card
    from .booking_detail import scrape_details, extract_detail_from_json_ld, extract_detail_from_dom

# ── ログ設定 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# =====================================================================
# メイン実行
# =====================================================================

async def run(max_pages: int = 3, headless: bool = True, detail: bool = False):
    """全エリアのスクレイピングを実行"""
    init_db()

    async with async_playwright() as p:
        browser, context = await create_browser_context(
            p, headless=headless, timeout=45000,
            extra_http_headers={"Accept-Language": "ja,en;q=0.9"},
        )
        page = await new_stealth_page(context)

        if detail:
            # ── 詳細スクレイピングモード ──
            logger.info("Detail scraping mode")
            to_scrape = get_listings_without_details(limit=50)
            if not to_scrape:
                logger.info("No listings need detail scraping")
            else:
                logger.info(f"Found {len(to_scrape)} listings to scrape details")
                await scrape_details(page, to_scrape)
        else:
            # ── 検索結果スクレイピングモード ──
            all_listings = []

            for i, area in enumerate(SEARCH_AREAS):
                logger.info(f"\n[{i+1}/{len(SEARCH_AREAS)}] {area['name']}")

                try:
                    area_listings = await scrape_area(page, area, max_pages=max_pages)
                    all_listings.extend(area_listings)

                    if area_listings:
                        save_listings(area_listings)

                    # エリア間のランダム遅延 8-15秒
                    if i < len(SEARCH_AREAS) - 1:
                        delay = random.uniform(8, 15)
                        logger.info(f"  Waiting {delay:.1f}s before next area...")
                        await asyncio.sleep(delay)

                except Exception as e:
                    logger.error(f"  Area error: {e}")
                    continue

            # ── サマリー出力 ──
            logger.info(f"\n{'='*50}")
            logger.info(f"  Total listings: {len(all_listings)}")

            # 区別集計
            ward_counts: dict[str, int] = {}
            for lst in all_listings:
                ward_counts[lst.ward] = ward_counts.get(lst.ward, 0) + 1
            for ward, count in sorted(ward_counts.items(), key=lambda x: -x[1]):
                logger.info(f"  {ward}: {count} listings")

        await browser.close()


def main():
    parser = argparse.ArgumentParser(description="Booking.com scraper for Sapporo vacation rentals")
    parser.add_argument("--max-pages", type=int, default=3,
                        help="Max pages per area (default: 3)")
    parser.add_argument("--detail", action="store_true",
                        help="Run detail scraping pass on existing listings")
    parser.add_argument("--headless", type=str, default="true",
                        help="Run headless (true/false)")
    args = parser.parse_args()

    headless = args.headless.lower() != "false"

    print("=" * 60)
    print("  Booking.com Scraper - 札幌市 バケーションレンタル収集")
    print("=" * 60)

    asyncio.run(run(
        max_pages=args.max_pages,
        headless=headless,
        detail=args.detail,
    ))


if __name__ == "__main__":
    main()
