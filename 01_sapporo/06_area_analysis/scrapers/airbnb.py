"""Airbnbスクレイパー — 札幌市の民泊物件データを区別に収集

Airbnbの検索結果から物件情報（価格、評価、レビュー数等）を取得し、
SQLiteに保存する。区別のADR・稼働率推計に使用する。

使用方法:
    python scrapers/airbnb.py                  # 全エリア検索
    python scrapers/airbnb.py --max-pages 3    # ページ数制限
    python scrapers/airbnb.py --headless false  # ブラウザ表示
    python scrapers/airbnb.py --detail          # 詳細ページスクレイピング
    python scrapers/airbnb.py --detail --max-listings 20  # 詳細20件まで
"""

import argparse
import asyncio
import logging
import random
import sqlite3
import sys
from pathlib import Path

from playwright.async_api import async_playwright

# ── 共有ブラウザモジュールをインポート ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.browser import create_browser_context, new_stealth_page

# ── サブモジュールからインポート ──
try:
    from .airbnb_db import (
        DB_PATH, AirbnbListing, AirbnbDetail,
        init_db, migrate_db, save_listings, save_detail, get_listings_needing_detail,
    )
    from .airbnb_utils import (
        find_nested_key, parse_listing_from_json, parse_price, parse_rating,
        parse_review_count, _parse_overview_text,
    )
    from .airbnb_search import SEARCH_AREAS, scrape_area, extract_from_next_data, extract_from_dom
    from .airbnb_detail import scrape_detail_page, _extract_detail_from_next_data, _extract_detail_from_dom
except ImportError:
    from airbnb_db import (
        DB_PATH, AirbnbListing, AirbnbDetail,
        init_db, migrate_db, save_listings, save_detail, get_listings_needing_detail,
    )
    from airbnb_utils import (
        find_nested_key, parse_listing_from_json, parse_price, parse_rating,
        parse_review_count, _parse_overview_text,
    )
    from airbnb_search import SEARCH_AREAS, scrape_area, extract_from_next_data, extract_from_dom
    from airbnb_detail import scrape_detail_page, _extract_detail_from_next_data, _extract_detail_from_dom

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

async def run(max_pages: int = 5, headless: bool = True):
    """全エリアのスクレイピングを実行"""
    init_db()
    migrate_db()
    all_listings = []

    async with async_playwright() as p:
        browser, context = await create_browser_context(p, headless=headless)
        page = await new_stealth_page(context)

        for i, area in enumerate(SEARCH_AREAS):
            logger.info(f"\n[{i+1}/{len(SEARCH_AREAS)}] {area['name']}")

            try:
                area_listings = await scrape_area(page, area, max_pages=max_pages)
                all_listings.extend(area_listings)

                if area_listings:
                    save_listings(area_listings)

                # エリア間のランダム遅延
                if i < len(SEARCH_AREAS) - 1:
                    delay = random.uniform(8, 15)
                    logger.info(f"  Waiting {delay:.1f}s before next area...")
                    await asyncio.sleep(delay)

            except Exception as e:
                logger.error(f"  Area error: {e}")
                continue

        await browser.close()

    # サマリー出力
    logger.info(f"\n{'='*50}")
    logger.info(f"  Total listings: {len(all_listings)}")

    # 区別集計
    ward_counts = {}
    for lst in all_listings:
        ward_counts[lst.ward] = ward_counts.get(lst.ward, 0) + 1
    for ward, count in sorted(ward_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {ward}: {count} listings")

    return all_listings


async def scrape_details(max_listings: int = 50, headless: bool = True):
    """詳細ページの第2パススクレイピングを実行

    DBからdetail_scraped_atがNULLまたは7日以上前の物件を取得し、
    各物件の詳細ページを訪問してデータを更新する。
    """
    init_db()
    migrate_db()

    urls = get_listings_needing_detail(max_listings)
    if not urls:
        logger.info("No listings need detail scraping.")
        return

    logger.info(f"Detail scraping: {len(urls)} listings to process")

    success_count = 0
    error_count = 0

    async with async_playwright() as p:
        browser, context = await create_browser_context(p, headless=headless)
        page = await new_stealth_page(context)

        for i, url in enumerate(urls):
            logger.info(f"\n[{i+1}/{len(urls)}] {url}")

            try:
                detail = await scrape_detail_page(page, url)

                if detail:
                    save_detail(detail)
                    success_count += 1
                else:
                    # スクレイプ失敗でもタイムスタンプを記録して無限ループを避ける
                    conn = sqlite3.connect(str(DB_PATH))
                    conn.execute(
                        "UPDATE airbnb_listings SET detail_scraped_at = CURRENT_TIMESTAMP WHERE listing_url = ?",
                        (url,),
                    )
                    conn.commit()
                    conn.close()
                    error_count += 1

            except Exception as e:
                logger.error(f"  Error: {e}")
                error_count += 1

            # レート制限: ページ間に5-10秒のランダム待機
            if i < len(urls) - 1:
                delay = random.uniform(5, 10)
                logger.info(f"  Waiting {delay:.1f}s...")
                await asyncio.sleep(delay)

        await browser.close()

    # サマリー
    logger.info(f"\n{'='*50}")
    logger.info(f"  Detail scraping complete: {success_count} success, {error_count} errors")


def main():
    parser = argparse.ArgumentParser(description="Airbnb scraper for Sapporo")
    parser.add_argument("--max-pages", type=int, default=5, help="Max pages per area")
    parser.add_argument("--headless", type=str, default="true",
                        help="Run headless (true/false)")
    parser.add_argument("--detail", action="store_true",
                        help="Run detail page scraping (second pass)")
    parser.add_argument("--max-listings", type=int, default=50,
                        help="Max listings for detail scraping")
    args = parser.parse_args()

    headless = args.headless.lower() != "false"

    if args.detail:
        print("=" * 60)
        print("  Airbnb Detail Scraper - 札幌市 物件詳細データ収集")
        print("=" * 60)
        asyncio.run(scrape_details(max_listings=args.max_listings, headless=headless))
    else:
        print("=" * 60)
        print("  Airbnb Scraper - 札幌市 区別物件データ収集")
        print("=" * 60)
        asyncio.run(run(max_pages=args.max_pages, headless=headless))


if __name__ == "__main__":
    main()
