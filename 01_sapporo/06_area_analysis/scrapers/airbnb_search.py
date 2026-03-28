"""Airbnb 検索結果スクレイピング"""

import asyncio
import json
import logging
import random
from typing import Optional

from playwright.async_api import Page

try:
    from .airbnb_db import AirbnbListing
    from .airbnb_utils import find_nested_key, parse_listing_from_json, parse_price, parse_rating, parse_review_count
except ImportError:
    from airbnb_db import AirbnbListing
    from airbnb_utils import find_nested_key, parse_listing_from_json, parse_price, parse_rating, parse_review_count

logger = logging.getLogger(__name__)

# ── 検索エリア定義 ──
# 札幌市の主要エリア: Airbnb検索で使うクエリとバウンディングボックス
SEARCH_AREAS = [
    {
        "name": "中央区（大通・すすきの）",
        "ward": "中央区",
        "ne_lat": 43.070, "ne_lng": 141.365,
        "sw_lat": 43.040, "sw_lng": 141.330,
    },
    {
        "name": "中央区（札幌駅・北側）",
        "ward": "中央区",
        "ne_lat": 43.080, "ne_lng": 141.365,
        "sw_lat": 43.062, "sw_lng": 141.330,
    },
    {
        "name": "中央区（中島公園・円山）",
        "ward": "中央区",
        "ne_lat": 43.050, "ne_lng": 141.355,
        "sw_lat": 43.030, "sw_lng": 141.300,
    },
    {
        "name": "北区",
        "ward": "北区",
        "ne_lat": 43.120, "ne_lng": 141.380,
        "sw_lat": 43.080, "sw_lng": 141.320,
    },
    {
        "name": "東区",
        "ward": "東区",
        "ne_lat": 43.100, "ne_lng": 141.410,
        "sw_lat": 43.060, "sw_lng": 141.360,
    },
    {
        "name": "白石区",
        "ward": "白石区",
        "ne_lat": 43.060, "ne_lng": 141.430,
        "sw_lat": 43.025, "sw_lng": 141.380,
    },
    {
        "name": "豊平区",
        "ward": "豊平区",
        "ne_lat": 43.050, "ne_lng": 141.410,
        "sw_lat": 43.015, "sw_lng": 141.360,
    },
    {
        "name": "南区",
        "ward": "南区",
        "ne_lat": 43.030, "ne_lng": 141.380,
        "sw_lat": 42.990, "sw_lng": 141.320,
    },
    {
        "name": "西区",
        "ward": "西区",
        "ne_lat": 43.100, "ne_lng": 141.330,
        "sw_lat": 43.050, "sw_lng": 141.270,
    },
    {
        "name": "厚別区",
        "ward": "厚別区",
        "ne_lat": 43.060, "ne_lng": 141.500,
        "sw_lat": 43.020, "sw_lng": 141.450,
    },
    {
        "name": "手稲区",
        "ward": "手稲区",
        "ne_lat": 43.140, "ne_lng": 141.270,
        "sw_lat": 43.100, "sw_lng": 141.210,
    },
    {
        "name": "清田区",
        "ward": "清田区",
        "ne_lat": 43.020, "ne_lng": 141.470,
        "sw_lat": 42.980, "sw_lng": 141.420,
    },
]


async def scrape_area(page: Page, area: dict, max_pages: int = 5) -> list[AirbnbListing]:
    """1エリアのAirbnb検索結果をスクレイピング"""
    listings = []

    # Airbnb検索URL構築
    # マップ範囲指定で検索
    url = (
        f"https://www.airbnb.jp/s/札幌市{area['ward']}/homes"
        f"?ne_lat={area['ne_lat']}&ne_lng={area['ne_lng']}"
        f"&sw_lat={area['sw_lat']}&sw_lng={area['sw_lng']}"
        f"&search_type=filter_change"
        f"&tab_id=home_tab"
    )

    logger.info(f"Searching: {area['name']}")

    for page_num in range(1, max_pages + 1):
        page_url = url if page_num == 1 else f"{url}&items_offset={(page_num - 1) * 18}"

        try:
            # レート制限
            await asyncio.sleep(random.uniform(5, 12))

            response = await page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
            if not response or response.status >= 400:
                logger.warning(f"  HTTP {response.status if response else 'None'} for page {page_num}")
                break

            # ページ読み込み待機
            await asyncio.sleep(random.uniform(3, 6))

            # 方法1: __NEXT_DATA__ JSONからデータ抽出（最も確実）
            page_listings = await extract_from_next_data(page, area)

            # 方法2: DOMからデータ抽出（フォールバック）
            if not page_listings:
                page_listings = await extract_from_dom(page, area)

            if not page_listings:
                logger.info(f"  Page {page_num}: No listings found, stopping")
                break

            listings.extend(page_listings)
            logger.info(f"  Page {page_num}: {len(page_listings)} listings ({len(listings)} total)")

            # 次のページがあるか確認
            has_next = await page.query_selector('a[aria-label="次"]') or \
                       await page.query_selector('a[aria-label="Next"]')
            if not has_next:
                break

        except Exception as e:
            logger.warning(f"  Page {page_num} error: {e}")
            break

    return listings


async def extract_from_next_data(page: Page, area: dict) -> list[AirbnbListing]:
    """__NEXT_DATA__ scriptタグからJSON抽出"""
    listings = []

    try:
        script_content = await page.evaluate("""
            () => {
                const script = document.querySelector('script#__NEXT_DATA__');
                return script ? script.textContent : null;
            }
        """)

        if not script_content:
            return listings

        data = json.loads(script_content)

        # Airbnbの内部構造からリスティングを探す
        search_results = find_nested_key(data, "searchResults") or \
                         find_nested_key(data, "dpiItems") or \
                         find_nested_key(data, "sections")

        if not search_results:
            return listings

        # リスト形式のデータを探索
        items = search_results if isinstance(search_results, list) else [search_results]

        for item in items:
            listing = parse_listing_from_json(item, area)
            if listing:
                listings.append(listing)

    except Exception as e:
        logger.debug(f"  __NEXT_DATA__ extraction failed: {e}")

    return listings


async def extract_from_dom(page: Page, area: dict) -> list[AirbnbListing]:
    """DOMから直接物件情報を抽出"""
    listings = []

    try:
        # Airbnbのリスティングカード要素を取得
        cards = await page.query_selector_all('[itemprop="itemListElement"], [data-testid="card-container"], .cy5jw6o, .c4mnd7m, .gsgwcm, .g1qv1ctd')

        if not cards:
            # より汎用的なセレクタ
            cards = await page.query_selector_all('div[aria-labelledby]')

        for card in cards:
            try:
                listing = AirbnbListing(
                    listing_url="",
                    ward=area["ward"],
                    search_area=area["name"],
                )

                # リンク
                link = await card.query_selector('a[href*="/rooms/"]')
                if link:
                    href = await link.get_attribute("href")
                    if href:
                        listing.listing_url = f"https://www.airbnb.jp{href}" if href.startswith("/") else href
                        # クエリパラメータを除去してIDベースのURLにする
                        listing.listing_url = listing.listing_url.split("?")[0]

                if not listing.listing_url:
                    continue

                # タイトル
                title_el = await card.query_selector('[data-testid="listing-card-title"], [id*="title"]')
                if title_el:
                    listing.listing_title = await title_el.inner_text()

                # 価格
                price_el = await card.query_selector('[data-testid="price-availability-row"] span, ._tyxjp1, ._1y74zjx')
                if not price_el:
                    price_el = await card.query_selector('span._tyxjp1, span:has-text("¥")')
                if price_el:
                    price_text = await price_el.inner_text()
                    listing.nightly_price = parse_price(price_text)

                # 評価
                rating_el = await card.query_selector('[aria-label*="5点"], span.r1dxllyb, [aria-label*="out of 5"]')
                if rating_el:
                    rating_text = await rating_el.get_attribute("aria-label") or await rating_el.inner_text()
                    listing.rating = parse_rating(rating_text)

                # レビュー数
                review_el = await card.query_selector('span:has-text("件のレビュー"), span:has-text("reviews")')
                if review_el:
                    review_text = await review_el.inner_text()
                    listing.review_count = parse_review_count(review_text)

                # プロパティタイプ
                type_el = await card.query_selector('[data-testid="listing-card-subtitle"]')
                if type_el:
                    listing.property_type = await type_el.inner_text()

                # スーパーホスト
                superhost_el = await card.query_selector('[aria-label*="スーパーホスト"], [aria-label*="Superhost"]')
                listing.superhost = superhost_el is not None

                if listing.listing_url and listing.nightly_price:
                    listings.append(listing)

            except Exception as e:
                logger.debug(f"  Card parse error: {e}")
                continue

    except Exception as e:
        logger.debug(f"  DOM extraction failed: {e}")

    return listings
