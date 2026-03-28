"""Booking.com スクレイパー — 検索結果スクレイピング"""

import asyncio
import json
import logging
import random
import re
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import Page

from .booking_db import BookingListing
from .booking_utils import parse_price, parse_booking_rating, parse_review_count

logger = logging.getLogger(__name__)

# ── 検索エリア定義 ──
# 札幌市10区: Booking.com検索で使うクエリとバウンディングボックス
SEARCH_AREAS = [
    {
        "name": "中央区",
        "ward": "中央区",
        "ne_lat": 43.080, "ne_lng": 141.365,
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


def build_search_url(area: dict, offset: int = 0) -> str:
    """Booking.com検索URLを構築

    フィルタ:
    - ht_id=201: アパートメント
    - ht_id=220: バケーションホーム
    """
    ward = area["ward"]
    base_url = "https://www.booking.com/searchresults.ja.html"
    params = {
        "ss": f"札幌市{ward}",
        "lang": "ja",
        "nflt": "ht_id=201;ht_id=220",    # アパートメント + バケーションホーム
        "rows": "25",                       # 1ページあたりの件数
    }
    if offset > 0:
        params["offset"] = str(offset)

    query = urlencode(params, safe=";=")
    return f"{base_url}?{query}"


async def scrape_area(page: Page, area: dict, max_pages: int = 3) -> list[BookingListing]:
    """1エリアのBooking.com検索結果をスクレイピング"""
    listings = []

    logger.info(f"Searching: {area['name']}")

    for page_num in range(1, max_pages + 1):
        offset = (page_num - 1) * 25
        search_url = build_search_url(area, offset=offset)

        try:
            # レート制限: ページ間 5-12秒
            await asyncio.sleep(random.uniform(5, 12))

            response = await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            if not response or response.status >= 400:
                logger.warning(f"  HTTP {response.status if response else 'None'} for page {page_num}")
                break

            # ページ読み込み待機
            await asyncio.sleep(random.uniform(3, 6))

            # CAPTCHAチェック
            captcha = await page.query_selector('[class*="captcha"], #captcha, [data-testid="captcha"]')
            if captcha:
                logger.warning("  CAPTCHA detected, stopping this area")
                break

            # 方法1: JSON-LD構造化データから抽出
            page_listings = await extract_from_json_ld(page, area)

            # 方法2: DOMから直接抽出（フォールバック）
            if not page_listings:
                page_listings = await extract_from_dom(page, area)

            if not page_listings:
                logger.info(f"  Page {page_num}: No listings found, stopping")
                break

            listings.extend(page_listings)
            logger.info(f"  Page {page_num}: {len(page_listings)} listings ({len(listings)} total)")

            # 次のページがあるか確認
            has_next = await page.query_selector(
                'button[aria-label="次のページ"], '
                'a[aria-label="次のページ"], '
                '[data-testid="pagination-button-next"]'
            )
            if not has_next:
                logger.info(f"  No more pages after page {page_num}")
                break

        except Exception as e:
            logger.warning(f"  Page {page_num} error: {e}")
            break

    return listings


async def extract_from_json_ld(page: Page, area: dict) -> list[BookingListing]:
    """JSON-LD構造化データからリスティング情報を抽出"""
    listings = []

    try:
        json_ld_texts = await page.evaluate("""
            () => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                return Array.from(scripts).map(s => s.textContent);
            }
        """)

        for text in (json_ld_texts or []):
            try:
                data = json.loads(text)
                # ItemList形式の場合
                if isinstance(data, dict) and data.get("@type") == "ItemList":
                    for item in data.get("itemListElement", []):
                        listing = parse_json_ld_item(item, area)
                        if listing:
                            listings.append(listing)
                # 配列形式の場合
                elif isinstance(data, list):
                    for item in data:
                        listing = parse_json_ld_item(item, area)
                        if listing:
                            listings.append(listing)
            except json.JSONDecodeError:
                continue

    except Exception as e:
        logger.debug(f"  JSON-LD extraction failed: {e}")

    return listings


def parse_json_ld_item(item: dict, area: dict) -> Optional[BookingListing]:
    """JSON-LDアイテムからBookingListingを構築"""
    if not isinstance(item, dict):
        return None

    # Hotel / LodgingBusiness / Apartment 等のタイプを処理
    entity = item.get("item", item)  # ItemList要素の場合
    if not isinstance(entity, dict):
        return None

    url = entity.get("url", "")
    name = entity.get("name", "")
    if not url:
        return None

    listing = BookingListing(
        listing_url=url,
        listing_title=name,
        ward=area["ward"],
    )

    # 評価
    aggregate_rating = entity.get("aggregateRating", {})
    if aggregate_rating:
        try:
            listing.rating = float(aggregate_rating.get("ratingValue", 0))
            listing.review_count = int(aggregate_rating.get("reviewCount", 0))
        except (ValueError, TypeError):
            pass

    # 価格
    offers = entity.get("offers") or entity.get("priceRange")
    if isinstance(offers, dict):
        try:
            listing.nightly_price = int(float(offers.get("price", 0)))
        except (ValueError, TypeError):
            pass

    # 座標
    geo = entity.get("geo", {})
    if geo:
        try:
            listing.latitude = float(geo.get("latitude", 0))
            listing.longitude = float(geo.get("longitude", 0))
        except (ValueError, TypeError):
            pass

    return listing


async def extract_from_dom(page: Page, area: dict) -> list[BookingListing]:
    """DOMから直接物件情報を抽出"""
    listings = []

    try:
        # Booking.comの物件カードセレクタ（複数パターン対応）
        cards = await page.query_selector_all(
            '[data-testid="property-card"], '
            '[data-testid="property-card-container"], '
            '.sr_property_block, '
            '[data-component="property-card"]'
        )

        if not cards:
            # より汎用的なセレクタ
            cards = await page.query_selector_all(
                '.a826ba81c4, '          # 検索結果カードの一般的なクラス
                '[data-testid="title"]'   # タイトル要素から親を辿る
            )

        logger.info(f"  Found {len(cards)} property cards")

        for card in cards:
            try:
                listing = await parse_property_card(card, area)
                if listing and listing.listing_url:
                    listings.append(listing)
            except Exception as e:
                logger.debug(f"  Card parse error: {e}")
                continue

    except Exception as e:
        logger.debug(f"  DOM extraction failed: {e}")

    return listings


async def parse_property_card(card, area: dict) -> Optional[BookingListing]:
    """1つの物件カードからデータを抽出"""
    listing = BookingListing(
        listing_url="",
        ward=area["ward"],
    )

    # ── リンクURL ──
    link = await card.query_selector(
        'a[data-testid="title-link"], '
        'a[data-testid="property-card-desktop-single-image"], '
        'a.js-sr-hotel-link, '
        'h3 a, '
        'a[href*="/hotel/"]'
    )
    if link:
        href = await link.get_attribute("href")
        if href:
            # 相対URLを絶対URLに変換
            if href.startswith("/"):
                listing.listing_url = f"https://www.booking.com{href}"
            else:
                listing.listing_url = href
            # トラッキングパラメータを簡略化（ベースURLのみ保持）
            listing.listing_url = listing.listing_url.split("?")[0] if "?" in listing.listing_url else listing.listing_url

    if not listing.listing_url:
        return None

    # ── タイトル ──
    title_el = await card.query_selector(
        '[data-testid="title"], '
        '.sr-hotel__name, '
        'h3, '
        '[class*="hotel_name"]'
    )
    if title_el:
        listing.listing_title = (await title_el.inner_text()).strip()

    # ── 価格 ──
    price_el = await card.query_selector(
        '[data-testid="price-and-discounted-price"], '
        '.bui-price-display__value, '
        '[class*="price_display"], '
        'span[data-testid="price-for-x-nights"], '
        '[class*="prco-valign-middle-helper"]'
    )
    if price_el:
        price_text = await price_el.inner_text()
        listing.nightly_price = parse_price(price_text)

    # ── 評価スコア（10点満点） ──
    rating_el = await card.query_selector(
        '[data-testid="review-score"] > div:first-child, '
        '.bui-review-score__badge, '
        '[class*="review-score-badge"], '
        '[aria-label*="スコア"]'
    )
    if rating_el:
        rating_text = await rating_el.inner_text()
        listing.rating = parse_booking_rating(rating_text)
    else:
        # aria-labelから取得を試行
        rating_container = await card.query_selector('[data-testid="review-score"]')
        if rating_container:
            aria = await rating_container.get_attribute("aria-label")
            if aria:
                listing.rating = parse_booking_rating(aria)

    # ── レビュー数 ──
    review_el = await card.query_selector(
        '[data-testid="review-score"] [class*="review_count"], '
        '.bui-review-score__text, '
        '[class*="review_count"], '
        '[class*="review-score-word"]'
    )
    if review_el:
        review_text = await review_el.inner_text()
        listing.review_count = parse_review_count(review_text)

    # ── プロパティタイプ ──
    type_el = await card.query_selector(
        '[data-testid="recommended-units"] [class*="unit_type"], '
        '[data-testid="property-card-unit-name"], '
        '[class*="room_link"] span, '
        'span[class*="property-type"]'
    )
    if type_el:
        listing.property_type = (await type_el.inner_text()).strip()

    return listing
