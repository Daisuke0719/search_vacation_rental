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
import json
import logging
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page

# ── パス設定 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "external_data" / "airbnb_listings.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── ログ設定 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
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


@dataclass
class AirbnbListing:
    """Airbnb物件データ"""
    listing_url: str
    listing_title: str = ""
    nightly_price: Optional[int] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    property_type: str = ""
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    ward: str = ""
    search_area: str = ""
    superhost: bool = False


@dataclass
class AirbnbDetail:
    """詳細ページから取得するデータ"""
    listing_url: str
    rating: Optional[float] = None
    review_count: Optional[int] = None
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    property_type: str = ""
    amenities: Optional[list[str]] = None
    superhost: bool = False
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    neighborhood: str = ""


# =====================================================================
# DB操作
# =====================================================================

def init_db():
    """Airbnb用DBスキーマを初期化"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS airbnb_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_url TEXT NOT NULL UNIQUE,
            listing_title TEXT,
            nightly_price INTEGER,
            rating REAL,
            review_count INTEGER,
            property_type TEXT,
            guest_capacity INTEGER,
            bedrooms INTEGER,
            ward TEXT,
            search_area TEXT,
            superhost INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_airbnb_ward ON airbnb_listings(ward)
    """)
    conn.commit()
    conn.close()
    logger.info(f"DB initialized: {DB_PATH}")


def migrate_db():
    """既存DBに新カラムを追加するマイグレーション"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.execute("PRAGMA table_info(airbnb_listings)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    new_columns = [
        ("bathrooms", "INTEGER"),
        ("amenities", "TEXT"),
        ("calendar_occupancy", "REAL"),
        ("detail_scraped_at", "TIMESTAMP"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
        ("neighborhood", "TEXT"),
    ]

    added = []
    for col_name, col_type in new_columns:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE airbnb_listings ADD COLUMN {col_name} {col_type}")
            added.append(col_name)

    conn.commit()
    conn.close()

    if added:
        logger.info(f"DB migration: added columns {added}")
    else:
        logger.info("DB migration: no new columns needed")


def save_listings(listings: list[AirbnbListing]):
    """物件データをDBに保存"""
    conn = sqlite3.connect(str(DB_PATH))
    inserted = 0
    updated = 0

    for item in listings:
        try:
            conn.execute(
                """INSERT INTO airbnb_listings
                   (listing_url, listing_title, nightly_price, rating, review_count,
                    property_type, guest_capacity, bedrooms, ward, search_area, superhost)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(listing_url) DO UPDATE SET
                     nightly_price = excluded.nightly_price,
                     rating = excluded.rating,
                     review_count = excluded.review_count,
                     scraped_at = CURRENT_TIMESTAMP""",
                (item.listing_url, item.listing_title, item.nightly_price,
                 item.rating, item.review_count, item.property_type,
                 item.guest_capacity, item.bedrooms, item.ward,
                 item.search_area, int(item.superhost)),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"DB insert error: {e}")
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Saved: {inserted} new, {updated} updated")


def save_detail(detail: AirbnbDetail):
    """詳細ページのデータをDBに更新"""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        amenities_json = json.dumps(detail.amenities, ensure_ascii=False) if detail.amenities else None
        conn.execute(
            """UPDATE airbnb_listings SET
                rating = COALESCE(?, rating),
                review_count = COALESCE(?, review_count),
                guest_capacity = COALESCE(?, guest_capacity),
                bedrooms = COALESCE(?, bedrooms),
                bathrooms = COALESCE(?, bathrooms),
                property_type = CASE WHEN ? != '' THEN ? ELSE property_type END,
                amenities = COALESCE(?, amenities),
                superhost = ?,
                latitude = COALESCE(?, latitude),
                longitude = COALESCE(?, longitude),
                neighborhood = CASE WHEN ? != '' THEN ? ELSE neighborhood END,
                detail_scraped_at = CURRENT_TIMESTAMP
               WHERE listing_url = ?""",
            (detail.rating, detail.review_count, detail.guest_capacity,
             detail.bedrooms, detail.bathrooms,
             detail.property_type, detail.property_type,
             amenities_json,
             int(detail.superhost),
             detail.latitude, detail.longitude,
             detail.neighborhood, detail.neighborhood,
             detail.listing_url),
        )
        conn.commit()
        changes = conn.execute("SELECT changes()").fetchone()[0]
        if changes > 0:
            logger.info(f"  Detail saved for {detail.listing_url}")
        else:
            logger.warning(f"  No row matched for {detail.listing_url}")
    except Exception as e:
        logger.warning(f"DB detail update error: {e}")
    finally:
        conn.close()


def get_listings_needing_detail(max_listings: int = 50) -> list[str]:
    """詳細スクレイピングが必要な物件URLを取得"""
    conn = sqlite3.connect(str(DB_PATH))
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        """SELECT listing_url FROM airbnb_listings
           WHERE detail_scraped_at IS NULL OR detail_scraped_at < ?
           ORDER BY detail_scraped_at ASC NULLS FIRST, scraped_at DESC
           LIMIT ?""",
        (cutoff, max_listings),
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


# =====================================================================
# 検索結果スクレイピング (既存機能)
# =====================================================================

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


# =====================================================================
# 詳細ページスクレイピング (第2パス)
# =====================================================================

async def scrape_detail_page(page: Page, listing_url: str) -> Optional[AirbnbDetail]:
    """個別物件の詳細ページから情報を抽出

    __NEXT_DATA__ JSONを最優先で試み、取れない場合はDOMパースにフォールバック。
    """
    detail = AirbnbDetail(listing_url=listing_url)

    try:
        response = await page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
        if not response or response.status >= 400:
            logger.warning(f"  HTTP {response.status if response else 'None'} for {listing_url}")
            return None

        # ページ読み込み待機
        await asyncio.sleep(random.uniform(2, 4))

        # 方法1: __NEXT_DATA__ JSONから抽出
        extracted = await _extract_detail_from_next_data(page, detail)

        # 方法2: DOMから抽出（フォールバック）
        if not extracted:
            await _extract_detail_from_dom(page, detail)

    except Exception as e:
        logger.warning(f"  Detail page error for {listing_url}: {e}")
        return None

    return detail


async def _extract_detail_from_next_data(page: Page, detail: AirbnbDetail) -> bool:
    """__NEXT_DATA__からリスティング詳細を抽出。成功したらTrueを返す。"""
    try:
        script_content = await page.evaluate("""
            () => {
                const script = document.querySelector('script#__NEXT_DATA__');
                return script ? script.textContent : null;
            }
        """)

        if not script_content:
            return False

        data = json.loads(script_content)

        # リスティングデータを探す（複数の既知パス）
        listing_data = (
            find_nested_key(data, "listingData")
            or find_nested_key(data, "pdpListing")
            or find_nested_key(data, "listing")
        )

        if not listing_data or not isinstance(listing_data, dict):
            # フラット化してsectionsなどから探す
            listing_data = data

        # 評価
        detail.rating = (
            find_nested_key(listing_data, "avgRating")
            or find_nested_key(listing_data, "overallRating")
            or find_nested_key(listing_data, "guestSatisfactionOverall")
        )
        if detail.rating is not None:
            detail.rating = float(detail.rating)

        # レビュー数
        detail.review_count = (
            find_nested_key(listing_data, "reviewsCount")
            or find_nested_key(listing_data, "visibleReviewCount")
            or find_nested_key(listing_data, "reviewCount")
        )
        if detail.review_count is not None:
            detail.review_count = int(detail.review_count)

        # 定員
        detail.guest_capacity = (
            find_nested_key(listing_data, "personCapacity")
            or find_nested_key(listing_data, "guestCapacity")
        )
        if detail.guest_capacity is not None:
            detail.guest_capacity = int(detail.guest_capacity)

        # 寝室数
        bedrooms = find_nested_key(listing_data, "bedrooms")
        if bedrooms is not None:
            detail.bedrooms = int(bedrooms)

        # バスルーム数
        bathrooms = (
            find_nested_key(listing_data, "bathrooms")
            or find_nested_key(listing_data, "bathroomCount")
        )
        if bathrooms is not None:
            detail.bathrooms = int(float(bathrooms))

        # プロパティタイプ
        detail.property_type = (
            find_nested_key(listing_data, "roomType")
            or find_nested_key(listing_data, "roomTypeCategory")
            or find_nested_key(listing_data, "propertyType")
            or ""
        )

        # アメニティ
        amenity_groups = find_nested_key(listing_data, "amenityGroups")
        if amenity_groups and isinstance(amenity_groups, list):
            amenities = []
            for group in amenity_groups:
                if isinstance(group, dict):
                    items = group.get("amenities") or group.get("items") or []
                    for item in items:
                        if isinstance(item, dict):
                            name = item.get("title") or item.get("name") or ""
                            if name:
                                amenities.append(name)
                        elif isinstance(item, str):
                            amenities.append(item)
            if amenities:
                detail.amenities = amenities
        else:
            # フラットなアメニティリスト
            amenities_list = find_nested_key(listing_data, "amenities")
            if amenities_list and isinstance(amenities_list, list):
                flat = []
                for a in amenities_list:
                    if isinstance(a, str):
                        flat.append(a)
                    elif isinstance(a, dict):
                        name = a.get("name") or a.get("title") or ""
                        if name:
                            flat.append(name)
                if flat:
                    detail.amenities = flat

        # スーパーホスト
        superhost = find_nested_key(listing_data, "isSuperhost")
        detail.superhost = bool(superhost) if superhost is not None else False

        # 位置情報
        lat = find_nested_key(listing_data, "lat") or find_nested_key(listing_data, "latitude")
        lng = find_nested_key(listing_data, "lng") or find_nested_key(listing_data, "longitude")
        if lat is not None and lng is not None:
            detail.latitude = float(lat)
            detail.longitude = float(lng)

        # 近隣エリア
        detail.neighborhood = (
            find_nested_key(listing_data, "neighborhood")
            or find_nested_key(listing_data, "localizedNeighborhood")
            or find_nested_key(listing_data, "locationTitle")
            or ""
        )

        # 何かデータが取れたか判定
        has_data = any([
            detail.rating, detail.review_count, detail.guest_capacity,
            detail.bedrooms, detail.bathrooms, detail.amenities,
            detail.latitude, detail.longitude,
        ])
        return has_data

    except Exception as e:
        logger.debug(f"  __NEXT_DATA__ detail extraction failed: {e}")
        return False


async def _extract_detail_from_dom(page: Page, detail: AirbnbDetail):
    """DOMパースで詳細ページからデータを抽出（フォールバック）"""
    try:
        # 評価 — aria-labelやmetaタグから
        rating_el = await page.query_selector(
            '[data-testid="pdp-reviews-highlight-banner-host-rating"] span, '
            'span[aria-label*="5点"], '
            'span[aria-label*="out of 5"], '
            'button[aria-label*="レビュー"] span, '
            'meta[itemprop="ratingValue"]'
        )
        if rating_el:
            tag_name = await rating_el.evaluate("el => el.tagName")
            if tag_name == "META":
                rating_text = await rating_el.get_attribute("content") or ""
            else:
                rating_text = await rating_el.get_attribute("aria-label") or await rating_el.inner_text()
            detail.rating = parse_rating(rating_text)

        # レビュー数
        review_el = await page.query_selector(
            'button[aria-label*="レビュー"], '
            'a[href*="reviews"] span, '
            'span:has-text("件のレビュー"), '
            'span:has-text("reviews")'
        )
        if review_el:
            review_text = await review_el.get_attribute("aria-label") or await review_el.inner_text()
            detail.review_count = parse_review_count(review_text)

        # ゲスト数・寝室数・バスルーム数 — 物件概要セクション
        # 「ゲスト2人 · 1ベッドルーム · ベッド1台 · 1バスルーム」のようなテキスト
        overview_el = await page.query_selector(
            '[data-testid="listing-overview-highlights"], '
            'div.lgx66tx, '  # よくある概要セクション
            'ol.lgx66tx'
        )
        if overview_el:
            overview_text = await overview_el.inner_text()
            _parse_overview_text(overview_text, detail)
        else:
            # ページ全体のテキストからパターンマッチ
            body_text = await page.evaluate("() => document.body.innerText.substring(0, 5000)")
            _parse_overview_text(body_text, detail)

        # プロパティタイプ
        type_el = await page.query_selector(
            'h2[data-testid="listing-overview-title"], '
            'div[data-section-id="TITLE_DEFAULT"] h2, '
            'h1 + div'
        )
        if type_el:
            type_text = await type_el.inner_text()
            # 「貸切 · マンション」「個室 · 一軒家」のようなテキスト
            for pattern_jp, pattern_en in [
                ("貸切", "entire"), ("個室", "private"), ("シェアルーム", "shared"),
            ]:
                if pattern_jp in type_text.lower() or pattern_en in type_text.lower():
                    detail.property_type = type_text.strip()
                    break

        # スーパーホスト
        superhost_el = await page.query_selector(
            '[aria-label*="スーパーホスト"], '
            '[aria-label*="Superhost"], '
            'span:has-text("スーパーホスト")'
        )
        detail.superhost = superhost_el is not None

        # アメニティ — 「アメニティ」セクションを展開して取得
        try:
            amenities_btn = await page.query_selector(
                'button:has-text("アメニティをすべて表示"), '
                'button:has-text("Show all amenities"), '
                'button:has-text("すべてのアメニティ")'
            )
            if amenities_btn:
                await amenities_btn.click()
                await asyncio.sleep(random.uniform(1, 2))

                amenity_items = await page.query_selector_all(
                    '[data-testid="amenity-row"] div, '
                    'div[aria-modal="true"] div.twad414'
                )
                if amenity_items:
                    amenities = []
                    for item in amenity_items:
                        text = (await item.inner_text()).strip()
                        if text and len(text) < 100:  # 不要に長いテキストを除外
                            amenities.append(text)
                    if amenities:
                        detail.amenities = amenities

                # モーダルを閉じる
                close_btn = await page.query_selector(
                    'button[aria-label="閉じる"], button[aria-label="Close"]'
                )
                if close_btn:
                    await close_btn.click()
                    await asyncio.sleep(0.5)
        except Exception:
            pass  # アメニティ取得失敗は無視

        # 位置情報 — metaタグやmap要素から
        try:
            # Open Graph / meta から
            lat_meta = await page.query_selector('meta[property="place:location:latitude"]')
            lng_meta = await page.query_selector('meta[property="place:location:longitude"]')
            if lat_meta and lng_meta:
                lat_str = await lat_meta.get_attribute("content")
                lng_str = await lng_meta.get_attribute("content")
                if lat_str and lng_str:
                    detail.latitude = float(lat_str)
                    detail.longitude = float(lng_str)
        except Exception:
            pass

        # 近隣エリア名
        neighborhood_el = await page.query_selector(
            '[data-testid="listing-location-name"], '
            'span.t1jojoys'
        )
        if neighborhood_el:
            detail.neighborhood = (await neighborhood_el.inner_text()).strip()

    except Exception as e:
        logger.debug(f"  DOM detail extraction failed: {e}")


def _parse_overview_text(text: str, detail: AirbnbDetail):
    """概要テキストからゲスト数・寝室数・バスルーム数を抽出"""
    if not text:
        return

    # ゲスト数: 「ゲスト2人」「2 guests」
    m = re.search(r"ゲスト\s*(\d+)\s*人|(\d+)\s*guests?", text)
    if m and detail.guest_capacity is None:
        detail.guest_capacity = int(m.group(1) or m.group(2))

    # 寝室数: 「1ベッドルーム」「1 bedroom」
    m = re.search(r"(\d+)\s*ベッドルーム|(\d+)\s*bedrooms?", text)
    if m and detail.bedrooms is None:
        detail.bedrooms = int(m.group(1) or m.group(2))

    # バスルーム数: 「1バスルーム」「1 bathroom」
    m = re.search(r"(\d+)\s*バスルーム|(\d+)\s*bathrooms?", text)
    if m and detail.bathrooms is None:
        detail.bathrooms = int(m.group(1) or m.group(2))


# =====================================================================
# 詳細スクレイピング実行
# =====================================================================

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
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        context.set_default_timeout(30000)

        # stealth対策
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
        except ImportError:
            stealth = None

        page = await context.new_page()
        if stealth:
            await stealth.apply_stealth_async(page)

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


# =====================================================================
# ユーティリティ
# =====================================================================

def find_nested_key(data, key):
    """ネストされたdict/listから特定のキーを再帰的に探す"""
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for v in data.values():
            result = find_nested_key(v, key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_nested_key(item, key)
            if result is not None:
                return result
    return None


def parse_listing_from_json(item: dict, area: dict) -> Optional[AirbnbListing]:
    """JSONオブジェクトからAirbnbListing を構築"""
    if not isinstance(item, dict):
        return None

    listing = AirbnbListing(
        listing_url="",
        ward=area["ward"],
        search_area=area["name"],
    )

    # URL/ID
    listing_id = item.get("id") or item.get("listingId") or find_nested_key(item, "id")
    if listing_id:
        listing.listing_url = f"https://www.airbnb.jp/rooms/{listing_id}"

    # タイトル
    listing.listing_title = item.get("name") or item.get("title") or \
                            find_nested_key(item, "name") or ""

    # 価格
    price_data = item.get("pricingQuote") or item.get("pricing") or \
                 find_nested_key(item, "pricingQuote")
    if isinstance(price_data, dict):
        rate = price_data.get("rate") or price_data.get("price")
        if isinstance(rate, dict):
            listing.nightly_price = rate.get("amount")
        elif isinstance(rate, (int, float)):
            listing.nightly_price = int(rate)

    # 評価
    listing.rating = item.get("avgRating") or find_nested_key(item, "avgRating")

    # レビュー数
    listing.review_count = item.get("reviewsCount") or find_nested_key(item, "reviewsCount")

    # 定員
    listing.guest_capacity = item.get("personCapacity") or find_nested_key(item, "personCapacity")

    # 寝室数
    listing.bedrooms = item.get("bedrooms") or find_nested_key(item, "bedrooms")

    # プロパティタイプ
    listing.property_type = item.get("roomType") or item.get("roomTypeCategory") or ""

    # スーパーホスト
    listing.superhost = bool(item.get("isSuperhost") or find_nested_key(item, "isSuperhost"))

    if listing.listing_url:
        return listing
    return None


def parse_price(text: str) -> Optional[int]:
    """価格テキストから数値を抽出"""
    if not text:
        return None
    text = text.replace(",", "").replace("，", "").replace(" ", "").replace("\u00a5", "").replace("¥", "")
    # "8500" or "8,500円" or "￥8,500/泊"
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        if val > 100:  # 少なくとも100円以上
            return val
    return None


def parse_rating(text: str) -> Optional[float]:
    """評価テキストから数値を抽出"""
    if not text:
        return None
    m = re.search(r"([\d.]+)", text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 5:
            return val
    return None


def parse_review_count(text: str) -> Optional[int]:
    """レビュー数テキストから数値を抽出"""
    if not text:
        return None
    m = re.search(r"(\d+)", text.replace(",", ""))
    if m:
        return int(m.group(1))
    return None


# =====================================================================
# メイン実行
# =====================================================================

async def run(max_pages: int = 5, headless: bool = True):
    """全エリアのスクレイピングを実行"""
    init_db()
    migrate_db()
    all_listings = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        context.set_default_timeout(30000)

        # stealth対策
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
        except ImportError:
            stealth = None

        page = await context.new_page()
        if stealth:
            await stealth.apply_stealth_async(page)

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
