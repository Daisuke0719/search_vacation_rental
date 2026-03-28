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
import json
import logging
import random
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlencode

from playwright.async_api import async_playwright, Page, BrowserContext

# ── パス設定 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "external_data" / "booking_listings.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── ログ設定 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
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


@dataclass
class BookingListing:
    """Booking.com物件データ"""
    listing_url: str
    listing_title: str = ""
    nightly_price: Optional[int] = None       # 1泊あたりJPY
    rating: Optional[float] = None            # 0-10スケール
    review_count: Optional[int] = None
    property_type: str = ""                   # apartment, house等
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    ward: str = ""                            # 区
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    amenities: Optional[str] = None           # JSONリスト
    detail_scraped: bool = False


# =====================================================================
# DB操作
# =====================================================================

def init_db():
    """Booking.com用DBスキーマを初期化"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS booking_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_url TEXT NOT NULL UNIQUE,
            listing_title TEXT,
            nightly_price INTEGER,
            rating REAL,
            review_count INTEGER,
            property_type TEXT,
            guest_capacity INTEGER,
            bedrooms INTEGER,
            bathrooms INTEGER,
            ward TEXT,
            latitude REAL,
            longitude REAL,
            amenities TEXT,
            detail_scraped INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_booking_ward ON booking_listings(ward)
    """)
    conn.commit()
    conn.close()
    logger.info(f"DB initialized: {DB_PATH}")


def save_listings(listings: list[BookingListing]):
    """物件データをDBに保存（UPSERT）"""
    conn = sqlite3.connect(str(DB_PATH))
    inserted = 0
    updated = 0

    for item in listings:
        try:
            conn.execute(
                """INSERT INTO booking_listings
                   (listing_url, listing_title, nightly_price, rating, review_count,
                    property_type, guest_capacity, bedrooms, bathrooms, ward,
                    latitude, longitude, amenities, detail_scraped)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(listing_url) DO UPDATE SET
                     listing_title = COALESCE(excluded.listing_title, listing_title),
                     nightly_price = COALESCE(excluded.nightly_price, nightly_price),
                     rating = COALESCE(excluded.rating, rating),
                     review_count = COALESCE(excluded.review_count, review_count),
                     property_type = COALESCE(excluded.property_type, property_type),
                     guest_capacity = COALESCE(excluded.guest_capacity, guest_capacity),
                     bedrooms = COALESCE(excluded.bedrooms, bedrooms),
                     bathrooms = COALESCE(excluded.bathrooms, bathrooms),
                     latitude = COALESCE(excluded.latitude, latitude),
                     longitude = COALESCE(excluded.longitude, longitude),
                     amenities = COALESCE(excluded.amenities, amenities),
                     detail_scraped = MAX(detail_scraped, excluded.detail_scraped),
                     scraped_at = CURRENT_TIMESTAMP""",
                (item.listing_url, item.listing_title, item.nightly_price,
                 item.rating, item.review_count, item.property_type,
                 item.guest_capacity, item.bedrooms, item.bathrooms, item.ward,
                 item.latitude, item.longitude, item.amenities,
                 int(item.detail_scraped)),
            )
            changes = conn.execute("SELECT changes()").fetchone()[0]
            if changes > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"DB insert error: {e}")
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Saved: {inserted} new/updated, {updated} errors")


def get_listings_without_details(limit: int = 50) -> list[dict]:
    """詳細未取得の物件URLリストを取得"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, listing_url, ward FROM booking_listings
           WHERE detail_scraped = 0
           ORDER BY scraped_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


# =====================================================================
# 検索結果スクレイピング
# =====================================================================

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


# =====================================================================
# 詳細ページスクレイピング（第2パス）
# =====================================================================

async def scrape_details(page: Page, listings_to_scrape: list[dict]):
    """個別物件ページから詳細情報を取得"""
    updated = []

    for i, row in enumerate(listings_to_scrape):
        url = row["listing_url"]
        logger.info(f"  [{i+1}/{len(listings_to_scrape)}] Detail: {url[:80]}...")

        try:
            # レート制限: 詳細ページ間 5-12秒
            await asyncio.sleep(random.uniform(5, 12))

            response = await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            if not response or response.status >= 400:
                logger.warning(f"    HTTP {response.status if response else 'None'}")
                continue

            await asyncio.sleep(random.uniform(2, 5))

            detail = BookingListing(
                listing_url=url,
                ward=row["ward"],
                detail_scraped=True,
            )

            # 方法1: JSON-LDから詳細を取得
            detail_from_ld = await extract_detail_from_json_ld(page)
            if detail_from_ld:
                detail.guest_capacity = detail_from_ld.get("guest_capacity")
                detail.bedrooms = detail_from_ld.get("bedrooms")
                detail.bathrooms = detail_from_ld.get("bathrooms")
                detail.amenities = detail_from_ld.get("amenities")
                detail.latitude = detail_from_ld.get("latitude")
                detail.longitude = detail_from_ld.get("longitude")

            # 方法2: DOMからフォールバック
            if not detail.guest_capacity:
                dom_detail = await extract_detail_from_dom(page)
                detail.guest_capacity = detail.guest_capacity or dom_detail.get("guest_capacity")
                detail.bedrooms = detail.bedrooms or dom_detail.get("bedrooms")
                detail.bathrooms = detail.bathrooms or dom_detail.get("bathrooms")
                detail.amenities = detail.amenities or dom_detail.get("amenities")

            updated.append(detail)

            if len(updated) % 10 == 0:
                save_listings(updated)
                updated = []

        except Exception as e:
            logger.warning(f"    Detail error: {e}")
            continue

    # 残りを保存
    if updated:
        save_listings(updated)

    logger.info(f"  Detail scraping complete: {len(listings_to_scrape)} processed")


async def extract_detail_from_json_ld(page: Page) -> Optional[dict]:
    """詳細ページのJSON-LDから情報を抽出"""
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
                if not isinstance(data, dict):
                    continue

                result = {}

                # 座標
                geo = data.get("geo", {})
                if geo:
                    try:
                        result["latitude"] = float(geo.get("latitude", 0))
                        result["longitude"] = float(geo.get("longitude", 0))
                    except (ValueError, TypeError):
                        pass

                # アメニティ
                amenity_feature = data.get("amenityFeature", [])
                if amenity_feature:
                    amenities = [
                        a.get("name", "") for a in amenity_feature
                        if isinstance(a, dict) and a.get("name")
                    ]
                    if amenities:
                        result["amenities"] = json.dumps(amenities, ensure_ascii=False)

                # 部屋数・定員（numberOfRooms等）
                if data.get("numberOfRooms"):
                    try:
                        result["bedrooms"] = int(data["numberOfRooms"])
                    except (ValueError, TypeError):
                        pass

                if data.get("occupancy") and isinstance(data["occupancy"], dict):
                    try:
                        result["guest_capacity"] = int(data["occupancy"].get("maxValue", 0))
                    except (ValueError, TypeError):
                        pass

                if result:
                    return result

            except json.JSONDecodeError:
                continue

    except Exception as e:
        logger.debug(f"    JSON-LD detail extraction failed: {e}")

    return None


async def extract_detail_from_dom(page: Page) -> dict:
    """詳細ページのDOMから物件情報を抽出"""
    result = {}

    try:
        # ── 定員・寝室・バスルーム ──
        # Booking.comのファシリティハイライトから取得
        highlights = await page.query_selector_all(
            '[data-testid="facility-group-icon-row"], '
            '.hprt-facilities-block li, '
            '[class*="facility"], '
            '[data-testid="property-highlights"] li'
        )

        for hl in highlights:
            text = (await hl.inner_text()).strip().lower()

            # 定員
            capacity_match = re.search(r"(\d+)\s*(?:名|人|ゲスト|guest)", text)
            if capacity_match:
                result["guest_capacity"] = int(capacity_match.group(1))

            # 寝室
            bedroom_match = re.search(r"(\d+)\s*(?:ベッドルーム|寝室|bedroom)", text)
            if bedroom_match:
                result["bedrooms"] = int(bedroom_match.group(1))

            # バスルーム
            bathroom_match = re.search(r"(\d+)\s*(?:バスルーム|浴室|bathroom)", text)
            if bathroom_match:
                result["bathrooms"] = int(bathroom_match.group(1))

        # ── アメニティ ──
        amenity_items = await page.query_selector_all(
            '[data-testid="facility-list-item"], '
            '.bui-list__description, '
            '[class*="facility"] span'
        )
        amenities = []
        for el in amenity_items:
            text = (await el.inner_text()).strip()
            if text and len(text) < 100:  # 不正な長文を除外
                amenities.append(text)

        if amenities:
            result["amenities"] = json.dumps(amenities[:50], ensure_ascii=False)  # 最大50個

    except Exception as e:
        logger.debug(f"    DOM detail extraction failed: {e}")

    return result


# =====================================================================
# ユーティリティ
# =====================================================================

def parse_price(text: str) -> Optional[int]:
    """価格テキストから1泊あたりの金額（JPY）を抽出

    Booking.comの価格表記例:
    - "￥8,500"
    - "¥ 12,000"
    - "8500 円"
    """
    if not text:
        return None
    # 通貨記号・カンマ・空白を除去
    cleaned = text.replace(",", "").replace("，", "").replace(" ", "")
    cleaned = cleaned.replace("￥", "").replace("¥", "").replace("円", "")

    # 数値を抽出（複数ある場合は最初のもの）
    m = re.search(r"(\d+)", cleaned)
    if m:
        val = int(m.group(1))
        if val >= 1000:  # 1泊1000円以上を有効とする
            return val
    return None


def parse_booking_rating(text: str) -> Optional[float]:
    """Booking.comの評価スコア（10点満点）を抽出

    表記例: "8.5", "スコア: 9.2", "9.2"
    """
    if not text:
        return None
    m = re.search(r"(\d+\.?\d*)", text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 10:
            return val
    return None


def parse_review_count(text: str) -> Optional[int]:
    """レビュー数テキストから数値を抽出

    表記例: "1,234件のレビュー", "レビュー234件", "234 reviews"
    """
    if not text:
        return None
    cleaned = text.replace(",", "").replace("，", "")
    m = re.search(r"(\d+)", cleaned)
    if m:
        return int(m.group(1))
    return None


# =====================================================================
# メイン実行
# =====================================================================

async def run(max_pages: int = 3, headless: bool = True, detail: bool = False):
    """全エリアのスクレイピングを実行"""
    init_db()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "ja,en;q=0.9",
            },
        )
        context.set_default_timeout(45000)

        # Stealth対策
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
        except ImportError:
            stealth = None
            logger.info("playwright-stealth not installed, proceeding without stealth")

        page = await context.new_page()
        if stealth:
            await stealth.apply_stealth_async(page)

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
