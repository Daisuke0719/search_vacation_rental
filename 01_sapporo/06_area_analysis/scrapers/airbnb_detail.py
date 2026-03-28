"""Airbnb 詳細ページスクレイピング"""

import asyncio
import json
import logging
import random
import re
from typing import Optional

from playwright.async_api import Page

try:
    from .airbnb_db import AirbnbDetail
    from .airbnb_utils import find_nested_key, parse_rating, parse_review_count, _parse_overview_text
except ImportError:
    from airbnb_db import AirbnbDetail
    from airbnb_utils import find_nested_key, parse_rating, parse_review_count, _parse_overview_text

logger = logging.getLogger(__name__)


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
