"""Booking.com スクレイパー — 詳細ページスクレイピング"""

import asyncio
import json
import logging
import random
import re
from typing import Optional

from playwright.async_api import Page

from .booking_db import BookingListing, save_listings

logger = logging.getLogger(__name__)


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
