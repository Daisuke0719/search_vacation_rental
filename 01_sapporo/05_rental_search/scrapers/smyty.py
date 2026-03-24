"""スマイティスクレイパー

スマイティ (sumaity.com) の賃貸フリーワード検索を利用。
"""

import logging
import re
from typing import Optional
from urllib.parse import quote

from scrapers.base import BaseScraper, ListingResult

logger = logging.getLogger(__name__)


class SmytyScraper(BaseScraper):
    """スマイティ賃貸スクレイパー"""

    site_name = "smyty"

    def _build_search_url(self, building_name: str, ward: str) -> str:
        encoded = quote(building_name, encoding="utf-8")
        return (
            f"https://sumaity.com/chintai/hokkaido/sapporo/"
            f"?keyword={encoded}"
        )

    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        results = []
        url = self._build_search_url(building_name, ward)
        page = await self._new_page()

        try:
            if not await self._safe_goto(page, url):
                logger.error(f"[smyty] Failed to load for {building_name}")
                return results

            no_result = await page.query_selector(
                ".noResult, .no-hit, [class*='no-result']"
            )
            if no_result:
                logger.debug(f"[smyty] No results for {building_name}")
                return results

            cards = await page.query_selector_all(
                ".building, .property-item, [class*='bukken'], [class*='property']"
            )

            for card in cards:
                try:
                    listing = await self._parse_card(card, building_name)
                    if listing:
                        results.append(listing)
                except Exception as e:
                    logger.warning(f"[smyty] Parse error: {e}")
                    continue

            logger.info(
                f"[smyty] {building_name} ({ward}): {len(results)} listings found"
            )
        except Exception as e:
            logger.error(f"[smyty] Error searching {building_name}: {e}")
        finally:
            await page.close()

        return results

    async def _parse_card(self, card, building_name: str) -> Optional[ListingResult]:
        name_el = await card.query_selector(
            ".building-name, [class*='bukken-name'], [class*='property-name']"
        )
        if name_el:
            name_text = (await name_el.inner_text()).strip()
            if not self._is_name_match(building_name, name_text):
                return None
        else:
            name_text = building_name

        link = await card.query_selector("a[href*='sumaity.com']")
        if not link:
            link = await card.query_selector("a")
        if not link:
            return None
        href = await link.get_attribute("href")
        if not href:
            return None
        url = href if href.startswith("http") else f"https://sumaity.com{href}"

        rent_el = await card.query_selector("[class*='rent'], [class*='price']")
        rent_text = (await rent_el.inner_text()).strip() if rent_el else ""
        rent_price = self._parse_rent(rent_text)

        plan_el = await card.query_selector("[class*='madori'], [class*='layout']")
        floor_plan = (await plan_el.inner_text()).strip() if plan_el else None

        area_el = await card.query_selector("[class*='menseki'], [class*='area']")
        area_text = (await area_el.inner_text()).strip() if area_el else ""
        area_sqm = self._parse_area(area_text)

        return ListingResult(
            site_name="smyty",
            listing_url=url,
            listing_title=name_text,
            rent_price=rent_price,
            floor_plan=floor_plan,
            area_sqm=area_sqm,
        )

    def _is_name_match(self, search_name: str, result_name: str) -> bool:
        s = re.sub(r"[\s　・\-]", "", search_name.lower())
        r = re.sub(r"[\s　・\-]", "", result_name.lower())
        return s in r or r in s
