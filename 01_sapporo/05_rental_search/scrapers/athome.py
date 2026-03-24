"""athomeスクレイパー

フリーワード検索URL:
https://www.athome.co.jp/chintai/hokkaido/sapporo-{ward}-city/list/?keyword={building_name}
"""

import logging
import re
from typing import Optional
from urllib.parse import quote

from scrapers.base import BaseScraper, ListingResult

logger = logging.getLogger(__name__)

# athome 区コードマッピング
ATHOME_WARD_CODES = {
    "中央区": "1101",
    "北区": "1102",
    "東区": "1103",
    "白石区": "1104",
    "豊平区": "1105",
    "南区": "1106",
    "西区": "1107",
    "厚別区": "1108",
    "手稲区": "1109",
    "清田区": "1110",
}


class AthomeScraper(BaseScraper):
    """athome賃貸スクレイパー"""

    site_name = "athome"

    def _build_search_url(self, building_name: str, ward: str) -> str:
        ward_code = ATHOME_WARD_CODES.get(ward, "1101")
        encoded = quote(building_name, encoding="utf-8")
        return (
            f"https://www.athome.co.jp/chintai/"
            f"hokkaido/sapporo-shi-{ward_code}/"
            f"keyword-{encoded}/list/"
        )

    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        results = []
        url = self._build_search_url(building_name, ward)
        page = await self._new_page()

        try:
            if not await self._safe_goto(page, url):
                logger.error(f"[athome] Failed to load for {building_name}")
                return results

            no_result = await page.query_selector(
                ".nodata, .no-result, [class*='noResult']"
            )
            if no_result:
                logger.debug(f"[athome] No results for {building_name}")
                return results

            # 物件カードを取得
            cards = await page.query_selector_all(
                ".p-property, .p-building, [class*='property-card']"
            )

            for card in cards:
                try:
                    listing = await self._parse_card(card, building_name)
                    if listing:
                        results.append(listing)
                except Exception as e:
                    logger.warning(f"[athome] Parse error: {e}")
                    continue

            logger.info(
                f"[athome] {building_name} ({ward}): {len(results)} listings found"
            )
        except Exception as e:
            logger.error(f"[athome] Error searching {building_name}: {e}")
        finally:
            await page.close()

        return results

    async def _parse_card(self, card, building_name: str) -> Optional[ListingResult]:
        name_el = await card.query_selector(
            ".p-property__title, .p-building__name, [class*='building-name']"
        )
        if name_el:
            name_text = (await name_el.inner_text()).strip()
            if not self._is_name_match(building_name, name_text):
                return None
        else:
            name_text = building_name

        link = await card.query_selector("a[href*='/chintai/']")
        if not link:
            return None
        href = await link.get_attribute("href")
        if not href:
            return None
        url = href if href.startswith("http") else f"https://www.athome.co.jp{href}"

        rent_el = await card.query_selector("[class*='rent'], [class*='price']")
        rent_text = (await rent_el.inner_text()).strip() if rent_el else ""
        rent_price = self._parse_rent(rent_text)

        plan_el = await card.query_selector("[class*='madori'], [class*='layout']")
        floor_plan = (await plan_el.inner_text()).strip() if plan_el else None

        area_el = await card.query_selector("[class*='menseki'], [class*='area']")
        area_text = (await area_el.inner_text()).strip() if area_el else ""
        area_sqm = self._parse_area(area_text)

        return ListingResult(
            site_name="athome",
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
