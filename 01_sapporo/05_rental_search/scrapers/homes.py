"""HOMES（ライフルホームズ）スクレイパー

フォーム入力方式でフリーワード検索を実行する。
ベースURL: https://www.homes.co.jp/chintai/
入力欄: input#cond_freeword (name="cond[freeword]")
ボタン: input[type="submit"].btnSubmit
"""

import logging
import re
from typing import Optional

from scrapers.base import BaseScraper, ListingResult

logger = logging.getLogger(__name__)

# フォーム検索の設定
HOMES_BASE_URL = "https://www.homes.co.jp/chintai/"
HOMES_INPUT_SELECTOR = "#cond_freeword"
HOMES_SUBMIT_SELECTOR = "input[type='submit'].btnSubmit"


class HomesScraper(BaseScraper):
    """HOMES賃貸スクレイパー（フォーム入力方式）"""

    site_name = "homes"

    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        results = []
        page = await self._new_page()

        try:
            # Step 1: ベースページに移動
            if not await self._safe_goto(page, HOMES_BASE_URL):
                logger.error(f"[homes] Failed to load base page")
                return results

            # Step 2: フリーワード入力欄に建物名を入力
            input_el = await page.query_selector(HOMES_INPUT_SELECTOR)
            if not input_el:
                logger.error(f"[homes] Freeword input not found: {HOMES_INPUT_SELECTOR}")
                return results

            await input_el.scroll_into_view_if_needed()
            await input_el.fill(building_name)
            await page.wait_for_timeout(500)

            # Step 3: 検索ボタンをクリック
            submit_el = await page.query_selector(HOMES_SUBMIT_SELECTOR)
            if submit_el:
                await submit_el.click()
            else:
                logger.warning(f"[homes] Submit button not found, pressing Enter")
                await input_el.press("Enter")

            # Step 4: 結果ページの読み込みを待機
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)

            # 結果なしチェック
            body_text = await page.inner_text("body")
            if "条件にあう物件がありません" in body_text or "該当する物件がありません" in body_text:
                logger.debug(f"[homes] No results for {building_name}")
                return results

            no_result = await page.query_selector(
                ".mod-noResult, .noSearchResult, [class*='no-result']"
            )
            if no_result:
                logger.debug(f"[homes] No results for {building_name}")
                return results

            # Step 5: 物件カードをパース
            cards = await page.query_selector_all(
                ".mod-mergeBuilding, .mod-building, [class*='building-card']"
            )

            for card in cards:
                try:
                    listing = await self._parse_card(card, building_name)
                    if listing:
                        results.append(listing)
                except Exception as e:
                    logger.warning(f"[homes] Parse error: {e}")
                    continue

            logger.info(
                f"[homes] {building_name} ({ward}): {len(results)} listings found"
            )
        except Exception as e:
            logger.error(f"[homes] Error searching {building_name}: {e}")
        finally:
            await page.close()

        return results

    async def _parse_card(self, card, building_name: str) -> Optional[ListingResult]:
        # 建物名チェック
        name_el = await card.query_selector(
            ".mod-mergeBuilding--name, .bukkenName, [class*='building-name']"
        )
        if name_el:
            name_text = (await name_el.inner_text()).strip()
            if not self._is_name_match(building_name, name_text):
                return None
        else:
            name_text = building_name

        # URL
        link = await card.query_selector("a[href*='/chintai/']")
        if not link:
            return None
        href = await link.get_attribute("href")
        if not href:
            return None
        url = href if href.startswith("http") else f"https://www.homes.co.jp{href}"

        # 家賃
        rent_el = await card.query_selector(
            ".priceLabel, [class*='rent'], [class*='price']"
        )
        rent_text = (await rent_el.inner_text()).strip() if rent_el else ""
        rent_price = self._parse_rent(rent_text)

        # 間取り
        plan_el = await card.query_selector("[class*='madori'], [class*='layout']")
        floor_plan = (await plan_el.inner_text()).strip() if plan_el else None

        # 面積
        area_el = await card.query_selector("[class*='menseki'], [class*='area']")
        area_text = (await area_el.inner_text()).strip() if area_el else ""
        area_sqm = self._parse_area(area_text)

        # 駅
        station_el = await card.query_selector("[class*='station'], [class*='traffic']")
        station_text = (await station_el.inner_text()).strip() if station_el else ""
        walk_min = self._parse_walk_minutes(station_text)

        return ListingResult(
            site_name="homes",
            listing_url=url,
            listing_title=name_text,
            rent_price=rent_price,
            floor_plan=floor_plan,
            area_sqm=area_sqm,
            nearest_station=station_text[:50] if station_text else None,
            walk_minutes=walk_min,
        )

    def _is_name_match(self, search_name: str, result_name: str) -> bool:
        s = re.sub(r"[\s　・\-]", "", search_name.lower())
        r = re.sub(r"[\s　・\-]", "", result_name.lower())
        return s in r or r in s
