"""SUUMOスクレイパー

SUUMOの賃貸フリーワード検索を利用して建物名で物件を検索する。
URL: https://suumo.jp/chintai/hokkaido/sc_01101/ (中央区の場合)
フリーワード検索パラメータ: fw={building_name}
"""

import logging
import re
from typing import Optional
from urllib.parse import quote

from scrapers.base import BaseScraper, ListingResult
from config import SAPPORO_WARD_CODES

logger = logging.getLogger(__name__)

# SUUMO 区コード
SUUMO_WARD_CODES = {
    "中央区": "01101",
    "北区": "01102",
    "東区": "01103",
    "白石区": "01104",
    "豊平区": "01105",
    "南区": "01106",
    "西区": "01107",
    "厚別区": "01108",
    "手稲区": "01109",
    "清田区": "01110",
}


class SuumoScraper(BaseScraper):
    """SUUMO賃貸スクレイパー"""

    site_name = "suumo"

    def _build_search_url(self, building_name: str, ward: str) -> str:
        """fw2パラメータ付き検索URLを構築"""
        ward_code = SUUMO_WARD_CODES.get(ward, "01101")
        encoded_name = quote(building_name, encoding="utf-8")
        return (
            f"https://suumo.jp/jj/chintai/ichiran/FR301FC001/"
            f"?ar=010&bs=040&ta=01&sc={ward_code}"
            f"&cb=0.0&ct=9999999&mb=0&mt=9999999"
            f"&et=9999999&cn=9999999"
            f"&shkr1=03&shkr2=03&shkr3=03&shkr4=03"
            f"&sngz=&po1=25&pc=50&fw2={encoded_name}"
        )

    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        """SUUMOでfw2フリーワード検索を実行"""
        results = []
        url = self._build_search_url(building_name, ward)
        page = await self._new_page()

        try:
            if not await self._safe_goto(page, url):
                logger.error(f"[suumo] Failed to load search page for {building_name}")
                return results

            # JS描画を待機
            await page.wait_for_timeout(2000)

            # 「条件にあう物件がありません」チェック
            body_text = await page.inner_text("body")
            if "条件にあう物件がありません" in body_text:
                logger.debug(f"[suumo] No results for {building_name}")
                return results

            # 物件カセットを取得
            cassets = await page.query_selector_all(".cassetteitem")
            if not cassets:
                cassets = await page.query_selector_all("[class*='property']")

            for casset in cassets:
                try:
                    listing = await self._parse_casset(casset, building_name)
                    if listing:
                        results.append(listing)
                except Exception as e:
                    logger.warning(f"[suumo] Error parsing casset: {e}")
                    continue

            logger.info(
                f"[suumo] {building_name} ({ward}): {len(results)} listings found"
            )
        except Exception as e:
            logger.error(f"[suumo] Error searching {building_name}: {e}")
        finally:
            await page.close()

        return results

    async def _parse_casset(self, casset, building_name: str) -> Optional[ListingResult]:
        """物件カセットから情報をパース"""
        # 建物名の確認（検索結果が関係ない建物を含む場合がある）
        title_el = await casset.query_selector(
            ".cassetteitem_content-title, [class*='building-name']"
        )
        if title_el:
            title_text = (await title_el.inner_text()).strip()
            # 建物名が結果に含まれていない場合はスキップ
            if not self._is_name_match(building_name, title_text):
                return None
        else:
            title_text = building_name

        # 建物レベルの情報を抽出（築年数、最寄駅、徒歩分）
        building_info = await self._parse_building_info(casset)

        # 各物件（部屋）のテーブル行を取得
        # SUUMOは1つのカセットに複数の部屋が表示される
        table_rows = await casset.query_selector_all(".js-cassette_link, tbody tr")
        listings = []

        for row in table_rows:
            try:
                listing = await self._parse_room_row(row, title_text)
                if listing:
                    # 建物レベルの情報をマージ
                    if building_info.get("building_age") and not listing.building_age:
                        listing.building_age = building_info["building_age"]
                    if building_info.get("nearest_station") and not listing.nearest_station:
                        listing.nearest_station = building_info["nearest_station"]
                    if building_info.get("walk_minutes") and not listing.walk_minutes:
                        listing.walk_minutes = building_info["walk_minutes"]
                    listings.append(listing)
            except Exception:
                continue

        # 部屋行がない場合はカセット全体から1件分取得
        if not listings:
            listing = await self._parse_casset_simple(casset, title_text)
            if listing:
                if building_info.get("building_age"):
                    listing.building_age = building_info["building_age"]
                if building_info.get("nearest_station"):
                    listing.nearest_station = building_info["nearest_station"]
                if building_info.get("walk_minutes"):
                    listing.walk_minutes = building_info["walk_minutes"]
                return listing

        return listings[0] if listings else None

    async def _parse_building_info(self, casset) -> dict:
        """カセットから建物レベルの情報を抽出（築年数・最寄駅・徒歩分）"""
        info: dict = {}

        # 築年数: .cassetteitem_detail-col3 内に「築XX年」がある
        try:
            detail_cols = await casset.query_selector_all(
                ".cassetteitem_detail-col3 div, .cassetteitem_detail-col3 span"
            )
            for col in detail_cols:
                text = (await col.inner_text()).strip()
                age_match = re.search(r"築(\d+)年", text)
                if age_match:
                    info["building_age"] = f"築{age_match.group(1)}年"
                    break
            # col3自体のテキストもチェック
            if "building_age" not in info:
                col3 = await casset.query_selector(".cassetteitem_detail-col3")
                if col3:
                    text = (await col3.inner_text()).strip()
                    age_match = re.search(r"築(\d+)年", text)
                    if age_match:
                        info["building_age"] = f"築{age_match.group(1)}年"
        except Exception:
            pass

        # 最寄駅・徒歩分: .cassetteitem_detail-col1 内
        try:
            station_el = await casset.query_selector(
                ".cassetteitem_detail-col1 .cassetteitem_detail-text"
            )
            if not station_el:
                station_el = await casset.query_selector(".cassetteitem_detail-col1")
            if station_el:
                station_text = (await station_el.inner_text()).strip()
                # "地下鉄東豊線/学園前駅 歩4分" のようなパターン
                walk_match = re.search(r"歩(\d+)分", station_text)
                if walk_match:
                    info["walk_minutes"] = int(walk_match.group(1))
                # 駅名を抽出: "路線名/駅名" or "駅名"
                station_name_match = re.search(r"(?:[^/]+/)?\s*(.+?駅)", station_text)
                if station_name_match:
                    info["nearest_station"] = station_name_match.group(1).strip()
        except Exception:
            pass

        return info

    async def _parse_room_row(self, row, building_title: str) -> Optional[ListingResult]:
        """テーブル行から物件情報をパース"""
        # リンクURL
        link = await row.query_selector("a[href*='/chintai/']")
        if not link:
            link = await row.query_selector("a")
        url = ""
        if link:
            href = await link.get_attribute("href")
            if href:
                url = href if href.startswith("http") else f"https://suumo.jp{href}"

        if not url:
            return None

        # 家賃
        rent_el = await row.query_selector(
            ".cassetteitem_price--rent, [class*='rent'], .detailbox-property-point"
        )
        rent_text = (await rent_el.inner_text()).strip() if rent_el else ""
        rent_price = self._parse_rent(rent_text)

        # 管理費
        admin_el = await row.query_selector(
            ".cassetteitem_price--administration, [class*='administration']"
        )
        admin_text = (await admin_el.inner_text()).strip() if admin_el else ""
        mgmt_fee = self._parse_rent(admin_text)

        # 敷金・礼金
        deposit_el = await row.query_selector(
            ".cassetteitem_price--deposit, [class*='deposit']"
        )
        deposit = (await deposit_el.inner_text()).strip() if deposit_el else None

        key_money_el = await row.query_selector(
            ".cassetteitem_price--gratuity, [class*='gratuity']"
        )
        key_money = (await key_money_el.inner_text()).strip() if key_money_el else None

        # 間取り
        plan_el = await row.query_selector(
            ".cassetteitem_madori, [class*='madori']"
        )
        floor_plan = (await plan_el.inner_text()).strip() if plan_el else None

        # 面積
        area_el = await row.query_selector(
            ".cassetteitem_menseki, [class*='menseki']"
        )
        area_text = (await area_el.inner_text()).strip() if area_el else ""
        area_sqm = self._parse_area(area_text)

        # 階数
        floor_el = await row.query_selector("[class*='col--floor'], td:nth-child(3)")
        floor_number = (await floor_el.inner_text()).strip() if floor_el else None

        # バストイレ別の判定
        bath_toilet_separate = await self._detect_bath_toilet_separate(row)

        return ListingResult(
            site_name="suumo",
            listing_url=url,
            listing_title=building_title,
            rent_price=rent_price,
            management_fee=mgmt_fee,
            deposit=deposit,
            key_money=key_money,
            floor_plan=floor_plan,
            area_sqm=area_sqm,
            floor_number=floor_number,
            bath_toilet_separate=bath_toilet_separate,
        )

    async def _detect_bath_toilet_separate(self, element) -> Optional[int]:
        """バストイレ別を検出する。1=別, 0=ユニット, None=不明"""
        try:
            text = (await element.inner_text()).strip()
            if "バス・トイレ別" in text or "BT別" in text:
                return 1
            if "ユニットバス" in text or "UB" in text:
                return 0
        except Exception:
            pass

        # アイコンクラスで判定
        try:
            icons = await element.query_selector_all(
                "li, span, [class*='icon'], [class*='cond']"
            )
            for icon in icons:
                icon_text = (await icon.inner_text()).strip()
                if "バス・トイレ別" in icon_text or "BT別" in icon_text:
                    return 1
                if "ユニットバス" in icon_text:
                    return 0
        except Exception:
            pass

        return None

    async def _parse_casset_simple(self, casset, building_title: str) -> Optional[ListingResult]:
        """カセット全体から簡易パース"""
        link = await casset.query_selector("a[href*='/chintai/']")
        if not link:
            return None
        href = await link.get_attribute("href")
        if not href:
            return None
        url = href if href.startswith("http") else f"https://suumo.jp{href}"

        rent_el = await casset.query_selector(
            ".cassetteitem_price--rent, [class*='rent']"
        )
        rent_text = (await rent_el.inner_text()).strip() if rent_el else ""

        return ListingResult(
            site_name="suumo",
            listing_url=url,
            listing_title=building_title,
            rent_price=self._parse_rent(rent_text),
        )

    def _is_name_match(self, search_name: str, result_name: str) -> bool:
        """建物名が検索名と一致するかチェック（ファジーマッチ）"""
        # 完全一致
        if search_name in result_name or result_name in search_name:
            return True
        # 正規化して比較
        s = re.sub(r"[\s　・\-]", "", search_name.lower())
        r = re.sub(r"[\s　・\-]", "", result_name.lower())
        if s in r or r in s:
            return True
        return False
