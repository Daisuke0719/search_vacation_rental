"""Google検索を使った各サイトの賃貸掲載検索

各不動産サイトを直接スクレイピングする代わりに、Google検索の
site: 演算子を使って各サイト上の掲載を発見する。

例: site:suumo.jp "AMSタワー" 賃貸 札幌
"""

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import quote, urlparse, parse_qs

from scrapers.base import BaseScraper, ListingResult

logger = logging.getLogger(__name__)

# 各サイトのドメインと表示名
SITE_DOMAINS = {
    "suumo": "suumo.jp",
    "homes": "homes.co.jp",
    "athome": "athome.co.jp",
    "yahoo": "realestate.yahoo.co.jp",
    "smyty": "sumaity.com",
}


class GoogleSearchScraper(BaseScraper):
    """Google検索を使った統合スクレイパー"""

    site_name = "google"  # 内部名、実際のsite_nameは検索結果から設定

    def __init__(self, target_site: str = "suumo"):
        super().__init__()
        self.target_site = target_site
        self.target_domain = SITE_DOMAINS.get(target_site, "suumo.jp")
        self.site_name = target_site  # 結果のsite_nameを対象サイトに設定

    def _build_google_url(self, building_name: str, ward: str) -> str:
        """Google検索URLを構築"""
        query = f'site:{self.target_domain} "{building_name}" 賃貸 札幌{ward}'
        encoded = quote(query, encoding="utf-8")
        return f"https://www.google.com/search?q={encoded}&num=20&hl=ja"

    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        """Google検索で対象サイトの掲載を検索"""
        results = []
        url = self._build_google_url(building_name, ward)
        page = await self._new_page()

        try:
            if not await self._safe_goto(page, url):
                logger.error(
                    f"[{self.target_site}] Google search failed for {building_name}"
                )
                return results

            # CAPTCHAチェック
            captcha = await page.query_selector(
                "#captcha-form, .g-recaptcha, [id*='captcha']"
            )
            if captcha:
                logger.warning(f"[{self.target_site}] Google CAPTCHA detected")
                return results

            # 検索結果を取得
            search_results = await page.query_selector_all("#search .g, .tF2Cxc")
            if not search_results:
                # 別セレクタ
                search_results = await page.query_selector_all(
                    "[data-hveid] a[href*='" + self.target_domain + "']"
                )

            for result in search_results:
                try:
                    listing = await self._parse_google_result(
                        result, building_name, page
                    )
                    if listing:
                        results.append(listing)
                except Exception as e:
                    logger.debug(f"[{self.target_site}] Parse error: {e}")
                    continue

            # 重複URL除去
            seen_urls = set()
            unique_results = []
            for r in results:
                if r.listing_url not in seen_urls:
                    seen_urls.add(r.listing_url)
                    unique_results.append(r)
            results = unique_results

            logger.info(
                f"[{self.target_site}] {building_name} ({ward}): "
                f"{len(results)} listings found via Google"
            )
        except Exception as e:
            logger.error(
                f"[{self.target_site}] Google search error for {building_name}: {e}"
            )
        finally:
            await page.close()

        return results

    async def _parse_google_result(
        self, result, building_name: str, page
    ) -> Optional[ListingResult]:
        """Google検索結果から物件情報を抽出"""
        # リンクを取得
        link = await result.query_selector("a[href]")
        if not link:
            return None

        href = await link.get_attribute("href")
        if not href or self.target_domain not in href:
            return None

        # Google のリダイレクトURLからクリーンなURLを抽出
        clean_url = self._clean_google_url(href)
        if not clean_url or self.target_domain not in clean_url:
            return None

        # 賃貸関連URLかフィルタ
        if not self._is_rental_url(clean_url):
            return None

        # タイトル
        title_el = await result.query_selector("h3")
        title_text = (await title_el.inner_text()).strip() if title_el else ""

        # 建物名の一致チェック
        if not self._is_name_match(building_name, title_text):
            # スニペットでもチェック
            snippet_el = await result.query_selector(
                ".VwiC3b, .st, [data-content-feature]"
            )
            snippet = (
                (await snippet_el.inner_text()).strip() if snippet_el else ""
            )
            if not self._is_name_match(building_name, snippet):
                return None

        # スニペットから価格情報を抽出
        snippet_el = await result.query_selector(".VwiC3b, .st")
        snippet = (await snippet_el.inner_text()).strip() if snippet_el else ""

        rent_price = self._extract_rent_from_snippet(snippet)
        floor_plan = self._extract_floor_plan(snippet + " " + title_text)

        return ListingResult(
            site_name=self.target_site,
            listing_url=clean_url,
            listing_title=title_text[:200],
            rent_price=rent_price,
            floor_plan=floor_plan,
        )

    def _clean_google_url(self, url: str) -> str:
        """GoogleリダイレクトURLをクリーンにする"""
        if "/url?" in url:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            if "q" in params:
                return params["q"][0]
            if "url" in params:
                return params["url"][0]
        return url

    def _is_rental_url(self, url: str) -> bool:
        """賃貸関連のURLかチェック"""
        rental_keywords = ["chintai", "rent", "賃貸"]
        url_lower = url.lower()
        # 除外パターン（売買、口コミ等）
        exclude = ["baikyaku", "kodate", "mansion/new", "review", "kuchikomi"]
        if any(e in url_lower for e in exclude):
            return False
        # 賃貸系なら通す、それ以外でも建物ページは通す
        if any(k in url_lower for k in rental_keywords):
            return True
        # library, buildingページも通す
        if "library" in url_lower or "building" in url_lower:
            return True
        return True  # 判断つかない場合は通す

    def _extract_rent_from_snippet(self, text: str) -> Optional[int]:
        """スニペットテキストから家賃を抽出"""
        # "5.5万円" パターン
        m = re.search(r"(\d+\.?\d*)\s*万\s*円", text)
        if m:
            return int(float(m.group(1)) * 10000)
        # "55,000円" パターン
        m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*円", text)
        if m:
            val = int(m.group(1).replace(",", ""))
            if 10000 <= val <= 1000000:  # 妥当な家賃範囲
                return val
        return None

    def _extract_floor_plan(self, text: str) -> Optional[str]:
        """テキストから間取りを抽出"""
        m = re.search(r"(\d[SLDK]{1,4})", text)
        return m.group(1) if m else None

    def _is_name_match(self, search_name: str, text: str) -> bool:
        """テキスト中に建物名が含まれるかチェック"""
        if not text:
            return False
        s = re.sub(r"[\s　・\-]", "", search_name.lower())
        t = re.sub(r"[\s　・\-]", "", text.lower())
        return s in t or t in s
