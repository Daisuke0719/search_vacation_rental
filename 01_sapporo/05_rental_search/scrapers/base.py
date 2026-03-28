"""スクレイパー基底クラス"""

import asyncio
import logging
import random
import re
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from shared.browser import get_user_agent, DEFAULT_VIEWPORT, DEFAULT_LOCALE, DEFAULT_TIMEZONE, _load_stealth

from config import RATE_LIMITS, PLAYWRIGHT_HEADLESS, PLAYWRIGHT_TIMEOUT

logger = logging.getLogger(__name__)


@dataclass
class ListingResult:
    """賃貸掲載情報"""
    site_name: str
    listing_url: str
    listing_title: str = ""
    rent_price: Optional[int] = None        # 月額賃料（円）
    management_fee: Optional[int] = None    # 管理費/共益費
    deposit: Optional[str] = None           # 敷金
    key_money: Optional[str] = None         # 礼金
    floor_plan: Optional[str] = None        # 間取り
    area_sqm: Optional[float] = None        # 専有面積
    floor_number: Optional[str] = None      # 階数
    building_age: Optional[str] = None      # 築年数
    nearest_station: Optional[str] = None   # 最寄駅
    walk_minutes: Optional[int] = None      # 徒歩分
    bath_toilet_separate: Optional[int] = None  # バストイレ別 (1=別, 0=一体, None=不明)


class BaseScraper(ABC):
    """スクレイパー基底クラス（Playwrightベース）"""

    site_name: str = ""

    def __init__(self):
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._last_request_time: float = 0
        self._request_count: int = 0
        self._rate_config = RATE_LIMITS.get(self.site_name, {
            "min_delay": 3, "max_delay": 7, "max_per_hour": 100
        })

    async def start(self):
        """ブラウザを起動（stealth対策付き）"""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=PLAYWRIGHT_HEADLESS,
        )
        self._context = await self._browser.new_context(
            viewport=DEFAULT_VIEWPORT,
            locale=DEFAULT_LOCALE,
            timezone_id=DEFAULT_TIMEZONE,
            user_agent=self._get_user_agent(),
        )
        self._stealth = _load_stealth()
        self._context.set_default_timeout(PLAYWRIGHT_TIMEOUT)
        logger.info(f"[{self.site_name}] Browser started")

    async def stop(self):
        """ブラウザを終了"""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info(f"[{self.site_name}] Browser stopped")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    def _get_user_agent(self) -> str:
        """ランダムなUser-Agentを返す"""
        return get_user_agent()

    async def _rate_limit(self):
        """レート制限を適用"""
        now = time.time()
        elapsed = now - self._last_request_time
        min_delay = self._rate_config["min_delay"]
        max_delay = self._rate_config["max_delay"]
        delay = random.uniform(min_delay, max_delay)

        if elapsed < delay:
            wait = delay - elapsed
            logger.debug(f"[{self.site_name}] Rate limiting: waiting {wait:.1f}s")
            await asyncio.sleep(wait)

        self._last_request_time = time.time()
        self._request_count += 1

    async def _new_page(self) -> Page:
        """新しいページを作成（stealth適用）"""
        page = await self._context.new_page()
        if self._stealth:
            await self._stealth.apply_stealth_async(page)
        return page

    async def _safe_goto(self, page: Page, url: str, retries: int = 3) -> bool:
        """リトライ付きのページ遷移"""
        for attempt in range(retries):
            try:
                await self._rate_limit()
                response = await page.goto(url, wait_until="domcontentloaded")
                if response and response.status < 400:
                    return True
                logger.warning(
                    f"[{self.site_name}] HTTP {response.status if response else 'None'} "
                    f"for {url} (attempt {attempt + 1})"
                )
            except TimeoutError:
                logger.warning(
                    f"[{self.site_name}] Timeout navigating to {url} "
                    f"(attempt {attempt + 1}/{retries})"
                )
            except ConnectionError as e:
                logger.warning(
                    f"[{self.site_name}] Connection error for {url} "
                    f"(attempt {attempt + 1}/{retries}): {e}"
                )
            except Exception as e:
                logger.warning(
                    f"[{self.site_name}] Unexpected error navigating to {url} "
                    f"(attempt {attempt + 1}/{retries}): {type(e).__name__}: {e}"
                )
            if attempt < retries - 1:
                wait = (2 ** attempt) * random.uniform(1, 2)
                await asyncio.sleep(wait)
        return False

    @abstractmethod
    async def search(self, building_name: str, ward: str) -> list[ListingResult]:
        """
        建物名と区でサイトを検索し、掲載情報を返す。

        Args:
            building_name: 検索する建物名
            ward: 区名（例: "中央区"）

        Returns:
            ListingResult のリスト
        """
        ...

    def _parse_rent(self, text: str) -> Optional[int]:
        """家賃テキストを整数（円）に変換"""
        if not text:
            return None
        text = text.replace(",", "").replace("，", "").replace(" ", "")
        # "6.5万円" -> 65000
        m = re.search(r"([\d.]+)\s*万", text)
        if m:
            return int(float(m.group(1)) * 10000)
        # "65000円" -> 65000
        m = re.search(r"(\d+)\s*円", text)
        if m:
            return int(m.group(1))
        return None

    def _parse_area(self, text: str) -> Optional[float]:
        """面積テキストをfloatに変換"""
        if not text:
            return None
        m = re.search(r"([\d.]+)\s*[㎡m²]", text)
        if m:
            return float(m.group(1))
        return None

    def _parse_walk_minutes(self, text: str) -> Optional[int]:
        """徒歩分テキストを整数に変換"""
        if not text:
            return None
        m = re.search(r"徒歩\s*(\d+)\s*分", text)
        if m:
            return int(m.group(1))
        return None
