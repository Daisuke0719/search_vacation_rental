"""Playwright ブラウザ共通設定

各スクレイパーで重複していたブラウザ起動・stealth設定を一元化。
"""

import logging
import random
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

# ── ブラウザ設定定数 ──
DEFAULT_VIEWPORT = {"width": 1280, "height": 800}
DEFAULT_LOCALE = "ja-JP"
DEFAULT_TIMEZONE = "Asia/Tokyo"
DEFAULT_TIMEOUT = 30000  # ms

FALLBACK_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def get_user_agent() -> str:
    """ランダムなUser-Agentを返す（fake_useragent利用、フォールバック付き）"""
    try:
        from fake_useragent import UserAgent
        ua = UserAgent()
        return ua.chrome
    except Exception:
        return random.choice(FALLBACK_USER_AGENTS)


def _load_stealth():
    """playwright-stealth をロードする。未インストール時はNoneを返す。"""
    try:
        from playwright_stealth import Stealth
        return Stealth()
    except ImportError:
        logger.debug("playwright-stealth not installed, skipping stealth")
        return None


async def create_browser_context(
    playwright,
    headless: bool = True,
    timeout: int = DEFAULT_TIMEOUT,
    extra_http_headers: Optional[dict] = None,
) -> tuple[Browser, BrowserContext]:
    """標準設定でブラウザとコンテキストを作成する。

    Returns:
        (browser, context) のタプル
    """
    browser = await playwright.chromium.launch(headless=headless)

    context_kwargs = {
        "viewport": DEFAULT_VIEWPORT,
        "locale": DEFAULT_LOCALE,
        "timezone_id": DEFAULT_TIMEZONE,
        "user_agent": get_user_agent(),
    }
    if extra_http_headers:
        context_kwargs["extra_http_headers"] = extra_http_headers

    context = await browser.new_context(**context_kwargs)
    context.set_default_timeout(timeout)
    return browser, context


async def new_stealth_page(context: BrowserContext) -> Page:
    """stealth対策を適用した新しいページを作成する。"""
    page = await context.new_page()
    stealth = _load_stealth()
    if stealth:
        await stealth.apply_stealth_async(page)
    return page
