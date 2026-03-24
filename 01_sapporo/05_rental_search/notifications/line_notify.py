"""LINE Messaging APIによる通知モジュール"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
from config import LINE_CHANNEL_TOKEN, LINE_USER_ID

logger = logging.getLogger(__name__)

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"


def send_line_message(message: str) -> bool:
    """LINEメッセージを送信"""
    if not LINE_CHANNEL_TOKEN or not LINE_USER_ID:
        logger.warning(
            "LINE credentials not configured. "
            "Set LINE_CHANNEL_TOKEN and LINE_USER_ID in .env"
        )
        return False

    if LINE_CHANNEL_TOKEN.startswith("your_"):
        logger.warning("LINE_CHANNEL_TOKEN is not configured (still placeholder)")
        return False

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": LINE_USER_ID,
        "messages": [{"type": "text", "text": message[:5000]}],
    }

    try:
        response = httpx.post(LINE_PUSH_URL, json=payload, headers=headers, timeout=30)
        if response.status_code == 200:
            logger.info("LINE message sent successfully")
            return True
        else:
            logger.error(
                f"LINE API error: {response.status_code} {response.text}"
            )
            return False
    except Exception as e:
        logger.error(f"Failed to send LINE message: {e}")
        return False


def format_new_listings_message(listings: list) -> str:
    """新着掲載をLINEメッセージにフォーマット"""
    msg = f"【民泊物件 新着通知】\n"
    msg += f"新しい賃貸掲載が{len(listings)}件見つかりました。\n\n"

    for i, listing in enumerate(listings[:10]):
        building_name = listing["building_name"] if isinstance(listing, dict) else listing[1]
        site_name = listing.get("site_name", "") if isinstance(listing, dict) else listing[4]
        rent = listing.get("rent_price", "") if isinstance(listing, dict) else listing[6]
        floor_plan = listing.get("floor_plan", "") if isinstance(listing, dict) else listing[11]
        url = listing.get("listing_url", "") if isinstance(listing, dict) else listing[5]
        ward = listing.get("ward", "") if isinstance(listing, dict) else listing[2]

        msg += f"■ {building_name} ({ward})\n"
        if rent:
            msg += f"  {site_name} | {rent:,}円/月"
        else:
            msg += f"  {site_name}"
        if floor_plan:
            msg += f" | {floor_plan}"
        msg += f"\n  {url}\n\n"

    if len(listings) > 10:
        msg += f"...他{len(listings) - 10}件\n"
    msg += "詳細はダッシュボードをご確認ください。"

    return msg


def send_new_listing_notification(listings: list) -> bool:
    """新着掲載のLINE通知を送信"""
    if not listings:
        return True

    # sqlite3.Row をdict化
    dict_listings = []
    for row in listings:
        try:
            dict_listings.append(dict(row))
        except Exception:
            dict_listings.append(row)

    message = format_new_listings_message(dict_listings)
    return send_line_message(message)
