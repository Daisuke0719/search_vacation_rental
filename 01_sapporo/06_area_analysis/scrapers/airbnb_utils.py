"""Airbnb ユーティリティ関数"""

import re
from typing import Optional

try:
    from .airbnb_db import AirbnbDetail, AirbnbListing
except ImportError:
    from airbnb_db import AirbnbDetail, AirbnbListing


def find_nested_key(data, key):
    """ネストされたdict/listから特定のキーを再帰的に探す"""
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for v in data.values():
            result = find_nested_key(v, key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_nested_key(item, key)
            if result is not None:
                return result
    return None


def parse_listing_from_json(item: dict, area: dict) -> Optional[AirbnbListing]:
    """JSONオブジェクトからAirbnbListing を構築"""
    if not isinstance(item, dict):
        return None

    listing = AirbnbListing(
        listing_url="",
        ward=area["ward"],
        search_area=area["name"],
    )

    # URL/ID
    listing_id = item.get("id") or item.get("listingId") or find_nested_key(item, "id")
    if listing_id:
        listing.listing_url = f"https://www.airbnb.jp/rooms/{listing_id}"

    # タイトル
    listing.listing_title = item.get("name") or item.get("title") or \
                            find_nested_key(item, "name") or ""

    # 価格
    price_data = item.get("pricingQuote") or item.get("pricing") or \
                 find_nested_key(item, "pricingQuote")
    if isinstance(price_data, dict):
        rate = price_data.get("rate") or price_data.get("price")
        if isinstance(rate, dict):
            listing.nightly_price = rate.get("amount")
        elif isinstance(rate, (int, float)):
            listing.nightly_price = int(rate)

    # 評価
    listing.rating = item.get("avgRating") or find_nested_key(item, "avgRating")

    # レビュー数
    listing.review_count = item.get("reviewsCount") or find_nested_key(item, "reviewsCount")

    # 定員
    listing.guest_capacity = item.get("personCapacity") or find_nested_key(item, "personCapacity")

    # 寝室数
    listing.bedrooms = item.get("bedrooms") or find_nested_key(item, "bedrooms")

    # プロパティタイプ
    listing.property_type = item.get("roomType") or item.get("roomTypeCategory") or ""

    # スーパーホスト
    listing.superhost = bool(item.get("isSuperhost") or find_nested_key(item, "isSuperhost"))

    if listing.listing_url:
        return listing
    return None


def parse_price(text: str) -> Optional[int]:
    """価格テキストから数値を抽出"""
    if not text:
        return None
    text = text.replace(",", "").replace("，", "").replace(" ", "").replace("\u00a5", "").replace("¥", "")
    # "8500" or "8,500円" or "￥8,500/泊"
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        if val > 100:  # 少なくとも100円以上
            return val
    return None


def parse_rating(text: str) -> Optional[float]:
    """評価テキストから数値を抽出"""
    if not text:
        return None
    m = re.search(r"([\d.]+)", text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 5:
            return val
    return None


def parse_review_count(text: str) -> Optional[int]:
    """レビュー数テキストから数値を抽出"""
    if not text:
        return None
    m = re.search(r"(\d+)", text.replace(",", ""))
    if m:
        return int(m.group(1))
    return None


def _parse_overview_text(text: str, detail: AirbnbDetail):
    """概要テキストからゲスト数・寝室数・バスルーム数を抽出"""
    if not text:
        return

    # ゲスト数: 「ゲスト2人」「2 guests」
    m = re.search(r"ゲスト\s*(\d+)\s*人|(\d+)\s*guests?", text)
    if m and detail.guest_capacity is None:
        detail.guest_capacity = int(m.group(1) or m.group(2))

    # 寝室数: 「1ベッドルーム」「1 bedroom」
    m = re.search(r"(\d+)\s*ベッドルーム|(\d+)\s*bedrooms?", text)
    if m and detail.bedrooms is None:
        detail.bedrooms = int(m.group(1) or m.group(2))

    # バスルーム数: 「1バスルーム」「1 bathroom」
    m = re.search(r"(\d+)\s*バスルーム|(\d+)\s*bathrooms?", text)
    if m and detail.bathrooms is None:
        detail.bathrooms = int(m.group(1) or m.group(2))
