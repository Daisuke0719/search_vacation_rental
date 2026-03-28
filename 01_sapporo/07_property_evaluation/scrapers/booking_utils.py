"""Booking.com スクレイパー — ユーティリティ関数"""

import re
from typing import Optional


def parse_price(text: str) -> Optional[int]:
    """価格テキストから1泊あたりの金額（JPY）を抽出

    Booking.comの価格表記例:
    - "￥8,500"
    - "¥ 12,000"
    - "8500 円"
    """
    if not text:
        return None
    # 通貨記号・カンマ・空白を除去
    cleaned = text.replace(",", "").replace("，", "").replace(" ", "")
    cleaned = cleaned.replace("￥", "").replace("¥", "").replace("円", "")

    # 数値を抽出（複数ある場合は最初のもの）
    m = re.search(r"(\d+)", cleaned)
    if m:
        val = int(m.group(1))
        if val >= 1000:  # 1泊1000円以上を有効とする
            return val
    return None


def parse_booking_rating(text: str) -> Optional[float]:
    """Booking.comの評価スコア（10点満点）を抽出

    表記例: "8.5", "スコア: 9.2", "9.2"
    """
    if not text:
        return None
    m = re.search(r"(\d+\.?\d*)", text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 10:
            return val
    return None


def parse_review_count(text: str) -> Optional[int]:
    """レビュー数テキストから数値を抽出

    表記例: "1,234件のレビュー", "レビュー234件", "234 reviews"
    """
    if not text:
        return None
    cleaned = text.replace(",", "").replace("，", "")
    m = re.search(r"(\d+)", cleaned)
    if m:
        return int(m.group(1))
    return None
