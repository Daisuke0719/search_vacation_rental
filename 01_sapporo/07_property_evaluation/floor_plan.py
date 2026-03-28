"""間取りパーサー - 日本の間取り表記をパースし部屋数・定員等を推定する。"""

import re
from typing import Optional


def parse_floor_plan(plan_str: Optional[str]) -> dict:
    """日本の間取り表記をパースし、部屋数・定員等を推定する。

    Args:
        plan_str: "1K", "1LDK", "2DK", "ワンルーム" など

    Returns:
        dict: rooms, has_living, has_dining, has_kitchen,
              estimated_bedrooms, estimated_capacity, plan_type
    """
    result = {
        "rooms": 1,
        "has_living": False,
        "has_dining": False,
        "has_kitchen": False,
        "estimated_bedrooms": 1,
        "estimated_capacity": 2,
        "plan_type": "unknown",
    }

    if not plan_str or not isinstance(plan_str, str):
        return result

    plan = plan_str.strip().upper()

    # ワンルーム
    if "ワンルーム" in plan_str or plan == "1R":
        result.update({
            "rooms": 1, "has_kitchen": False,
            "estimated_bedrooms": 1, "estimated_capacity": 2,
            "plan_type": "ワンルーム",
        })
        return result

    # 標準パターン: 数字 + (L)(D)(K)(+S)
    m = re.match(r"(\d+)\s*([SLDK]*)", plan)
    if not m:
        return result

    num_rooms = int(m.group(1))
    suffix = m.group(2)

    has_l = "L" in suffix
    has_d = "D" in suffix
    has_k = "K" in suffix

    result["rooms"] = num_rooms
    result["has_living"] = has_l
    result["has_dining"] = has_d
    result["has_kitchen"] = has_k
    result["plan_type"] = plan_str.strip()

    # 寝室数 = 部屋数（LDK部分はリビングとして使用想定）
    result["estimated_bedrooms"] = num_rooms

    # 定員推定
    capacity_map = {
        (1, False, False, True):  2,   # 1K
        (1, False, True,  True):  3,   # 1DK
        (1, True,  True,  True):  3,   # 1LDK
        (2, False, True,  True):  4,   # 2DK
        (2, True,  True,  True):  4,   # 2LDK
        (3, False, True,  True):  6,   # 3DK
        (3, True,  True,  True):  6,   # 3LDK
        (4, True,  True,  True):  8,   # 4LDK
    }

    key = (num_rooms, has_l, has_d, has_k)
    if key in capacity_map:
        result["estimated_capacity"] = capacity_map[key]
    else:
        # フォールバック: 部屋数 * 2
        result["estimated_capacity"] = max(2, num_rooms * 2)

    return result
