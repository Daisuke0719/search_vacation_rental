"""収益シミュレーション - ADR推計・稼働率推計・月次収益計算。"""

import re
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    SEASON_MONTHS, SEASON_MULTIPLIER, DAYS_IN_MONTH,
    WARD_OCCUPANCY_BASELINE,
    CLEANING_COST_PER_TURNOVER, TURNOVER_RATIO, OTA_COMMISSION_RATE,
    UTILITIES_MONTHLY, CONSUMABLES_MONTHLY, WIFI_SUBSCRIPTIONS_MONTHLY,
    MANAGEMENT_OUTSOURCE_RATE,
    INITIAL_FURNITURE, INITIAL_FIRE_SAFETY, INITIAL_REGISTRATION,
    AMORTIZATION_MONTHS, BUSINESS_TYPES,
    FALLBACK_ADR, REVIEW_RATE, AVG_STAY_NIGHTS,
)


# =====================================================================
# 4. ADR推計
# =====================================================================

def estimate_adr(
    similar_properties: pd.DataFrame,
    floor_plan_info: dict,
) -> dict:
    """類似物件の価格データから季節別ADRを推計する。

    類似度スコアで重み付けした中央値を算出し、
    季節倍率を適用する。

    データ不足時は config.FALLBACK_ADR を使用する。
    """
    result = {"peak_adr": 0, "normal_adr": 0, "offpeak_adr": 0, "weighted_avg_adr": 0}

    if similar_properties.empty or similar_properties["comp_price"].dropna().empty:
        # フォールバック: シミュレーションパラメータから取得
        plan_type = floor_plan_info.get("plan_type", "default")
        fallback_key = _match_fallback_key(plan_type)
        adr = FALLBACK_ADR.get(fallback_key, FALLBACK_ADR["default"])
        result["peak_adr"] = adr["peak"]
        result["normal_adr"] = adr["normal"]
        result["offpeak_adr"] = adr["offpeak"]
        result["weighted_avg_adr"] = _calc_weighted_avg_adr(result)
        result["method"] = "fallback"
        return result

    # 類似度で重み付けした中央値（加重中央値）
    prices = similar_properties["comp_price"].dropna().values
    weights = similar_properties.loc[
        similar_properties["comp_price"].notna(), "similarity_score"
    ].values

    comp_base_price = _weighted_median(prices, weights)

    # comp物件の寝室・定員データの充実度を確認
    # データがNULLばかりの場合、マッチング精度が低いためフォールバックとブレンド
    has_bedrooms = similar_properties["comp_bedrooms"].notna().sum()
    has_capacity = similar_properties["comp_capacity"].notna().sum()
    total_comps = len(similar_properties)
    data_quality = (has_bedrooms + has_capacity) / (total_comps * 2) if total_comps > 0 else 0

    # フォールバックADRも取得
    plan_type = floor_plan_info.get("plan_type", "default")
    fallback_key = _match_fallback_key(plan_type)
    fallback_adr = FALLBACK_ADR.get(fallback_key, FALLBACK_ADR["default"])

    # データ品質に応じてcomp基準ADRとフォールバックをブレンド
    # data_quality=1.0 → comp 100%, data_quality=0.0 → comp 30% + fallback 70%
    comp_weight = 0.3 + 0.7 * data_quality

    for season_key, mult_key in [("peak_adr", "peak"), ("normal_adr", "normal"), ("offpeak_adr", "offpeak")]:
        comp_val = comp_base_price * SEASON_MULTIPLIER[mult_key]
        fallback_val = fallback_adr[mult_key]
        result[season_key] = int(comp_val * comp_weight + fallback_val * (1 - comp_weight))

    result["weighted_avg_adr"] = _calc_weighted_avg_adr(result)
    result["method"] = f"blended(comp={comp_weight:.0%})"
    result["data_quality"] = round(data_quality, 2)

    return result


def _match_fallback_key(plan_type: str) -> str:
    """間取り表記からフォールバックADRのキーを特定する。"""
    plan_type = plan_type.strip().upper()
    for key in ["3LDK", "2LDK", "2DK", "1LDK", "1K"]:
        if key in plan_type:
            return key
    if "ワンルーム" in plan_type or "1R" in plan_type:
        return "1K"
    return "default"


def _weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    """重み付き中央値を計算する。"""
    if len(values) == 0:
        return 0.0
    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]
    cumulative = np.cumsum(sorted_weights)
    midpoint = cumulative[-1] / 2.0
    idx = np.searchsorted(cumulative, midpoint)
    return float(sorted_values[min(idx, len(sorted_values) - 1)])


def _calc_weighted_avg_adr(adr_dict: dict) -> int:
    """季節別ADRの加重平均を算出する（月数ベース）。"""
    peak_months = len(SEASON_MONTHS["peak"])
    normal_months = len(SEASON_MONTHS["normal"])
    offpeak_months = len(SEASON_MONTHS["offpeak"])
    total_months = peak_months + normal_months + offpeak_months

    avg = (
        adr_dict["peak_adr"] * peak_months
        + adr_dict["normal_adr"] * normal_months
        + adr_dict["offpeak_adr"] * offpeak_months
    ) / total_months

    return int(avg)


# =====================================================================
# 5. 稼働率推計
# =====================================================================

def estimate_occupancy(
    similar_properties: pd.DataFrame,
    ward: str,
) -> dict:
    """類似物件のレビュー数と区ベースラインから稼働率を推計する。

    方法1: Airbnbレビュー数から逆算
        monthly_bookings = review_count / months_listed / review_rate
        occupancy = monthly_bookings * avg_stay / 30

    方法2: 区レベルのベースライン稼働率

    両方をブレンドして季節別稼働率を返す。
    """
    baseline = WARD_OCCUPANCY_BASELINE.get(ward, 0.45)

    # 方法1: レビューからの推計
    review_occupancy = None
    if not similar_properties.empty:
        airbnb_comps = similar_properties[
            (similar_properties["source"] == "airbnb")
            & (similar_properties["comp_review_count"].notna())
            & (similar_properties["comp_review_count"] > 0)
        ]
        if not airbnb_comps.empty:
            avg_reviews = airbnb_comps["comp_review_count"].mean()
            # 仮に平均12ヶ月掲載と推定
            months_listed = 12
            monthly_bookings = avg_reviews / months_listed / REVIEW_RATE
            review_occupancy = min(0.95, monthly_bookings * AVG_STAY_NIGHTS / 30)

    # ブレンド
    if review_occupancy is not None:
        # レビュー推計 60%, ベースライン 40%
        blended = review_occupancy * 0.6 + baseline * 0.4
    else:
        blended = baseline

    # 妥当な範囲にクリップ
    blended = max(0.20, min(0.85, blended))

    # 季節別稼働率
    result = {
        "peak_occupancy": min(0.95, blended * 1.30),
        "normal_occupancy": blended,
        "offpeak_occupancy": max(0.15, blended * 0.65),
        "annual_avg_occupancy": blended,
        "method": "blended" if review_occupancy is not None else "baseline_only",
    }

    return result


# =====================================================================
# 6. 収益シミュレーション
# =====================================================================

def simulate_revenue(
    rental: pd.Series,
    adr_estimates: dict,
    occupancy_estimates: dict,
    business_type: str = "minpaku",
    self_managed: bool = False,
) -> dict:
    """月次収益シミュレーションを実行する。

    Args:
        rental: 賃貸物件データ
        adr_estimates: 季節別ADR推計
        occupancy_estimates: 季節別稼働率推計
        business_type: "minpaku" (180日) or "kaniyado" (365日)
        self_managed: True=自主管理（運営代行なし）

    Returns:
        月次・年間の収益サマリー
    """
    max_days = BUSINESS_TYPES.get(business_type, 180)
    rent = rental.get("rent_price") or 0
    mgmt_fee = rental.get("management_fee") or 0
    # NaN対策
    if pd.isna(rent):
        rent = 0
    if pd.isna(mgmt_fee):
        mgmt_fee = 0
    rent = int(rent)
    mgmt_fee = int(mgmt_fee)

    # 敷金・礼金のパース（"1ヶ月" → 家賃の倍数）
    deposit_raw = rental.get("deposit")
    key_money_raw = rental.get("key_money")
    # NaN/None対策
    if pd.isna(deposit_raw):
        deposit_raw = None
    if pd.isna(key_money_raw):
        key_money_raw = None
    deposit_amount = _parse_money_months(deposit_raw, rent)
    key_money_amount = _parse_money_months(key_money_raw, rent)

    # 初期費用（月割り償却）
    initial_total = (
        INITIAL_FURNITURE
        + INITIAL_FIRE_SAFETY
        + INITIAL_REGISTRATION
        + deposit_amount
        + key_money_amount
    )
    monthly_amortization = initial_total / AMORTIZATION_MONTHS

    # 月次計算
    monthly_data = []
    annual_revenue = 0
    annual_cost = 0
    annual_operating_days = 0
    remaining_days = max_days

    # 民泊の場合、繁忙期を優先配分
    month_order = _prioritize_months(business_type)

    for month in month_order:
        season = _get_season(month)
        days_in_m = DAYS_IN_MONTH[month]

        # 残り営業日数でキャップ
        available_days = min(days_in_m, remaining_days)
        if available_days <= 0:
            monthly_data.append(_empty_month(month, rent, mgmt_fee, monthly_amortization, self_managed))
            continue

        # 稼働率とADR
        occ_key = f"{season}_occupancy"
        adr_key = f"{season}_adr"
        occupancy = occupancy_estimates.get(occ_key, 0.5)
        adr = adr_estimates.get(adr_key, 7500)

        operating_days = available_days * occupancy
        revenue = adr * operating_days

        # コスト
        turnovers = operating_days * TURNOVER_RATIO
        cleaning = CLEANING_COST_PER_TURNOVER * turnovers
        ota_fee = revenue * OTA_COMMISSION_RATE
        outsource_fee = revenue * MANAGEMENT_OUTSOURCE_RATE if not self_managed else 0

        total_cost = (
            rent + mgmt_fee
            + cleaning
            + ota_fee
            + UTILITIES_MONTHLY
            + CONSUMABLES_MONTHLY
            + WIFI_SUBSCRIPTIONS_MONTHLY
            + outsource_fee
            + monthly_amortization
        )

        profit = revenue - total_cost

        monthly_data.append({
            "month": month,
            "season": season,
            "available_days": available_days,
            "occupancy": round(occupancy, 3),
            "operating_days": round(operating_days, 1),
            "adr": adr,
            "revenue": int(revenue),
            "rent": rent,
            "management_fee": mgmt_fee,
            "cleaning": int(cleaning),
            "ota_fee": int(ota_fee),
            "utilities": UTILITIES_MONTHLY,
            "consumables": CONSUMABLES_MONTHLY,
            "wifi_subs": WIFI_SUBSCRIPTIONS_MONTHLY,
            "outsource_fee": int(outsource_fee),
            "amortization": int(monthly_amortization),
            "total_cost": int(total_cost),
            "profit": int(profit),
        })

        remaining_days -= available_days
        annual_revenue += revenue
        annual_cost += total_cost
        annual_operating_days += operating_days

    # 月番号順にソート
    monthly_data.sort(key=lambda x: x["month"])

    annual_profit = annual_revenue - annual_cost
    annual_investment = initial_total + (rent + mgmt_fee) * 12
    roi = (annual_profit / annual_investment * 100) if annual_investment > 0 else 0

    return {
        "monthly": monthly_data,
        "annual_revenue": int(annual_revenue),
        "annual_cost": int(annual_cost),
        "annual_profit": int(annual_profit),
        "annual_operating_days": round(annual_operating_days, 1),
        "initial_investment": int(initial_total),
        "annual_roi": round(roi, 1),
        "business_type": business_type,
        "self_managed": self_managed,
    }


def _empty_month(
    month: int, rent: int, mgmt_fee: int,
    monthly_amortization: float, self_managed: bool,
) -> dict:
    """営業日ゼロの月のデータを生成する。"""
    total_cost = (
        rent + mgmt_fee
        + UTILITIES_MONTHLY + CONSUMABLES_MONTHLY + WIFI_SUBSCRIPTIONS_MONTHLY
        + int(monthly_amortization)
    )
    return {
        "month": month, "season": _get_season(month),
        "available_days": 0, "occupancy": 0, "operating_days": 0,
        "adr": 0, "revenue": 0,
        "rent": rent, "management_fee": mgmt_fee,
        "cleaning": 0, "ota_fee": 0,
        "utilities": UTILITIES_MONTHLY,
        "consumables": CONSUMABLES_MONTHLY,
        "wifi_subs": WIFI_SUBSCRIPTIONS_MONTHLY,
        "outsource_fee": 0,
        "amortization": int(monthly_amortization),
        "total_cost": total_cost,
        "profit": -total_cost,
    }


def _parse_money_months(value: Optional[str], base_rent: int) -> int:
    """敷金・礼金の表記をパースする。

    "1ヶ月" → base_rent * 1, "なし" → 0, "50000" → 50000 等。
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0
    if not isinstance(value, str):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    value = value.strip()
    if value in ("なし", "-", "無", "0", ""):
        return 0

    # "4.4万円" パターン
    m = re.search(r"([\d.]+)\s*万円?", value)
    if m:
        return int(float(m.group(1)) * 10_000)

    # "1ヶ月", "2ヶ月" パターン
    m = re.search(r"([\d.]+)\s*[ヶか月]", value)
    if m:
        return int(float(m.group(1)) * base_rent)

    # 数値のみ
    m = re.search(r"([\d,]+)", value.replace(",", ""))
    if m:
        return int(m.group(1))

    return 0


def _get_season(month: int) -> str:
    """月から季節区分を返す。"""
    for season, months in SEASON_MONTHS.items():
        if month in months:
            return season
    return "normal"


def _prioritize_months(business_type: str) -> list[int]:
    """事業形態に応じた月の営業優先順を返す。

    民泊の場合、繁忙期 → 通常期 → 閑散期の順に日数を配分。
    簡易宿所の場合は全月が対象なので1-12月順。
    """
    if business_type == "kaniyado":
        return list(range(1, 13))

    # 民泊: 繁忙期を優先
    return (
        SEASON_MONTHS["peak"]
        + SEASON_MONTHS["normal"]
        + SEASON_MONTHS["offpeak"]
    )
