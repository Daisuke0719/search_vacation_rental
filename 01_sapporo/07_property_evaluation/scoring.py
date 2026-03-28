"""物件スコアリング - 収益性・立地・需要・品質・リスクの総合評価。"""

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import WARD_LOCATION_SCORE


# =====================================================================
# 7. 物件スコアリング
# =====================================================================

def score_property(
    rental: pd.Series,
    revenue_result: dict,
    similar_count: int,
    floor_plan_info: dict,
    similar_properties: pd.DataFrame,
) -> dict:
    """物件を100点満点でスコアリングする（連続関数方式）。

    配点:
      - 収益性 (30点): 利益率・ROI・コスト効率の連続スケール
      - 立地 (20点): 区スコア + 家賃単価から推定するミクロ立地 + 駅距離
      - 需要安定性 (15点): comp充実度・レビュー密度・評価の連続スケール
      - 物件クオリティ (20点): 築年数・バストイレ別・面積適合度・ゲスト効率
      - リスク (15点): 損益分岐ハードル・競合飽和度・固定費率

    Returns:
        スコア詳細dict
    """
    scores = {}
    area_sqm = rental.get("area_sqm") or 0
    rent = rental.get("rent_price") or 0

    # --- 収益性 (30点) ---
    annual_revenue = revenue_result.get("annual_revenue", 0)
    annual_profit = revenue_result.get("annual_profit", 0)
    annual_cost = revenue_result.get("annual_cost", 1)

    # 利益率 (profit / revenue): 0-12pt
    if annual_revenue > 0:
        profit_margin = annual_profit / annual_revenue
    else:
        profit_margin = -1.0
    profit_margin_score = _linear_scale(profit_margin, -0.3, 0.5, 0, 12)

    # ROI: 0-13pt
    roi = revenue_result.get("annual_roi", 0)
    roi_score = _linear_scale(roi, -10, 40, 0, 13)

    # コスト効率: revenue / total_cost: 0-5pt
    if annual_cost > 0:
        cost_efficiency = annual_revenue / annual_cost
    else:
        cost_efficiency = 0
    cost_eff_score = _linear_scale(cost_efficiency, 0.5, 2.0, 0, 5)

    scores["profitability"] = round(profit_margin_score + roi_score + cost_eff_score, 1)

    # --- 立地 (20点) ---
    ward = rental.get("ward", "")
    # 区ベーススコア: 0-8pt
    ward_raw = WARD_LOCATION_SCORE.get(ward, 10)
    ward_score = _linear_scale(ward_raw, 10, 25, 2, 8)

    # ミクロ立地推定: 家賃単価(円/㎡): 0-7pt
    if area_sqm > 0 and rent > 0:
        rent_per_sqm = rent / area_sqm
    else:
        rent_per_sqm = 1500
    micro_location_score = _linear_scale(rent_per_sqm, 1000, 2800, 1, 7)

    # 駅距離: 0-5pt
    walk_min = rental.get("walk_minutes")
    if walk_min is not None and not pd.isna(walk_min):
        walk_min = float(walk_min)
        walk_score = _linear_scale(walk_min, 20, 3, 0, 5)
    else:
        walk_score = 2.0  # 不明=やや低め（データ欠損ペナルティ）

    scores["location"] = round(min(20, ward_score + micro_location_score + walk_score), 1)

    # --- 需要安定性 (15点) ---
    # comp物件の充実度: 0-5pt
    comp_count_score = _linear_scale(similar_count, 0, 30, 0, 5)

    # compレビュー密度: 0-5pt
    review_density_score = 1.0
    if not similar_properties.empty and "comp_review_count" in similar_properties.columns:
        reviews = similar_properties["comp_review_count"].dropna()
        if len(reviews) > 0:
            avg_reviews = reviews.mean()
            review_density_score = _linear_scale(avg_reviews, 0, 80, 0, 5)

    # comp評価スコア: 0-5pt
    rating_score = 1.5
    if not similar_properties.empty and "comp_rating" in similar_properties.columns:
        ratings = similar_properties["comp_rating"].dropna()
        if len(ratings) > 0:
            avg_rating = ratings.mean()
            rating_score = _linear_scale(avg_rating, 3.0, 4.8, 0, 5)

    scores["demand_stability"] = round(min(15, comp_count_score + review_density_score + rating_score), 1)

    # --- 物件クオリティ (20点) ---
    # 築年数: 0-7pt（民泊ゲストは清潔感・新しさを重視）
    building_age = _parse_building_age(rental.get("building_age"))
    if building_age is not None:
        # 新築→7pt, 10年→5pt, 25年→2pt, 45年以上→0pt
        age_quality_score = _linear_scale(building_age, 45, 0, 0, 7)
    else:
        age_quality_score = 2.5  # 不明=低めの中間（データ欠損ペナルティ）

    # バストイレ別: 0-5pt（ゲスト満足度・レビュー評価に直結）
    bath_toilet = rental.get("bath_toilet_separate")
    if bath_toilet is not None and not pd.isna(bath_toilet):
        bath_toilet = int(bath_toilet)
        if bath_toilet == 1:
            bt_score = 5.0   # バストイレ別 → 高評価
        else:
            bt_score = 1.0   # ユニットバス → 低評価
    else:
        bt_score = 2.5  # 不明=中間

    # 面積適合度: 0-4pt（民泊に最適な面積帯は25-55㎡）
    if area_sqm > 0:
        if area_sqm <= 45:
            area_fit = _linear_scale(area_sqm, 15, 35, 0.5, 4)
        else:
            area_fit = _linear_scale(area_sqm, 130, 45, 0.5, 4)
    else:
        area_fit = 2.0

    # ゲスト収容効率: 0-4pt（ゲスト1人あたり家賃が安いほど効率的）
    capacity = floor_plan_info.get("estimated_capacity", 2)
    rent_per_guest = rent / capacity if capacity > 0 and rent > 0 else 50000
    guest_eff_score = _linear_scale(rent_per_guest, 45000, 12000, 0, 4)

    scores["property_quality"] = round(min(20, age_quality_score + bt_score + area_fit + guest_eff_score), 1)

    # --- リスク (15点: 高いほどリスクが低い=良い) ---
    # 損益分岐余裕度: 0-6pt
    if annual_revenue > 0:
        breakeven_margin = annual_profit / annual_revenue
        breakeven_score = _linear_scale(breakeven_margin, -0.2, 0.4, 0, 6)
    else:
        breakeven_score = 0

    # 競合飽和度: 0-4pt
    if similar_count <= 10:
        competition_score = _linear_scale(similar_count, 0, 10, 1, 4)
    else:
        competition_score = _linear_scale(similar_count, 50, 10, 0.5, 4)

    # 固定費率: 月額固定費 / 想定月間売上  0-5pt
    # 固定費が重いほど稼働率低下時のリスクが高い
    monthly_fixed = (rent or 0)
    mgmt_fee = rental.get("management_fee")
    if mgmt_fee is not None and not pd.isna(mgmt_fee):
        monthly_fixed += int(mgmt_fee)
    monthly_revenue_est = annual_revenue / 12 if annual_revenue > 0 else 1
    fixed_cost_ratio = monthly_fixed / monthly_revenue_est if monthly_revenue_est > 0 else 1.0
    # 0.2(固定費が売上の20%)→5pt, 0.5→2.5pt, 0.8以上→0pt
    fixed_cost_score = _linear_scale(fixed_cost_ratio, 0.8, 0.2, 0, 5)

    scores["risk"] = round(min(15, breakeven_score + competition_score + fixed_cost_score), 1)

    # 合計
    scores["total"] = round(sum(scores.values()), 1)

    return scores


def _linear_scale(value: float, low: float, high: float, out_low: float, out_high: float) -> float:
    """値を[low, high]から[out_low, out_high]に線形変換する。範囲外はクランプ。"""
    if high == low:
        return (out_low + out_high) / 2
    ratio = (value - low) / (high - low)
    ratio = max(0.0, min(1.0, ratio))
    return out_low + ratio * (out_high - out_low)


def _parse_building_age(age_str: Optional[str]) -> Optional[int]:
    """築年数表記をパースする。"築5年" → 5, "2010年3月" → 16 等。"""
    if not age_str or not isinstance(age_str, str):
        return None

    # "築X年" パターン
    m = re.search(r"築\s*(\d+)\s*年", age_str)
    if m:
        return int(m.group(1))

    # "YYYY年" パターン（建築年）
    m = re.search(r"(\d{4})\s*年", age_str)
    if m:
        built_year = int(m.group(1))
        current_year = datetime.now().year
        age = current_year - built_year
        return max(0, age)

    # 数値のみ
    m = re.search(r"(\d+)", age_str)
    if m:
        val = int(m.group(1))
        if val > 100:
            # 年号の可能性
            return max(0, datetime.now().year - val)
        return val

    return None
