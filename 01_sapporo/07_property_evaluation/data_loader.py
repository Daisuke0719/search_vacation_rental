"""データ読み込み・類似物件マッチング - 賃貸・Airbnb・Booking DBからのデータロード。"""

import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RENTAL_DB_PATH, AIRBNB_DB_PATH, BOOKING_DB_PATH, WARD_ADJACENCY

from floor_plan import parse_floor_plan

logger = logging.getLogger(__name__)


# =====================================================================
# 1. データ読み込み
# =====================================================================

def _migrate_rental_db(conn: sqlite3.Connection):
    """賃貸DBに新カラムが無ければ追加する（マイグレーション）。"""
    cur = conn.execute("PRAGMA table_info(listings)")
    existing_cols = {row[1] for row in cur.fetchall()}
    if "bath_toilet_separate" not in existing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN bath_toilet_separate INTEGER")
        conn.commit()
        logger.info("DBマイグレーション: bath_toilet_separate カラムを追加")


def load_rental_listings() -> pd.DataFrame:
    """賃貸物件データベースからアクティブ物件を読み込む。

    buildings テーブルと listings テーブルを結合し、
    is_active=1 かつ検証で除外されていない物件を返す。
    """
    if not RENTAL_DB_PATH.exists():
        logger.warning(f"賃貸DBが見つかりません: {RENTAL_DB_PATH}")
        return pd.DataFrame()

    conn = sqlite3.connect(str(RENTAL_DB_PATH))

    # bath_toilet_separate カラムが存在しない場合はマイグレーション
    _migrate_rental_db(conn)

    query = """
        SELECT
            l.id AS listing_id,
            l.building_id,
            b.building_name,
            b.ward,
            b.address_base,
            l.site_name,
            l.listing_url,
            l.listing_title,
            l.rent_price,
            l.management_fee,
            l.deposit,
            l.key_money,
            l.floor_plan,
            l.area_sqm,
            l.floor_number,
            l.building_age,
            l.nearest_station,
            l.walk_minutes,
            l.bath_toilet_separate,
            l.first_seen_at,
            l.last_seen_at
        FROM listings l
        JOIN buildings b ON l.building_id = b.id
        LEFT JOIN listing_verifications v ON l.id = v.listing_id
        WHERE l.is_active = 1
          AND (v.status IS NULL OR v.status NOT IN ('mismatch', 'suspicious'))
        ORDER BY b.ward, l.rent_price
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    logger.info(f"賃貸物件: {len(df)}件読み込み")
    return df


def load_airbnb_comps() -> pd.DataFrame:
    """Airbnb物件データベースから類似物件候補を読み込む。"""
    if not AIRBNB_DB_PATH.exists():
        logger.warning(f"Airbnb DBが見つかりません: {AIRBNB_DB_PATH}")
        return pd.DataFrame()

    conn = sqlite3.connect(str(AIRBNB_DB_PATH))
    query = """
        SELECT
            id,
            listing_url,
            listing_title,
            nightly_price,
            rating,
            review_count,
            property_type,
            guest_capacity,
            bedrooms,
            ward,
            search_area,
            superhost,
            scraped_at
        FROM airbnb_listings
        WHERE nightly_price IS NOT NULL AND nightly_price > 0
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Airbnb検索結果の価格は数泊分の合計で表示されることが多い。
    # 札幌の1泊相場（5,000〜30,000円）を大きく超える場合は
    # 推定泊数（3泊）で割って1泊単価に補正する。
    AIRBNB_NIGHTLY_MAX = 35_000  # これ以上は合計価格と判断
    AIRBNB_ASSUMED_NIGHTS = 3    # Airbnb検索のデフォルト表示泊数
    if not df.empty and "nightly_price" in df.columns:
        mask = df["nightly_price"] > AIRBNB_NIGHTLY_MAX
        if mask.any():
            logger.info(
                f"Airbnb価格補正: {mask.sum()}件を{AIRBNB_ASSUMED_NIGHTS}泊合計→1泊単価に変換"
            )
            df.loc[mask, "nightly_price"] = (
                df.loc[mask, "nightly_price"] / AIRBNB_ASSUMED_NIGHTS
            ).astype(int)

    logger.info(f"Airbnb物件: {len(df)}件読み込み")
    return df


def load_booking_comps() -> pd.DataFrame:
    """Booking.com物件データベースから類似物件候補を読み込む。

    DBが存在しない場合は空のDataFrameを返す。
    """
    if not BOOKING_DB_PATH.exists():
        logger.info("Booking DBが見つかりません（スキップ）")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(str(BOOKING_DB_PATH))
        query = """
            SELECT *
            FROM booking_listings
            WHERE nightly_price IS NOT NULL AND nightly_price > 0
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info(f"Booking物件: {len(df)}件読み込み")
        return df
    except Exception as e:
        logger.warning(f"Booking DB読み込みエラー: {e}")
        return pd.DataFrame()


# =====================================================================
# 3. 類似物件マッチング
# =====================================================================

def find_similar_properties(
    rental: pd.Series,
    airbnb_df: pd.DataFrame,
    booking_df: pd.DataFrame,
) -> pd.DataFrame:
    """賃貸物件に類似するAirbnb/Booking物件を検索する。

    類似度スコア（0-1）を算出し、0.5以上の物件を返す。

    スコア構成:
      - 区一致 (weight=0.30): 同区=1.0, 隣接区=0.5, その他=0.0
      - 間取り一致 (weight=0.30): 同寝室数=1.0, ±1=0.5
      - 面積一致 (weight=0.20): 1 - |差|/max(面積)
      - 定員一致 (weight=0.20): 同=1.0, ±1=0.7, ±2=0.4
    """
    rental_plan = parse_floor_plan(rental.get("floor_plan"))
    rental_ward = rental.get("ward", "")
    rental_area = rental.get("area_sqm") or 0
    rental_bedrooms = rental_plan["estimated_bedrooms"]
    rental_capacity = rental_plan["estimated_capacity"]

    adjacent = WARD_ADJACENCY.get(rental_ward, [])

    all_comps = []

    # Airbnbデータの処理
    if not airbnb_df.empty:
        for _, comp in airbnb_df.iterrows():
            score = _calc_similarity(
                rental_ward, adjacent, rental_bedrooms, rental_area, rental_capacity,
                comp.get("ward", ""),
                comp.get("bedrooms") or 1,
                0,  # Airbnbに面積情報はない
                comp.get("guest_capacity") or 2,
            )
            if score >= 0.5:
                all_comps.append({
                    "source": "airbnb",
                    "comp_id": comp.get("id"),
                    "comp_title": comp.get("listing_title", ""),
                    "comp_ward": comp.get("ward", ""),
                    "comp_price": comp.get("nightly_price"),
                    "comp_bedrooms": comp.get("bedrooms"),
                    "comp_capacity": comp.get("guest_capacity"),
                    "comp_rating": comp.get("rating"),
                    "comp_review_count": comp.get("review_count"),
                    "comp_superhost": comp.get("superhost"),
                    "similarity_score": round(score, 3),
                })

    # Bookingデータの処理
    if not booking_df.empty:
        for _, comp in booking_df.iterrows():
            score = _calc_similarity(
                rental_ward, adjacent, rental_bedrooms, rental_area, rental_capacity,
                comp.get("ward", ""),
                comp.get("bedrooms") or 1,
                comp.get("area_sqm") or 0,
                comp.get("guest_capacity") or 2,
            )
            if score >= 0.5:
                all_comps.append({
                    "source": "booking",
                    "comp_id": comp.get("id"),
                    "comp_title": comp.get("listing_title", ""),
                    "comp_ward": comp.get("ward", ""),
                    "comp_price": comp.get("nightly_price"),
                    "comp_bedrooms": comp.get("bedrooms"),
                    "comp_capacity": comp.get("guest_capacity"),
                    "comp_rating": comp.get("rating"),
                    "comp_review_count": comp.get("review_count"),
                    "comp_superhost": None,
                    "similarity_score": round(score, 3),
                })

    if not all_comps:
        return pd.DataFrame()

    result = pd.DataFrame(all_comps)
    result.sort_values("similarity_score", ascending=False, inplace=True)
    return result.reset_index(drop=True)


def _calc_similarity(
    rental_ward: str, adjacent_wards: list[str],
    rental_bedrooms: int, rental_area: float, rental_capacity: int,
    comp_ward: str, comp_bedrooms: int, comp_area: float, comp_capacity: int,
) -> float:
    """類似度スコアを計算する（内部関数）。"""
    # 区の一致度
    if comp_ward == rental_ward:
        ward_score = 1.0
    elif comp_ward in adjacent_wards:
        ward_score = 0.5
    else:
        ward_score = 0.0

    # 間取り（寝室数）の一致度
    bedroom_diff = abs(rental_bedrooms - comp_bedrooms)
    if bedroom_diff == 0:
        plan_score = 1.0
    elif bedroom_diff == 1:
        plan_score = 0.5
    else:
        plan_score = 0.0

    # 面積の一致度
    if rental_area > 0 and comp_area > 0:
        max_area = max(rental_area, comp_area)
        area_score = max(0.0, 1.0 - abs(rental_area - comp_area) / max_area)
    else:
        # 面積情報がない場合は中立スコア
        area_score = 0.5

    # 定員の一致度
    cap_diff = abs(rental_capacity - comp_capacity)
    if cap_diff == 0:
        cap_score = 1.0
    elif cap_diff == 1:
        cap_score = 0.7
    elif cap_diff == 2:
        cap_score = 0.4
    else:
        cap_score = 0.0

    # 加重平均
    total = (
        ward_score * 0.30
        + plan_score * 0.30
        + area_score * 0.20
        + cap_score * 0.20
    )
    return total
