"""物件評価エンジン - 共通定数・設定

札幌市の民泊事業向け物件評価に使用する定数を一元管理する。
"""

from pathlib import Path

# ── パス設定 ──
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
SAPPORO_DIR = BASE_DIR.parent

RENTAL_DB_PATH = SAPPORO_DIR / "05_rental_search" / "db" / "rental_search.db"
AIRBNB_DB_PATH = SAPPORO_DIR / "06_area_analysis" / "external_data" / "airbnb_listings.db"
BOOKING_DB_PATH = BASE_DIR / "external_data" / "booking_listings.db"
OUTPUT_DIR = BASE_DIR / "output"

# ── 札幌市 区の隣接マッピング ──
WARD_ADJACENCY: dict[str, list[str]] = {
    "中央区": ["北区", "東区", "白石区", "豊平区", "南区", "西区"],
    "北区":   ["中央区", "東区", "西区", "手稲区", "石狩市"],
    "東区":   ["中央区", "北区", "白石区", "豊平区"],
    "白石区": ["中央区", "東区", "豊平区", "厚別区"],
    "豊平区": ["中央区", "東区", "白石区", "南区", "清田区"],
    "南区":   ["中央区", "豊平区", "西区"],
    "西区":   ["中央区", "北区", "南区", "手稲区"],
    "厚別区": ["白石区", "清田区", "江別市"],
    "手稲区": ["北区", "西区", "小樽市"],
    "清田区": ["豊平区", "白石区", "厚別区", "北広島市"],
}

# ── 季節定義 ──
SEASON_MONTHS: dict[str, list[int]] = {
    "peak":     [2, 7, 8, 12],          # 繁忙期
    "normal":   [5, 6, 9, 10],          # 通常期
    "offpeak":  [1, 3, 4, 11],          # 閑散期
}

# 季節ごとの価格倍率
SEASON_MULTIPLIER: dict[str, float] = {
    "peak":    1.5,
    "normal":  1.0,
    "offpeak": 0.7,
}

# 各月の日数
DAYS_IN_MONTH: dict[int, int] = {
    1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
}

# ── 区別の需要レベル（稼働率ベースライン） ──
WARD_OCCUPANCY_BASELINE: dict[str, float] = {
    "中央区": 0.65,
    "北区":   0.55,
    "豊平区": 0.50,
    "白石区": 0.48,
    "東区":   0.45,
    "南区":   0.45,
    "西区":   0.45,
    "厚別区": 0.40,
    "手稲区": 0.38,
    "清田区": 0.35,
}

# 区別のロケーションスコア（25点満点）
WARD_LOCATION_SCORE: dict[str, int] = {
    "中央区": 25,
    "北区":   18,
    "豊平区": 16,
    "南区":   14,
    "白石区": 13,
    "東区":   12,
    "西区":   12,
    "厚別区": 10,
    "手稲区": 10,
    "清田区": 10,
}

# ── コスト定数 ──
CLEANING_COST_PER_TURNOVER = 4_000     # 清掃費（1回あたり）
TURNOVER_RATIO = 0.4                    # 稼働日あたりターンオーバー率
OTA_COMMISSION_RATE = 0.15              # OTA手数料率
UTILITIES_MONTHLY = 20_000              # 水道光熱費（月額）
CONSUMABLES_MONTHLY = 10_000            # 消耗品費（月額）
WIFI_SUBSCRIPTIONS_MONTHLY = 8_000      # WiFi・サブスク（月額）
MANAGEMENT_OUTSOURCE_RATE = 0.20        # 運営代行手数料率

# 初期費用
INITIAL_FURNITURE = 500_000             # 家具・家電
INITIAL_FIRE_SAFETY = 100_000           # 消防設備
INITIAL_REGISTRATION = 50_000           # 届出・登録費用
AMORTIZATION_MONTHS = 24                # 初期費用の償却月数

# ── 事業形態 ──
BUSINESS_TYPES: dict[str, int] = {
    "minpaku":  180,    # 民泊新法: 年間180日上限
    "kaniyado": 365,    # 簡易宿所: 年間365日
}

# ── フォールバックADR（Airbnb/Bookingデータが無い場合） ──
# 04_収支シミュレーション.md のパターンAベースの値
FALLBACK_ADR: dict[str, dict[str, int]] = {
    "1K":       {"peak": 10_000, "normal": 6_500, "offpeak": 4_500},
    "1LDK":     {"peak": 12_000, "normal": 7_500, "offpeak": 5_000},
    "2DK":      {"peak": 15_000, "normal": 10_000, "offpeak": 7_000},
    "2LDK":     {"peak": 18_000, "normal": 12_000, "offpeak": 8_000},
    "3LDK":     {"peak": 25_000, "normal": 16_000, "offpeak": 11_000},
    "default":  {"peak": 12_000, "normal": 7_500, "offpeak": 5_000},
}

# ── Airbnbレビュー率推計 ──
REVIEW_RATE = 0.35          # レビュー投稿率
AVG_STAY_NIGHTS = 2.5       # 平均宿泊日数
