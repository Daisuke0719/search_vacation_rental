"""プロジェクト共通パス定義

各モジュールで重複していたパス解決パターンを一元化。
"""

from pathlib import Path

# プロジェクトルート（民泊/）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# 札幌ディレクトリ
SAPPORO_DIR = PROJECT_ROOT / "01_sapporo"

# 各モジュールのディレクトリ
RENTAL_SEARCH_DIR = SAPPORO_DIR / "05_rental_search"
AREA_ANALYSIS_DIR = SAPPORO_DIR / "06_area_analysis"
PROPERTY_EVAL_DIR = SAPPORO_DIR / "07_property_evaluation"

# リファレンスデータ
REF_DIR = SAPPORO_DIR / "00_ref"
EXCEL_PATH = REF_DIR / "札幌市内の民泊施設一覧.xlsx"

# データベースパス
RENTAL_DB_PATH = RENTAL_SEARCH_DIR / "db" / "rental_search.db"
AIRBNB_DB_PATH = AREA_ANALYSIS_DIR / "external_data" / "airbnb_listings.db"
BOOKING_DB_PATH = PROPERTY_EVAL_DIR / "external_data" / "booking_listings.db"
