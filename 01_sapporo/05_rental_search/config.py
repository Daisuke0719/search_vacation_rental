"""賃貸掲載リサーチアプリケーション - 設定"""

import os
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルート
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
load_dotenv(BASE_DIR / ".env")

# データベース
DB_PATH = BASE_DIR / "db" / "rental_search.db"

# 元データ
EXCEL_PATH = PROJECT_ROOT / "01_sapporo" / "00_ref" / "札幌市内の民泊施設一覧.xlsx"

# 出力先
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# 前処理済み中間ファイル
BUILDINGS_CSV = OUTPUT_DIR / "buildings.csv"

# LINE Messaging API
LINE_CHANNEL_TOKEN = os.getenv("LINE_CHANNEL_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")

# レート制限設定（秒）
RATE_LIMITS = {
    "suumo": {"min_delay": 3, "max_delay": 7, "max_per_hour": 120},
    "homes": {"min_delay": 4, "max_delay": 10, "max_per_hour": 80},
    "athome": {"min_delay": 4, "max_delay": 10, "max_per_hour": 80},
    "yahoo": {"min_delay": 3, "max_delay": 8, "max_per_hour": 100},
    "smyty": {"min_delay": 3, "max_delay": 8, "max_per_hour": 100},
    "google": {"min_delay": 5, "max_delay": 12, "max_per_hour": 60},
}

# 札幌市 区コード（SUUMO用）
SAPPORO_WARD_CODES = {
    "中央区": "01101",
    "北区": "01102",
    "東区": "01103",
    "白石区": "01104",
    "豊平区": "01105",
    "南区": "01106",
    "西区": "01107",
    "厚別区": "01108",
    "手稲区": "01109",
    "清田区": "01110",
}

# 検索対象サイト（デフォルト）
# suumo: 直接スクレイピング（fw2パラメータ）
# homes: フォーム入力方式（フリーワード検索）
ENABLED_SITES = ["suumo", "homes"]

# Google site:検索経由で追加可能なサイト（将来用）
# ※GoogleのCAPTCHAが出る場合あり。IPブロック時は動作しない
GOOGLE_SEARCH_SITES = ["homes", "athome", "yahoo", "smyty"]

# 直接スクレイピング可能なサイト
DIRECT_SCRAPE_SITES = ["suumo", "homes"]

# Playwright設定
PLAYWRIGHT_HEADLESS = True
PLAYWRIGHT_TIMEOUT = 30000  # ms

# Notion API
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

# ログ設定
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
