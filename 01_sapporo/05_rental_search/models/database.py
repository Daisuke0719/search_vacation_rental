"""SQLiteデータベース管理モジュール"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from config import DB_PATH


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """SQLite接続を取得"""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db(db_path: Optional[Path] = None):
    """コンテキストマネージャでDB接続を管理"""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Optional[Path] = None):
    """データベーススキーマを初期化"""
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


SCHEMA_SQL = """
-- 建物マスタ（重複排除済み）
CREATE TABLE IF NOT EXISTS buildings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_name TEXT NOT NULL,
    address_base TEXT NOT NULL,
    ward TEXT NOT NULL,
    unit_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(building_name, address_base)
);

-- 民泊届出レコード（元Excel）
CREATE TABLE IF NOT EXISTS minpaku_registrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER REFERENCES buildings(id),
    full_address TEXT NOT NULL,
    room_number TEXT,
    registration_number TEXT NOT NULL,
    registration_date DATE,
    fire_violation TEXT,
    UNIQUE(registration_number)
);

-- 賃貸掲載情報
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER REFERENCES buildings(id),
    site_name TEXT NOT NULL,
    listing_url TEXT NOT NULL,
    listing_title TEXT,
    rent_price INTEGER,
    management_fee INTEGER,
    deposit TEXT,
    key_money TEXT,
    floor_plan TEXT,
    area_sqm REAL,
    floor_number TEXT,
    building_age TEXT,
    nearest_station TEXT,
    walk_minutes INTEGER,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1,
    UNIQUE(site_name, listing_url)
);

-- 検索実行履歴
CREATE TABLE IF NOT EXISTS search_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started_at TIMESTAMP,
    run_finished_at TIMESTAMP,
    buildings_searched INTEGER DEFAULT 0,
    total_listings_found INTEGER DEFAULT 0,
    new_listings_count INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'
);

-- 検索ログ（建物×サイトごと）
CREATE TABLE IF NOT EXISTS search_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES search_runs(id),
    building_id INTEGER REFERENCES buildings(id),
    site_name TEXT NOT NULL,
    search_status TEXT,
    error_message TEXT,
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 通知履歴
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER REFERENCES listings(id),
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notification_type TEXT,
    message_text TEXT,
    delivery_status TEXT
);

-- インデックス
CREATE INDEX IF NOT EXISTS idx_listings_building ON listings(building_id);
CREATE INDEX IF NOT EXISTS idx_listings_site ON listings(site_name);
CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active);
CREATE INDEX IF NOT EXISTS idx_listings_first_seen ON listings(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_buildings_ward ON buildings(ward);
CREATE INDEX IF NOT EXISTS idx_search_log_run ON search_log(run_id);
CREATE INDEX IF NOT EXISTS idx_search_log_building_site ON search_log(building_id, site_name);
"""


# --- Buildings CRUD ---

def upsert_building(conn: sqlite3.Connection, building_name: str,
                    address_base: str, ward: str, unit_count: int = 1) -> int:
    """建物を挿入または更新し、IDを返す"""
    cursor = conn.execute(
        """INSERT INTO buildings (building_name, address_base, ward, unit_count)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(building_name, address_base)
           DO UPDATE SET unit_count = excluded.unit_count
           RETURNING id""",
        (building_name, address_base, ward, unit_count),
    )
    return cursor.fetchone()[0]


def upsert_registration(conn: sqlite3.Connection, building_id: int,
                        full_address: str, room_number: Optional[str],
                        registration_number: str, registration_date: Optional[str],
                        fire_violation: Optional[str] = None):
    """民泊届出レコードを挿入"""
    conn.execute(
        """INSERT INTO minpaku_registrations
           (building_id, full_address, room_number, registration_number,
            registration_date, fire_violation)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(registration_number) DO UPDATE SET
             building_id = excluded.building_id,
             room_number = excluded.room_number""",
        (building_id, full_address, room_number, registration_number,
         registration_date, fire_violation),
    )


# --- Listings CRUD ---

def upsert_listing(conn: sqlite3.Connection, building_id: int, site_name: str,
                   listing_url: str, **kwargs) -> tuple[int, bool]:
    """掲載情報を挿入または更新。(listing_id, is_new) を返す"""
    existing = conn.execute(
        "SELECT id FROM listings WHERE site_name = ? AND listing_url = ?",
        (site_name, listing_url),
    ).fetchone()

    if existing:
        # 既存: last_seen_at を更新
        conn.execute(
            "UPDATE listings SET last_seen_at = ?, is_active = 1 WHERE id = ?",
            (datetime.now().isoformat(), existing["id"]),
        )
        return existing["id"], False
    else:
        # 新規挿入
        now = datetime.now().isoformat()
        columns = ["building_id", "site_name", "listing_url",
                    "first_seen_at", "last_seen_at"]
        values = [building_id, site_name, listing_url, now, now]

        for col in ["listing_title", "rent_price", "management_fee", "deposit",
                     "key_money", "floor_plan", "area_sqm", "floor_number",
                     "building_age", "nearest_station", "walk_minutes"]:
            if col in kwargs and kwargs[col] is not None:
                columns.append(col)
                values.append(kwargs[col])

        placeholders = ", ".join(["?"] * len(columns))
        col_str = ", ".join(columns)
        cursor = conn.execute(
            f"INSERT INTO listings ({col_str}) VALUES ({placeholders})",
            values,
        )
        return cursor.lastrowid, True


def mark_inactive_listings(conn: sqlite3.Connection, building_id: int,
                           site_name: str, active_urls: list[str]):
    """今回の検索で見つからなかった掲載を非アクティブにする"""
    if not active_urls:
        conn.execute(
            "UPDATE listings SET is_active = 0 WHERE building_id = ? AND site_name = ?",
            (building_id, site_name),
        )
    else:
        placeholders = ", ".join(["?"] * len(active_urls))
        conn.execute(
            f"""UPDATE listings SET is_active = 0
                WHERE building_id = ? AND site_name = ?
                AND listing_url NOT IN ({placeholders})""",
            [building_id, site_name] + active_urls,
        )


# --- Search Runs ---

def create_search_run(conn: sqlite3.Connection) -> int:
    """検索実行レコードを作成"""
    cursor = conn.execute(
        "INSERT INTO search_runs (run_started_at, status) VALUES (?, 'running')",
        (datetime.now().isoformat(),),
    )
    return cursor.lastrowid


def finish_search_run(conn: sqlite3.Connection, run_id: int,
                      buildings_searched: int, total_found: int,
                      new_count: int, errors: int):
    """検索実行を完了に更新"""
    conn.execute(
        """UPDATE search_runs
           SET run_finished_at = ?, buildings_searched = ?,
               total_listings_found = ?, new_listings_count = ?,
               errors_count = ?, status = 'completed'
           WHERE id = ?""",
        (datetime.now().isoformat(), buildings_searched, total_found,
         new_count, errors, run_id),
    )


def log_search(conn: sqlite3.Connection, run_id: int, building_id: int,
               site_name: str, status: str, error_message: Optional[str] = None):
    """検索ログを記録"""
    conn.execute(
        """INSERT INTO search_log (run_id, building_id, site_name, search_status, error_message)
           VALUES (?, ?, ?, ?, ?)""",
        (run_id, building_id, site_name, status, error_message),
    )


def is_already_searched(conn: sqlite3.Connection, run_id: int,
                        building_id: int, site_name: str) -> bool:
    """今回の実行で既に検索済みかチェック（中断再開用）"""
    row = conn.execute(
        """SELECT 1 FROM search_log
           WHERE run_id = ? AND building_id = ? AND site_name = ?""",
        (run_id, building_id, site_name),
    ).fetchone()
    return row is not None


def get_latest_run_id(conn: sqlite3.Connection) -> Optional[int]:
    """最新の実行中runのIDを取得（中断再開用）"""
    row = conn.execute(
        "SELECT id FROM search_runs WHERE status = 'running' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


# --- Notifications ---

def log_notification(conn: sqlite3.Connection, listing_id: int,
                     notification_type: str, message: str, status: str):
    """通知ログを記録"""
    conn.execute(
        """INSERT INTO notifications (listing_id, notification_type, message_text, delivery_status)
           VALUES (?, ?, ?, ?)""",
        (listing_id, notification_type, message, status),
    )


# --- Query helpers ---

def get_new_listings_today(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """今日初めて検出された掲載を取得"""
    today = date.today().isoformat()
    return conn.execute(
        """SELECT l.*, b.building_name, b.ward, b.address_base
           FROM listings l
           JOIN buildings b ON l.building_id = b.id
           WHERE date(l.first_seen_at) = ? AND l.is_active = 1
           ORDER BY l.rent_price""",
        (today,),
    ).fetchall()


def get_all_active_listings(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """全アクティブ掲載を取得"""
    return conn.execute(
        """SELECT l.*, b.building_name, b.ward, b.address_base
           FROM listings l
           JOIN buildings b ON l.building_id = b.id
           WHERE l.is_active = 1
           ORDER BY b.ward, b.building_name, l.site_name""",
    ).fetchall()


def get_all_buildings(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """全建物を取得"""
    return conn.execute(
        "SELECT * FROM buildings ORDER BY ward, building_name"
    ).fetchall()


def get_search_stats(conn: sqlite3.Connection) -> dict:
    """検索統計サマリーを取得"""
    stats = {}
    row = conn.execute("SELECT COUNT(*) as cnt FROM buildings").fetchone()
    stats["total_buildings"] = row["cnt"]

    row = conn.execute(
        """SELECT COUNT(DISTINCT building_id) as cnt
           FROM listings WHERE is_active = 1"""
    ).fetchone()
    stats["buildings_with_listings"] = row["cnt"]

    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM listings WHERE is_active = 1"
    ).fetchone()
    stats["total_active_listings"] = row["cnt"]

    today = date.today().isoformat()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM listings WHERE date(first_seen_at) = ?",
        (today,),
    ).fetchone()
    stats["new_today"] = row["cnt"]

    return stats
