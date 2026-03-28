"""Airbnb DB操作 & データモデル"""

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ── パス設定 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "external_data" / "airbnb_listings.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)


@dataclass
class AirbnbListing:
    """Airbnb物件データ"""
    listing_url: str
    listing_title: str = ""
    nightly_price: Optional[int] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    property_type: str = ""
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    ward: str = ""
    search_area: str = ""
    superhost: bool = False


@dataclass
class AirbnbDetail:
    """詳細ページから取得するデータ"""
    listing_url: str
    rating: Optional[float] = None
    review_count: Optional[int] = None
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    property_type: str = ""
    amenities: Optional[list[str]] = None
    superhost: bool = False
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    neighborhood: str = ""


# =====================================================================
# DB操作
# =====================================================================

def init_db():
    """Airbnb用DBスキーマを初期化"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS airbnb_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_url TEXT NOT NULL UNIQUE,
            listing_title TEXT,
            nightly_price INTEGER,
            rating REAL,
            review_count INTEGER,
            property_type TEXT,
            guest_capacity INTEGER,
            bedrooms INTEGER,
            ward TEXT,
            search_area TEXT,
            superhost INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_airbnb_ward ON airbnb_listings(ward)
    """)
    conn.commit()
    conn.close()
    logger.info(f"DB initialized: {DB_PATH}")


def migrate_db():
    """既存DBに新カラムを追加するマイグレーション"""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.execute("PRAGMA table_info(airbnb_listings)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    new_columns = [
        ("bathrooms", "INTEGER"),
        ("amenities", "TEXT"),
        ("calendar_occupancy", "REAL"),
        ("detail_scraped_at", "TIMESTAMP"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
        ("neighborhood", "TEXT"),
    ]

    added = []
    for col_name, col_type in new_columns:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE airbnb_listings ADD COLUMN {col_name} {col_type}")
            added.append(col_name)

    conn.commit()
    conn.close()

    if added:
        logger.info(f"DB migration: added columns {added}")
    else:
        logger.info("DB migration: no new columns needed")


def save_listings(listings: list[AirbnbListing]):
    """物件データをDBに保存"""
    conn = sqlite3.connect(str(DB_PATH))
    inserted = 0
    updated = 0

    for item in listings:
        try:
            conn.execute(
                """INSERT INTO airbnb_listings
                   (listing_url, listing_title, nightly_price, rating, review_count,
                    property_type, guest_capacity, bedrooms, ward, search_area, superhost)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(listing_url) DO UPDATE SET
                     nightly_price = excluded.nightly_price,
                     rating = excluded.rating,
                     review_count = excluded.review_count,
                     scraped_at = CURRENT_TIMESTAMP""",
                (item.listing_url, item.listing_title, item.nightly_price,
                 item.rating, item.review_count, item.property_type,
                 item.guest_capacity, item.bedrooms, item.ward,
                 item.search_area, int(item.superhost)),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"DB insert error: {e}")
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Saved: {inserted} new, {updated} updated")


def save_detail(detail: AirbnbDetail):
    """詳細ページのデータをDBに更新"""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        amenities_json = json.dumps(detail.amenities, ensure_ascii=False) if detail.amenities else None
        conn.execute(
            """UPDATE airbnb_listings SET
                rating = COALESCE(?, rating),
                review_count = COALESCE(?, review_count),
                guest_capacity = COALESCE(?, guest_capacity),
                bedrooms = COALESCE(?, bedrooms),
                bathrooms = COALESCE(?, bathrooms),
                property_type = CASE WHEN ? != '' THEN ? ELSE property_type END,
                amenities = COALESCE(?, amenities),
                superhost = ?,
                latitude = COALESCE(?, latitude),
                longitude = COALESCE(?, longitude),
                neighborhood = CASE WHEN ? != '' THEN ? ELSE neighborhood END,
                detail_scraped_at = CURRENT_TIMESTAMP
               WHERE listing_url = ?""",
            (detail.rating, detail.review_count, detail.guest_capacity,
             detail.bedrooms, detail.bathrooms,
             detail.property_type, detail.property_type,
             amenities_json,
             int(detail.superhost),
             detail.latitude, detail.longitude,
             detail.neighborhood, detail.neighborhood,
             detail.listing_url),
        )
        conn.commit()
        changes = conn.execute("SELECT changes()").fetchone()[0]
        if changes > 0:
            logger.info(f"  Detail saved for {detail.listing_url}")
        else:
            logger.warning(f"  No row matched for {detail.listing_url}")
    except Exception as e:
        logger.warning(f"DB detail update error: {e}")
    finally:
        conn.close()


def get_listings_needing_detail(max_listings: int = 50) -> list[str]:
    """詳細スクレイピングが必要な物件URLを取得"""
    conn = sqlite3.connect(str(DB_PATH))
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        """SELECT listing_url FROM airbnb_listings
           WHERE detail_scraped_at IS NULL OR detail_scraped_at < ?
           ORDER BY detail_scraped_at ASC NULLS FIRST, scraped_at DESC
           LIMIT ?""",
        (cutoff, max_listings),
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]
