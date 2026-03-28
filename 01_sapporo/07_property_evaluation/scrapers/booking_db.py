"""Booking.com スクレイパー — DB操作とデータモデル"""

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── パス設定 ──
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "external_data" / "booking_listings.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class BookingListing:
    """Booking.com物件データ"""
    listing_url: str
    listing_title: str = ""
    nightly_price: Optional[int] = None       # 1泊あたりJPY
    rating: Optional[float] = None            # 0-10スケール
    review_count: Optional[int] = None
    property_type: str = ""                   # apartment, house等
    guest_capacity: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    ward: str = ""                            # 区
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    amenities: Optional[str] = None           # JSONリスト
    detail_scraped: bool = False


def init_db():
    """Booking.com用DBスキーマを初期化"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS booking_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_url TEXT NOT NULL UNIQUE,
            listing_title TEXT,
            nightly_price INTEGER,
            rating REAL,
            review_count INTEGER,
            property_type TEXT,
            guest_capacity INTEGER,
            bedrooms INTEGER,
            bathrooms INTEGER,
            ward TEXT,
            latitude REAL,
            longitude REAL,
            amenities TEXT,
            detail_scraped INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_booking_ward ON booking_listings(ward)
    """)
    conn.commit()
    conn.close()
    logger.info(f"DB initialized: {DB_PATH}")


def save_listings(listings: list[BookingListing]):
    """物件データをDBに保存（UPSERT）"""
    conn = sqlite3.connect(str(DB_PATH))
    inserted = 0
    updated = 0

    for item in listings:
        try:
            conn.execute(
                """INSERT INTO booking_listings
                   (listing_url, listing_title, nightly_price, rating, review_count,
                    property_type, guest_capacity, bedrooms, bathrooms, ward,
                    latitude, longitude, amenities, detail_scraped)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(listing_url) DO UPDATE SET
                     listing_title = COALESCE(excluded.listing_title, listing_title),
                     nightly_price = COALESCE(excluded.nightly_price, nightly_price),
                     rating = COALESCE(excluded.rating, rating),
                     review_count = COALESCE(excluded.review_count, review_count),
                     property_type = COALESCE(excluded.property_type, property_type),
                     guest_capacity = COALESCE(excluded.guest_capacity, guest_capacity),
                     bedrooms = COALESCE(excluded.bedrooms, bedrooms),
                     bathrooms = COALESCE(excluded.bathrooms, bathrooms),
                     latitude = COALESCE(excluded.latitude, latitude),
                     longitude = COALESCE(excluded.longitude, longitude),
                     amenities = COALESCE(excluded.amenities, amenities),
                     detail_scraped = MAX(detail_scraped, excluded.detail_scraped),
                     scraped_at = CURRENT_TIMESTAMP""",
                (item.listing_url, item.listing_title, item.nightly_price,
                 item.rating, item.review_count, item.property_type,
                 item.guest_capacity, item.bedrooms, item.bathrooms, item.ward,
                 item.latitude, item.longitude, item.amenities,
                 int(item.detail_scraped)),
            )
            changes = conn.execute("SELECT changes()").fetchone()[0]
            if changes > 0:
                inserted += 1
        except Exception as e:
            logger.warning(f"DB insert error: {e}")
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Saved: {inserted} new/updated, {updated} errors")


def get_listings_without_details(limit: int = 50) -> list[dict]:
    """詳細未取得の物件URLリストを取得"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, listing_url, ward FROM booking_listings
           WHERE detail_scraped = 0
           ORDER BY scraped_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]
