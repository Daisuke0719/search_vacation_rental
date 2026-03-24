"""賃貸掲載リサーチ - メインバッチ実行スクリプト

Usage:
    python main.py                    # 全サイト・全建物で実行
    python main.py --sites suumo      # SUUMOのみ
    python main.py --ward 中央区      # 中央区のみ
    python main.py --limit 10         # 最初の10棟のみ（テスト用）
    python main.py --resume           # 前回の中断から再開
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# PYTHONPATH に自身のディレクトリを追加
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import ENABLED_SITES, GOOGLE_SEARCH_SITES, DIRECT_SCRAPE_SITES, LOG_DIR, BUILDINGS_CSV
from extractors.building_name import (
    load_and_extract, load_buildings_csv, export_buildings_csv,
    get_search_name, BuildingGroup,
)
from models.database import (
    init_db, get_db, create_search_run, finish_search_run,
    upsert_building, upsert_registration, upsert_listing,
    mark_inactive_listings, log_search, is_already_searched,
    get_latest_run_id, get_new_listings_today,
)
from scrapers.base import ListingResult
from scrapers.suumo import SuumoScraper
from scrapers.homes import HomesScraper
from scrapers.google_search import GoogleSearchScraper
from notifications.line_notify import send_new_listing_notification
from exporters.excel_export import export_results

logger = logging.getLogger(__name__)

# 直接スクレイピング可能なサイト
DIRECT_SCRAPER_MAP = {
    "suumo": SuumoScraper,
    "homes": HomesScraper,
}

# 全対応サイト（直接 + Google検索経由）
ALL_SITES = DIRECT_SCRAPE_SITES + GOOGLE_SEARCH_SITES


def setup_logging():
    """ロギング設定"""
    log_file = LOG_DIR / f"search_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_file), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_buildings_to_db(groups: list[BuildingGroup]) -> dict[tuple[str, str], int]:
    """建物情報をDBに登録し、building_id マッピングを返す"""
    building_ids = {}
    with get_db() as conn:
        for group in groups:
            bid = upsert_building(
                conn, group.building_name, group.address_base,
                group.ward, group.unit_count,
            )
            building_ids[(group.building_name, group.address_base)] = bid

            for reg in group.registrations:
                upsert_registration(
                    conn, bid, reg.full_address, reg.room_number,
                    reg.registration_number, reg.registration_date,
                    reg.fire_violation,
                )
    return building_ids


def _create_scraper(site_name: str):
    """サイト名に応じてスクレイパーを生成（直接 or Google検索経由）"""
    if site_name in DIRECT_SCRAPER_MAP:
        return DIRECT_SCRAPER_MAP[site_name]()
    else:
        # Google site: 検索経由
        return GoogleSearchScraper(target_site=site_name)


async def search_site(site_name: str, groups: list[BuildingGroup],
                      building_ids: dict, run_id: int,
                      resume: bool = False) -> dict:
    """1サイトについて全建物を検索"""
    stats = {"searched": 0, "found": 0, "new": 0, "errors": 0}

    if site_name not in ALL_SITES:
        logger.warning(f"Unknown site: {site_name}")
        return stats

    scraper = _create_scraper(site_name)
    method = "direct" if site_name in DIRECT_SCRAPER_MAP else "google"
    logger.info(f"[{site_name}] Using {method} scraping")

    async with scraper:

        for group in groups:
            bid = building_ids.get(
                (group.building_name, group.address_base)
            )
            if not bid:
                continue

            # 中断再開チェック
            if resume:
                with get_db() as conn:
                    if is_already_searched(conn, run_id, bid, site_name):
                        continue

            search_name = get_search_name(group.building_name)
            try:
                results = await scraper.search(search_name, group.ward)
                stats["searched"] += 1

                active_urls = []
                for r in results:
                    with get_db() as conn:
                        lid, is_new = upsert_listing(
                            conn, bid, site_name, r.listing_url,
                            listing_title=r.listing_title,
                            rent_price=r.rent_price,
                            management_fee=r.management_fee,
                            deposit=r.deposit,
                            key_money=r.key_money,
                            floor_plan=r.floor_plan,
                            area_sqm=r.area_sqm,
                            floor_number=r.floor_number,
                            building_age=r.building_age,
                            nearest_station=r.nearest_station,
                            walk_minutes=r.walk_minutes,
                        )
                    active_urls.append(r.listing_url)
                    stats["found"] += 1
                    if is_new:
                        stats["new"] += 1

                # 今回見つからなかった掲載を非アクティブに
                with get_db() as conn:
                    mark_inactive_listings(conn, bid, site_name, active_urls)
                    log_search(
                        conn, run_id, bid, site_name,
                        "success" if results else "no_results",
                    )

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"[{site_name}] Error for {group.building_name}: {e}")
                with get_db() as conn:
                    log_search(conn, run_id, bid, site_name, "error", str(e))

    return stats


async def run_search(sites: list[str], groups: list[BuildingGroup],
                     building_ids: dict, resume: bool = False):
    """全サイトの検索を実行"""
    # 実行レコード作成（または再開）
    with get_db() as conn:
        if resume:
            run_id = get_latest_run_id(conn)
            if run_id:
                logger.info(f"Resuming run {run_id}")
            else:
                run_id = create_search_run(conn)
                logger.info(f"No incomplete run found. Starting new run {run_id}")
        else:
            run_id = create_search_run(conn)
            logger.info(f"Starting run {run_id}")

    total_stats = {"searched": 0, "found": 0, "new": 0, "errors": 0}

    # サイトごとに順次実行（同時アクセスを避ける）
    for site in sites:
        if site not in ALL_SITES:
            logger.warning(f"Unknown site: {site}")
            continue

        logger.info(f"=== Searching {site} ({len(groups)} buildings) ===")
        stats = await search_site(
            site, groups, building_ids, run_id, resume
        )

        for k in total_stats:
            total_stats[k] += stats[k]

        logger.info(
            f"[{site}] Done: {stats['searched']} searched, "
            f"{stats['found']} found, {stats['new']} new, "
            f"{stats['errors']} errors"
        )

    # 実行完了
    with get_db() as conn:
        finish_search_run(
            conn, run_id,
            total_stats["searched"], total_stats["found"],
            total_stats["new"], total_stats["errors"],
        )

    logger.info(
        f"=== Run complete: {total_stats['searched']} searched, "
        f"{total_stats['found']} found, {total_stats['new']} new, "
        f"{total_stats['errors']} errors ==="
    )

    return total_stats


def main():
    parser = argparse.ArgumentParser(description="民泊物件 賃貸掲載リサーチ")
    parser.add_argument(
        "--sites", nargs="+", default=None,
        help=(
            f"検索対象サイト (default: {ENABLED_SITES}). "
            f"Google経由で追加可能: {GOOGLE_SEARCH_SITES}"
        ),
    )
    parser.add_argument("--ward", type=str, default=None, help="区名で絞り込み")
    parser.add_argument("--limit", type=int, default=None, help="検索建物数の上限（テスト用）")
    parser.add_argument("--resume", action="store_true", help="前回の中断から再開")
    parser.add_argument("--no-notify", action="store_true", help="LINE通知を無効化")
    parser.add_argument("--no-export", action="store_true", help="Excel出力を無効化")
    parser.add_argument("--rebuild-csv", action="store_true", help="Excelから建物リストCSVを再生成")
    args = parser.parse_args()

    setup_logging()
    logger.info("=== 賃貸掲載リサーチ開始 ===")

    # DB初期化
    init_db()

    # 建物リスト読み込み
    csv_path = str(BUILDINGS_CSV)
    if args.rebuild_csv or not BUILDINGS_CSV.exists():
        logger.info("Excelから建物名を抽出中...")
        groups = load_and_extract()
        export_buildings_csv(groups, csv_path)
        logger.info(f"Extracted {len(groups)} unique buildings -> {csv_path}")
    else:
        groups = load_buildings_csv(csv_path)
        logger.info(f"Loaded {len(groups)} buildings from {csv_path}")

    # フィルタ
    if args.ward:
        groups = [g for g in groups if g.ward == args.ward]
        logger.info(f"Filtered to {len(groups)} buildings in {args.ward}")

    # ユニット数の多い建物を優先
    groups.sort(key=lambda g: -g.unit_count)

    if args.limit:
        groups = groups[:args.limit]
        logger.info(f"Limited to {len(groups)} buildings")

    # 建物情報をDBに登録
    building_ids = load_buildings_to_db(groups)
    logger.info(f"Registered {len(building_ids)} buildings in DB")

    # 検索実行
    sites = args.sites or ENABLED_SITES
    total_stats = asyncio.run(
        run_search(sites, groups, building_ids, args.resume)
    )

    # 新着通知
    if not args.no_notify and total_stats["new"] > 0:
        try:
            with get_db() as conn:
                new_listings = get_new_listings_today(conn)
            if new_listings:
                send_new_listing_notification(new_listings)
                logger.info(f"Sent LINE notification for {len(new_listings)} new listings")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

    # Excel出力
    if not args.no_export:
        try:
            output_path = export_results()
            logger.info(f"Excel exported to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export Excel: {e}")

    logger.info("=== 完了 ===")


if __name__ == "__main__":
    main()
