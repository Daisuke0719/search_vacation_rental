"""CI/CD パイプライン オーケストレーションスクリプト

GitHub Actions から呼び出され、以下を順次実行する:
1. 建物リスト読み込み・DB登録
2. 賃貸サイトスクレイピング
3. Excel出力
4. LINE通知（新着時）
5. マップHTML生成
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import ENABLED_SITES, BUILDINGS_CSV, LOG_DIR
from extractors.building_name import (
    load_and_extract, load_buildings_csv, export_buildings_csv,
)
from models.database import init_db, get_db, get_new_listings_today
from main import load_buildings_to_db, run_search, setup_logging
from exporters.excel_export import export_results
from notifications.line_notify import send_new_listing_notification
from generate_map import (
    load_listings, geocode_all, build_map_data, render_html,
    find_latest_excel, MAP_OUTPUT_PATH,
)

logger = logging.getLogger(__name__)


def generate_map():
    """Excel出力からマップHTMLを生成（webbrowser.open なし）"""
    excel_path = find_latest_excel()
    if not excel_path or not excel_path.exists():
        logger.warning("Excelファイルが見つかりません。マップ生成をスキップします")
        return False

    logger.info(f"[MAP] Excelデータ読み込み: {excel_path.name}")
    df = load_listings(excel_path)

    logger.info("[MAP] ジオコーディング（国土地理院API）")
    coords = geocode_all(df)

    logger.info("[MAP] マップデータ構築")
    properties, unmapped = build_map_data(df, coords)
    logger.info(f"[MAP] マップ表示: {len(properties)}件, 未マッピング: {len(unmapped)}件")

    logger.info("[MAP] HTML生成")
    render_html(properties, unmapped, MAP_OUTPUT_PATH)
    logger.info(f"[MAP] マップHTML生成完了: {MAP_OUTPUT_PATH}")
    return True


def main():
    setup_logging()
    logger.info("=== CI/CD パイプライン開始 ===")

    # 1. DB初期化
    init_db()
    logger.info("DB初期化完了")

    # 2. 建物リスト読み込み
    csv_path = str(BUILDINGS_CSV)
    if not BUILDINGS_CSV.exists():
        logger.info("Excelから建物名を抽出中...")
        groups = load_and_extract()
        export_buildings_csv(groups, csv_path)
        logger.info(f"Extracted {len(groups)} unique buildings -> {csv_path}")
    else:
        groups = load_buildings_csv(csv_path)
        logger.info(f"Loaded {len(groups)} buildings from {csv_path}")

    # ユニット数の多い建物を優先
    groups.sort(key=lambda g: -g.unit_count)

    # 3. 建物情報をDBに登録
    building_ids = load_buildings_to_db(groups)
    logger.info(f"Registered {len(building_ids)} buildings in DB")

    # 4. スクレイピング実行
    try:
        total_stats = asyncio.run(
            run_search(ENABLED_SITES, groups, building_ids)
        )
        logger.info(
            f"検索完了: {total_stats['searched']} searched, "
            f"{total_stats['found']} found, {total_stats['new']} new, "
            f"{total_stats['errors']} errors"
        )
    except Exception as e:
        logger.error(f"スクレイピング中にエラー: {e}")
        total_stats = {"searched": 0, "found": 0, "new": 0, "errors": 1}

    # 5. Excel出力
    try:
        output_path = export_results()
        logger.info(f"Excel exported to {output_path}")
    except Exception as e:
        logger.error(f"Excel出力エラー: {e}")

    # 6. LINE通知
    if total_stats.get("new", 0) > 0:
        try:
            with get_db() as conn:
                new_listings = get_new_listings_today(conn)
            if new_listings:
                send_new_listing_notification(new_listings)
                logger.info(f"Sent LINE notification for {len(new_listings)} new listings")
        except Exception as e:
            logger.error(f"LINE通知エラー: {e}")

    # 7. マップHTML生成
    try:
        generate_map()
    except Exception as e:
        logger.error(f"マップ生成エラー: {e}")

    logger.info("=== CI/CD パイプライン完了 ===")

    # スクレイピングのエラーだけでは失敗にしない（部分的な結果も価値がある）
    if total_stats.get("searched", 0) == 0 and total_stats.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
