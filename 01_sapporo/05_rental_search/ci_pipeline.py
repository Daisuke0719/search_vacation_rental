"""CI/CD パイプライン オーケストレーションスクリプト

GitHub Actions から呼び出され、以下を順次実行する:
1. 建物リスト読み込み・DB登録
2. 賃貸サイトスクレイピング
3. SUUMO掲載の検証（建物名・住所の一致確認）
4. Excel出力
5. 物件評価（収益シミュレーション・スコアリング）
6. LINE通知（新着時）
7. マップHTML生成
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# 物件評価モジュールのディレクトリ
_EVAL_DIR = Path(__file__).resolve().parent.parent / "07_property_evaluation"

from config import ENABLED_SITES, BUILDINGS_CSV, LOG_DIR
from extractors.building_name import (
    load_and_extract, load_buildings_csv, export_buildings_csv,
)
from models.database import (
    init_db, get_db, get_new_listings_today,
    get_unverified_suumo_listings, upsert_verification,
)
from main import load_buildings_to_db, run_search, setup_logging
from exporters.excel_export import export_results
from notifications.line_notify import send_new_listing_notification
from verify_listings import VerificationScraper, verify_all, print_summary
from generate_map import (
    load_listings, geocode_all, build_map_data, render_html,
    find_latest_excel, load_evaluation_scores, MAP_OUTPUT_PATH,
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

    logger.info("[MAP] 評価スコア読み込み")
    scores = load_evaluation_scores()

    logger.info("[MAP] マップデータ構築")
    properties, unmapped = build_map_data(df, coords, scores)
    logger.info(f"[MAP] マップ表示: {len(properties)}件, 未マッピング: {len(unmapped)}件")

    logger.info("[MAP] HTML生成")
    render_html(properties, unmapped, MAP_OUTPUT_PATH)
    logger.info(f"[MAP] マップHTML生成完了: {MAP_OUTPUT_PATH}")
    return True


def main():
    parser = argparse.ArgumentParser(description="CI/CD パイプライン")
    parser.add_argument("--sites", nargs="+", default=None, help="検索対象サイト (例: suumo homes)")
    parser.add_argument("--ward", type=str, default=None, help="区名で絞り込み (例: 中央区)")
    parser.add_argument("--limit", type=int, default=None, help="検索建物数の上限（テスト用）")
    args = parser.parse_args()

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

    # フィルタ
    if args.ward:
        groups = [g for g in groups if g.ward == args.ward]
        logger.info(f"Filtered to {len(groups)} buildings in {args.ward}")

    # ユニット数の多い建物を優先
    groups.sort(key=lambda g: -g.unit_count)

    if args.limit:
        groups = groups[:args.limit]
        logger.info(f"Limited to {len(groups)} buildings")

    # 3. 建物情報をDBに登録
    building_ids = load_buildings_to_db(groups)
    logger.info(f"Registered {len(building_ids)} buildings in DB")

    # 4. スクレイピング実行
    sites = args.sites or ENABLED_SITES
    try:
        total_stats = asyncio.run(
            run_search(sites, groups, building_ids)
        )
        logger.info(
            f"検索完了: {total_stats['searched']} searched, "
            f"{total_stats['found']} found, {total_stats['new']} new, "
            f"{total_stats['errors']} errors"
        )
    except Exception as e:
        logger.error(f"スクレイピング中にエラー: {e}")
        total_stats = {"searched": 0, "found": 0, "new": 0, "errors": 1}

    # 5. SUUMO掲載の検証（未検証分のみ）
    try:
        with get_db() as conn:
            unverified = get_unverified_suumo_listings(conn, ward=args.ward)
        if unverified:
            logger.info(f"=== SUUMO掲載検証開始: {len(unverified)}件 ===")

            async def run_verify():
                scraper = VerificationScraper()
                async with scraper:
                    return await verify_all(unverified, scraper)

            verify_results = asyncio.run(run_verify())

            # 検証結果をDBに保存
            with get_db() as conn:
                for r in verify_results:
                    upsert_verification(
                        conn, r.listing_id,
                        r.actual_name, r.actual_address,
                        r.name_score, r.address_match,
                        r.status, r.reason,
                    )
            print_summary(verify_results)
            logger.info("検証結果をDBに保存しました")

            # mismatch/suspicious の掲載を非アクティブ化
            with get_db() as conn:
                result = conn.execute(
                    """UPDATE listings SET is_active = 0
                       WHERE id IN (
                           SELECT v.listing_id FROM listing_verifications v
                           WHERE v.status IN ('mismatch', 'suspicious')
                       ) AND is_active = 1"""
                )
                if result.rowcount > 0:
                    logger.info(f"検証NG掲載を非アクティブ化: {result.rowcount}件")
        else:
            logger.info("未検証のSUUMO掲載はありません")
    except Exception as e:
        logger.error(f"SUUMO検証エラー: {e}")

    # 6. Excel出力
    try:
        output_path = export_results()
        logger.info(f"Excel exported to {output_path}")
    except Exception as e:
        logger.error(f"Excel出力エラー: {e}")

    # 7. 物件評価実行（サブプロセスで実行: config衝突回避）
    try:
        import subprocess
        eval_script = _EVAL_DIR / "evaluate.py"
        if eval_script.exists():
            logger.info("=== 物件評価開始 ===")
            result = subprocess.run(
                [sys.executable, str(eval_script)],
                capture_output=True, text=True, timeout=120,
                cwd=str(_EVAL_DIR),
                env={**__import__("os").environ, "PYTHONIOENCODING": "utf-8"},
            )
            if result.returncode == 0:
                logger.info("物件評価完了")
            else:
                logger.warning(f"物件評価で警告: {result.stderr[:500] if result.stderr else 'N/A'}")
        else:
            logger.warning("評価スクリプトが見つかりません")
    except Exception as e:
        logger.error(f"物件評価エラー（パイプライン続行）: {e}")

    # 8. LINE通知
    if total_stats.get("new", 0) > 0:
        try:
            with get_db() as conn:
                new_listings = get_new_listings_today(conn)
            if new_listings:
                send_new_listing_notification(new_listings)
                logger.info(f"Sent LINE notification for {len(new_listings)} new listings")
        except Exception as e:
            logger.error(f"LINE通知エラー: {e}")

    # 9. マップHTML生成
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
