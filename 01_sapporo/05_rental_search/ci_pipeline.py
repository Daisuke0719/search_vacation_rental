"""CI/CD パイプライン オーケストレーションスクリプト

GitHub Actions から呼び出され、以下を順次実行する:
1. 建物リスト読み込み・DB登録
2. 賃貸サイトスクレイピング
3. SUUMO掲載の検証（建物名・住所の一致確認）
4. Excel出力
5. 物件評価（収益シミュレーション・スコアリング）
6. LINE通知（新着時）
7. マップHTML生成

各ステージは独立した関数に分離され、統一的なエラーハンドリングと
結果追跡（成功/失敗・所要時間）を行う。
"""

import argparse
import asyncio
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

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
from verify_listings import VerificationScraper, verify_all, print_summary as print_verify_summary
from generate_map import (
    load_listings, geocode_all, build_map_data, render_html,
    find_latest_excel, load_evaluation_scores, MAP_OUTPUT_PATH,
)

logger = logging.getLogger(__name__)

# 物件評価モジュールのディレクトリ
_EVAL_DIR = Path(__file__).resolve().parent.parent / "07_property_evaluation"


# =====================================================================
# ステージ結果の追跡
# =====================================================================

@dataclass
class StageResult:
    """各パイプラインステージの実行結果"""
    name: str
    success: bool
    message: str = ""
    duration_sec: float = 0


def run_stage(name: str, func, *args, **kwargs) -> StageResult:
    """ステージを実行し、結果をログ付きで返す共通ラッパー。

    全ステージに統一的なログ出力・エラーハンドリング・所要時間計測を適用する。
    """
    logger.info(f"=== {name} 開始 ===")
    start = time.time()

    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        msg = result if isinstance(result, str) else "完了"
        logger.info(f"=== {name} 完了 ({elapsed:.1f}s) ===")
        return StageResult(name=name, success=True, message=msg, duration_sec=elapsed)
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"=== {name} 失敗 ({elapsed:.1f}s): {e} ===")
        return StageResult(name=name, success=False, message=str(e), duration_sec=elapsed)


# =====================================================================
# 各ステージ
# =====================================================================

def stage_load_buildings(args) -> str:
    """建物リスト読み込み・フィルタ・DB登録を行い、結果をコンテキストに格納する。"""
    init_db()

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

    groups.sort(key=lambda g: -g.unit_count)

    if args.limit:
        groups = groups[:args.limit]
        logger.info(f"Limited to {len(groups)} buildings")

    building_ids = load_buildings_to_db(groups)
    logger.info(f"Registered {len(building_ids)} buildings in DB")

    # パイプラインコンテキストに保存
    _ctx["groups"] = groups
    _ctx["building_ids"] = building_ids

    return f"{len(groups)}棟登録"


def stage_scrape(args) -> str:
    """賃貸サイトスクレイピングを実行する。"""
    groups = _ctx["groups"]
    building_ids = _ctx["building_ids"]
    sites = args.sites or ENABLED_SITES

    total_stats = asyncio.run(run_search(sites, groups, building_ids))
    _ctx["total_stats"] = total_stats

    return (
        f"検索{total_stats['searched']}, 発見{total_stats['found']}, "
        f"新着{total_stats['new']}, エラー{total_stats['errors']}"
    )


def stage_verify(args) -> str:
    """SUUMO掲載の検証（未検証分のみ）を実行する。"""
    with get_db() as conn:
        unverified = get_unverified_suumo_listings(conn, ward=args.ward)

    if not unverified:
        return "未検証のSUUMO掲載なし"

    logger.info(f"SUUMO掲載検証: {len(unverified)}件")

    async def _run_verify():
        scraper = VerificationScraper()
        async with scraper:
            return await verify_all(unverified, scraper)

    verify_results = asyncio.run(_run_verify())

    # 検証結果をDBに保存
    with get_db() as conn:
        for r in verify_results:
            upsert_verification(
                conn, r.listing_id,
                r.actual_name, r.actual_address,
                r.name_score, r.address_match,
                r.status, r.reason,
            )
    print_verify_summary(verify_results)

    # mismatch/suspicious の掲載を非アクティブ化
    with get_db() as conn:
        result = conn.execute(
            """UPDATE listings SET is_active = 0
               WHERE id IN (
                   SELECT v.listing_id FROM listing_verifications v
                   WHERE v.status IN ('mismatch', 'suspicious')
               ) AND is_active = 1"""
        )
        deactivated = result.rowcount

    msg = f"検証{len(verify_results)}件"
    if deactivated > 0:
        msg += f", NG{deactivated}件を非アクティブ化"
    return msg


def stage_export() -> str:
    """検索結果をExcelファイルに出力する。"""
    output_path = export_results()
    return f"{output_path.name}"


def stage_evaluate() -> str:
    """物件評価を実行する（サブプロセス: config衝突回避）。"""
    eval_script = _EVAL_DIR / "evaluate.py"
    if not eval_script.exists():
        return "評価スクリプト未検出（スキップ）"

    import os
    result = subprocess.run(
        [sys.executable, str(eval_script)],
        capture_output=True, text=True, timeout=120,
        cwd=str(_EVAL_DIR),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    if result.returncode == 0:
        return "評価完了"
    else:
        stderr_preview = result.stderr[:300] if result.stderr else "N/A"
        logger.warning(f"物件評価で警告: {stderr_preview}")
        return f"警告あり（returncode={result.returncode}）"


def stage_notify() -> str:
    """新着物件のLINE通知を送信する。"""
    total_stats = _ctx.get("total_stats", {})
    if total_stats.get("new", 0) == 0:
        return "新着なし（通知スキップ）"

    with get_db() as conn:
        new_listings = get_new_listings_today(conn)

    if not new_listings:
        return "新着データなし"

    send_new_listing_notification(new_listings)
    return f"{len(new_listings)}件通知送信"


def stage_generate_map() -> str:
    """Excel出力からマップHTMLを生成する。"""
    excel_path = find_latest_excel()
    if not excel_path or not excel_path.exists():
        return "Excelファイル未検出（スキップ）"

    df = load_listings(excel_path)
    coords = geocode_all(df)
    scores = load_evaluation_scores()
    properties, unmapped = build_map_data(df, coords, scores)

    render_html(properties, unmapped, MAP_OUTPUT_PATH)
    return f"マップ{len(properties)}件, 未マッピング{len(unmapped)}件"


# =====================================================================
# パイプラインオーケストレーター
# =====================================================================

# ステージ間でデータを受け渡すコンテキスト
_ctx: dict = {}


def print_summary(stages: list[StageResult]) -> None:
    """全ステージの実行結果をサマリー表示する。"""
    total_sec = sum(s.duration_sec for s in stages)
    succeeded = sum(1 for s in stages if s.success)
    failed = sum(1 for s in stages if not s.success)

    print(f"\n{'='*60}")
    print(f"  パイプライン実行サマリー")
    print(f"{'='*60}")
    print(f"  {'ステージ':<22} {'結果':<6} {'時間':>7}  {'詳細'}")
    print(f"  {'-'*56}")

    for s in stages:
        icon = "OK" if s.success else "FAIL"
        time_str = f"{s.duration_sec:.1f}s"
        print(f"  {s.name:<22} {icon:<6} {time_str:>7}  {s.message}")

    print(f"  {'-'*56}")
    print(f"  合計: {succeeded}成功, {failed}失敗, {total_sec:.1f}s")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="CI/CD パイプライン")
    parser.add_argument("--sites", nargs="+", default=None, help="検索対象サイト (例: suumo homes)")
    parser.add_argument("--ward", type=str, default=None, help="区名で絞り込み (例: 中央区)")
    parser.add_argument("--limit", type=int, default=None, help="検索建物数の上限（テスト用）")
    args = parser.parse_args()

    setup_logging()
    logger.info("=== CI/CD パイプライン開始 ===")

    stages: list[StageResult] = []

    # ── Stage 1: 建物リスト読み込み・DB登録 ──
    r = run_stage("建物リスト読み込み", stage_load_buildings, args)
    stages.append(r)
    if not r.success:
        print_summary(stages)
        sys.exit(1)

    # ── Stage 2: スクレイピング ──
    stages.append(run_stage("スクレイピング", stage_scrape, args))

    # ── Stage 3: SUUMO掲載検証 ──
    stages.append(run_stage("SUUMO掲載検証", stage_verify, args))

    # ── Stage 4: Excel出力 ──
    stages.append(run_stage("Excel出力", stage_export))

    # ── Stage 5: 物件評価 ──
    stages.append(run_stage("物件評価", stage_evaluate))

    # ── Stage 6: LINE通知 ──
    stages.append(run_stage("LINE通知", stage_notify))

    # ── Stage 7: マップ生成 ──
    stages.append(run_stage("マップ生成", stage_generate_map))

    # ── サマリー出力 ──
    print_summary(stages)

    logger.info("=== CI/CD パイプライン完了 ===")

    # スクレイピングが完全に失敗した場合のみ終了コード1
    total_stats = _ctx.get("total_stats", {})
    if total_stats.get("searched", 0) == 0 and total_stats.get("errors", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
