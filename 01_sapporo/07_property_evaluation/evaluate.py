"""物件評価エンジン - 札幌市民泊事業向け総合物件スコアリング

賃貸物件データとAirbnb/Booking類似物件データを統合し、
収益シミュレーション・スコアリングを行う。

使用方法:
    python evaluate.py                           # 全物件評価（民泊新法）
    python evaluate.py --business-type kaniyado  # 簡易宿所として評価
    python evaluate.py --output results.xlsx     # 出力ファイル指定
    python evaluate.py --self-managed            # 運営代行なし
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── 自モジュールのconfig読み込み ──
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import OUTPUT_DIR

# ── 共通パス（rental_search.db への書き込み用） ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared.paths import RENTAL_DB_PATH

from data_loader import load_rental_listings, load_airbnb_comps, load_booking_comps, find_similar_properties
from floor_plan import parse_floor_plan
from revenue import estimate_adr, estimate_occupancy, simulate_revenue
from scoring import score_property
from excel_output import export_results

# ── ログ設定 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# =====================================================================
# 8. メイン評価パイプライン
# =====================================================================

def evaluate_all(
    business_type: str = "minpaku",
    self_managed: bool = False,
) -> tuple[pd.DataFrame, list[dict]]:
    """全物件の評価パイプラインを実行する。

    Returns:
        (results_df, details_list): スコア付きDataFrameと詳細情報リスト
    """
    # データ読み込み
    rental_df = load_rental_listings()
    if rental_df.empty:
        logger.error("賃貸物件データがありません。評価を中止します。")
        return pd.DataFrame(), []

    airbnb_df = load_airbnb_comps()
    booking_df = load_booking_comps()

    results = []
    details = []

    for idx, rental in rental_df.iterrows():
        listing_id = rental["listing_id"]
        logger.info(
            f"[{idx+1}/{len(rental_df)}] {rental.get('building_name', '不明')} "
            f"- {rental.get('floor_plan', '?')} ({rental.get('ward', '?')})"
        )

        try:
            # 1) 間取りパース
            plan_info = parse_floor_plan(rental.get("floor_plan"))

            # 2) 類似物件検索
            similar = find_similar_properties(rental, airbnb_df, booking_df)
            similar_count = len(similar)

            # 3) ADR推計
            adr_est = estimate_adr(similar, plan_info)

            # 4) 稼働率推計
            occ_est = estimate_occupancy(similar, rental.get("ward", ""))

            # 5) 収益シミュレーション
            rev = simulate_revenue(
                rental, adr_est, occ_est,
                business_type=business_type,
                self_managed=self_managed,
            )

            # 6) スコアリング
            scores = score_property(rental, rev, similar_count, plan_info, similar)

            # 結果まとめ
            row = {
                "listing_id": listing_id,
                "building_name": rental.get("building_name", ""),
                "ward": rental.get("ward", ""),
                "address": rental.get("address_base", ""),
                "floor_plan": rental.get("floor_plan", ""),
                "area_sqm": rental.get("area_sqm"),
                "rent_price": rental.get("rent_price"),
                "management_fee": rental.get("management_fee"),
                "walk_minutes": rental.get("walk_minutes"),
                "nearest_station": rental.get("nearest_station", ""),
                "building_age": rental.get("building_age", ""),
                "estimated_capacity": plan_info["estimated_capacity"],
                "similar_count": similar_count,
                "adr_method": adr_est.get("method", ""),
                "peak_adr": adr_est["peak_adr"],
                "normal_adr": adr_est["normal_adr"],
                "offpeak_adr": adr_est["offpeak_adr"],
                "weighted_avg_adr": adr_est["weighted_avg_adr"],
                "occupancy_method": occ_est.get("method", ""),
                "peak_occupancy": occ_est["peak_occupancy"],
                "normal_occupancy": occ_est["normal_occupancy"],
                "offpeak_occupancy": occ_est["offpeak_occupancy"],
                "annual_revenue": rev["annual_revenue"],
                "annual_cost": rev["annual_cost"],
                "annual_profit": rev["annual_profit"],
                "annual_roi": rev["annual_roi"],
                "score_profitability": scores["profitability"],
                "score_location": scores["location"],
                "score_demand": scores["demand_stability"],
                "score_quality": scores["property_quality"],
                "score_risk": scores["risk"],
                "total_score": scores["total"],
                "listing_url": rental.get("listing_url", ""),
            }
            results.append(row)

            details.append({
                "listing_id": listing_id,
                "plan_info": plan_info,
                "similar_properties": similar,
                "adr_estimates": adr_est,
                "occupancy_estimates": occ_est,
                "revenue": rev,
                "scores": scores,
            })

        except Exception as e:
            logger.error(f"  評価エラー (listing_id={listing_id}): {e}")
            continue

    if not results:
        logger.warning("評価結果がありません。")
        return pd.DataFrame(), []

    results_df = pd.DataFrame(results)
    results_df.sort_values("total_score", ascending=False, inplace=True)
    results_df.reset_index(drop=True, inplace=True)
    results_df.index += 1  # 1始まりのランキング

    logger.info(f"\n評価完了: {len(results_df)}件")
    return results_df, details


# =====================================================================
# 9. 評価スコアのDB保存
# =====================================================================

def save_scores_to_db(results_df: pd.DataFrame, business_type: str = "minpaku"):
    """評価スコアを rental_search.db の evaluation_scores テーブルに保存する。

    evaluate.py は subprocess で実行されるため、05_rental_search/models/database.py に
    依存せず直接 SQLite に接続する。
    """
    if not RENTAL_DB_PATH.exists():
        logger.warning(f"賃貸DB未検出、スコア保存スキップ: {RENTAL_DB_PATH}")
        return

    conn = sqlite3.connect(str(RENTAL_DB_PATH))
    try:
        # テーブルが無ければ作成（init_dbが未実行の場合のフォールバック）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS evaluation_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER REFERENCES listings(id),
                business_type TEXT DEFAULT 'minpaku',
                total_score REAL,
                score_profitability REAL,
                score_location REAL,
                score_demand REAL,
                score_quality REAL,
                score_risk REAL,
                annual_revenue INTEGER,
                annual_cost INTEGER,
                annual_profit INTEGER,
                annual_roi REAL,
                weighted_avg_adr INTEGER,
                similar_count INTEGER,
                estimated_capacity INTEGER,
                evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(listing_id, business_type)
            )
        """)

        saved = 0
        for _, row in results_df.iterrows():
            conn.execute(
                """INSERT INTO evaluation_scores
                   (listing_id, business_type, total_score,
                    score_profitability, score_location, score_demand,
                    score_quality, score_risk,
                    annual_revenue, annual_cost, annual_profit, annual_roi,
                    weighted_avg_adr, similar_count, estimated_capacity,
                    evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(listing_id, business_type) DO UPDATE SET
                     total_score = excluded.total_score,
                     score_profitability = excluded.score_profitability,
                     score_location = excluded.score_location,
                     score_demand = excluded.score_demand,
                     score_quality = excluded.score_quality,
                     score_risk = excluded.score_risk,
                     annual_revenue = excluded.annual_revenue,
                     annual_cost = excluded.annual_cost,
                     annual_profit = excluded.annual_profit,
                     annual_roi = excluded.annual_roi,
                     weighted_avg_adr = excluded.weighted_avg_adr,
                     similar_count = excluded.similar_count,
                     estimated_capacity = excluded.estimated_capacity,
                     evaluated_at = excluded.evaluated_at""",
                (int(row["listing_id"]), business_type,
                 row["total_score"],
                 row["score_profitability"], row["score_location"],
                 row["score_demand"], row["score_quality"], row["score_risk"],
                 int(row["annual_revenue"]), int(row["annual_cost"]),
                 int(row["annual_profit"]), row["annual_roi"],
                 int(row["weighted_avg_adr"]),
                 int(row["similar_count"]), int(row["estimated_capacity"]),
                 datetime.now().isoformat()),
            )
            saved += 1

        conn.commit()
        logger.info(f"評価スコアをDBに保存: {saved}件 → {RENTAL_DB_PATH.name}")
    except Exception as e:
        logger.error(f"評価スコアDB保存エラー: {e}")
    finally:
        conn.close()


# =====================================================================
# 10. CLI
# =====================================================================

def main():
    """CLIエントリーポイント。"""
    parser = argparse.ArgumentParser(
        description="物件評価エンジン - 札幌市民泊事業向け総合物件スコアリング"
    )
    parser.add_argument(
        "--business-type",
        choices=["minpaku", "kaniyado"],
        default="minpaku",
        help="事業形態: minpaku=民泊新法(180日), kaniyado=簡易宿所(365日) (default: minpaku)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="出力Excelファイルパス (default: output/evaluation_YYYYMMDD_HHMMSS.xlsx)",
    )
    parser.add_argument(
        "--self-managed",
        action="store_true",
        help="自主管理モード（運営代行費用なし）",
    )
    args = parser.parse_args()

    # 出力パス
    if args.output:
        output_path = Path(args.output)
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUT_DIR / f"evaluation_{timestamp}.xlsx"

    # ヘッダー出力
    btype_label = "簡易宿所（365日営業）" if args.business_type == "kaniyado" else "民泊新法（180日営業）"
    mgmt_label = "自主管理" if args.self_managed else "運営代行あり"

    print("=" * 60)
    print("  物件評価エンジン - 札幌市民泊事業")
    print("=" * 60)
    print(f"  事業形態: {btype_label}")
    print(f"  運営方式: {mgmt_label}")
    print(f"  出力先:   {output_path}")
    print("=" * 60)

    # 評価実行
    results_df, details = evaluate_all(
        business_type=args.business_type,
        self_managed=args.self_managed,
    )

    if results_df.empty:
        print("\n評価対象の物件がありませんでした。")
        sys.exit(1)

    # Excel出力
    export_results(
        results_df, details, output_path,
        business_type=args.business_type,
        self_managed=args.self_managed,
    )

    # DB保存
    save_scores_to_db(results_df, business_type=args.business_type)

    # サマリー表示
    print("\n" + "=" * 60)
    print("  評価結果サマリー")
    print("=" * 60)
    print(f"  評価物件数: {len(results_df)}件")
    print(f"  平均スコア: {results_df['total_score'].mean():.1f}/100")
    print(f"  最高スコア: {results_df['total_score'].max()}/100")
    print(f"  平均年間利益: {results_df['annual_profit'].mean():,.0f}円")
    print(f"  平均ROI: {results_df['annual_roi'].mean():.1f}%")

    # トップ5表示
    print("\n  --- トップ5物件 ---")
    top5 = results_df.head(5)
    for _, row in top5.iterrows():
        print(
            f"  [{row['total_score']:>3}点] {row['building_name']}"
            f" ({row['ward']}) {row['floor_plan']}"
            f" 家賃{row['rent_price']:,}円"
            f" → 年間利益{row['annual_profit']:,}円"
            f" (ROI {row['annual_roi']}%)"
        )

    print(f"\n  詳細: {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
