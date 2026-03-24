"""検索結果をExcelに出力するモジュール"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from config import OUTPUT_DIR, DB_PATH
from models.database import get_db, get_all_active_listings, get_all_buildings, get_search_stats


def export_results(output_dir: Path = None) -> Path:
    """検索結果をExcelファイルに出力する"""
    out_dir = output_dir or OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"rental_search_results_{timestamp}.xlsx"

    with get_db() as conn:
        stats = get_search_stats(conn)
        active_listings = get_all_active_listings(conn)
        buildings = get_all_buildings(conn)

        # 掲載一覧データ
        listings_data = []
        for row in active_listings:
            listings_data.append({
                "建物名": row["building_name"],
                "区": row["ward"],
                "住所": row["address_base"],
                "サイト": row["site_name"],
                "物件タイトル": row["listing_title"],
                "家賃（円）": row["rent_price"],
                "管理費（円）": row["management_fee"],
                "敷金": row["deposit"],
                "礼金": row["key_money"],
                "間取り": row["floor_plan"],
                "面積（㎡）": row["area_sqm"],
                "階数": row["floor_number"],
                "築年数": row["building_age"],
                "最寄駅": row["nearest_station"],
                "徒歩（分）": row["walk_minutes"],
                "URL": row["listing_url"],
                "初回検出日": row["first_seen_at"],
                "最終確認日": row["last_seen_at"],
            })

        # 建物一覧データ
        buildings_data = []
        for row in buildings:
            buildings_data.append({
                "建物名": row["building_name"],
                "区": row["ward"],
                "住所": row["address_base"],
                "民泊登録ユニット数": row["unit_count"],
            })

        # サイト別集計
        site_summary = {}
        for item in listings_data:
            site = item["サイト"]
            site_summary.setdefault(site, {"件数": 0, "平均家賃": []})
            site_summary[site]["件数"] += 1
            if item["家賃（円）"]:
                site_summary[site]["平均家賃"].append(item["家賃（円）"])

        site_summary_data = []
        for site, data in site_summary.items():
            avg_rent = (
                int(sum(data["平均家賃"]) / len(data["平均家賃"]))
                if data["平均家賃"] else None
            )
            site_summary_data.append({
                "サイト": site,
                "掲載件数": data["件数"],
                "平均家賃（円）": avg_rent,
            })

    # Excel書き出し
    with pd.ExcelWriter(str(output_path), engine="xlsxwriter") as writer:
        # サマリーシート
        summary_df = pd.DataFrame([{
            "項目": "検索建物数",
            "値": stats["total_buildings"],
        }, {
            "項目": "掲載あり建物数",
            "値": stats["buildings_with_listings"],
        }, {
            "項目": "総アクティブ掲載数",
            "値": stats["total_active_listings"],
        }, {
            "項目": "本日新着数",
            "値": stats["new_today"],
        }, {
            "項目": "出力日時",
            "値": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }])
        summary_df.to_excel(writer, sheet_name="サマリー", index=False)

        # サイト別集計
        if site_summary_data:
            pd.DataFrame(site_summary_data).to_excel(
                writer, sheet_name="サイト別集計", index=False
            )

        # 掲載一覧
        if listings_data:
            pd.DataFrame(listings_data).to_excel(
                writer, sheet_name="掲載一覧", index=False
            )

        # 建物一覧
        if buildings_data:
            pd.DataFrame(buildings_data).to_excel(
                writer, sheet_name="建物一覧", index=False
            )

        # カラム幅の自動調整
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column("A:R", 18)

    return output_path
