"""観光庁 宿泊旅行統計調査Excelのパース

ダウンロード済みの年間統計Excelから北海道のデータを抽出し、
分析用CSVに変換する。

出力:
- external_data/hokkaido_monthly_stays.csv  (月別延べ宿泊者数)
- external_data/hokkaido_occupancy.csv      (月別稼働率)
- external_data/hokkaido_accommodation_stats.csv (更新版サマリー)
"""

import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "external_data" / "shukuhaku_001984048.xlsx"
OUTPUT_DIR = BASE_DIR / "external_data"

MONTHS = list(range(1, 13))
MONTH_NAMES = [f"{m}月" for m in MONTHS]


def extract_hokkaido_row(xls, sheet_name: str, keyword: str = "北海道") -> list:
    """シートから北海道の行データを抽出"""
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    for _, row in df.iterrows():
        row_str = " ".join(str(v) for v in row.values if pd.notna(v))
        if keyword in row_str:
            return [v for v in row.values]
    return []


def extract_monthly_stays(xls) -> pd.DataFrame:
    """第2表: 月別延べ宿泊者数（北海道）"""
    print("[1/3] 月別延べ宿泊者数を抽出...")
    records = []

    # 年計
    row = extract_hokkaido_row(xls, "第2表(年計)")
    if row:
        records.append({
            "period": "年計",
            "total_stays": row[1] if len(row) > 1 else None,
            "foreign_stays": row[8] if len(row) > 8 else None,
        })

    # 月別
    for m in MONTHS:
        sheet_name = f"第2表({m}月)"
        row = extract_hokkaido_row(xls, sheet_name)
        if row:
            records.append({
                "period": f"{m}月",
                "month": m,
                "total_stays": row[1] if len(row) > 1 else None,
                "foreign_stays": row[8] if len(row) > 8 else None,
            })

    df = pd.DataFrame(records)
    csv_path = OUTPUT_DIR / "hokkaido_monthly_stays.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  → {csv_path.name} ({len(df)} records)")
    print(df.to_string(index=False))
    return df


def extract_monthly_occupancy(xls) -> pd.DataFrame:
    """第6表(定員稼働率) + 第8表(客室稼働率): 月別稼働率"""
    print("\n[2/3] 月別稼働率を抽出...")
    records = []

    # ヘッダー構造（第8表）:
    # col 0: 都道府県名
    # col 1: 客室稼働率(全体)
    # col 2-7: 従業者数別（観光/非観光）
    # col 8以降: 施設タイプ別（旅館、リゾート、ビジネス、シティ、簡易宿所、会社等保養所）
    #
    # 北海道 row 7: [北海道, 61.2, 60.2, 67.2, 37.5, 30.1, 43.9, 71.3, 67.0, 74.3, ...]

    for table_prefix, rate_type in [("第8表", "客室稼働率"), ("第6表", "定員稼働率")]:
        # 年計
        row = extract_hokkaido_row(xls, f"{table_prefix}(年計)")
        if row:
            records.append({
                "period": "年計",
                "rate_type": rate_type,
                "total": row[1] if len(row) > 1 else None,
                "ryokan": row[7] if len(row) > 7 else None,
                "resort_hotel": row[8] if len(row) > 8 else None,
                "business_hotel": row[9] if len(row) > 9 else None,
                "city_hotel": row[10] if len(row) > 10 else None,
                "simple_inn": row[11] if len(row) > 11 else None,
            })

        # 月別
        for m in MONTHS:
            sheet_name = f"{table_prefix}({m}月)"
            row = extract_hokkaido_row(xls, sheet_name)
            if row:
                records.append({
                    "period": f"{m}月",
                    "month": m,
                    "rate_type": rate_type,
                    "total": row[1] if len(row) > 1 else None,
                    "ryokan": row[7] if len(row) > 7 else None,
                    "resort_hotel": row[8] if len(row) > 8 else None,
                    "business_hotel": row[9] if len(row) > 9 else None,
                    "city_hotel": row[10] if len(row) > 10 else None,
                    "simple_inn": row[11] if len(row) > 11 else None,
                })

    df = pd.DataFrame(records)
    csv_path = OUTPUT_DIR / "hokkaido_occupancy.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  → {csv_path.name} ({len(df)} records)")

    # 見やすく表示
    df_room = df[df["rate_type"] == "客室稼働率"].copy()
    print("\n  客室稼働率(%):")
    print(df_room[["period", "total", "business_hotel", "city_hotel", "simple_inn"]].to_string(index=False))
    return df


def update_summary_csv(stays_df: pd.DataFrame, occ_df: pd.DataFrame):
    """サマリーCSVを更新"""
    print("\n[3/3] サマリーCSV更新...")

    records = []

    # 年計の延べ宿泊者数
    annual = stays_df[stays_df["period"] == "年計"].iloc[0] if len(stays_df[stays_df["period"] == "年計"]) > 0 else None
    if annual is not None:
        records.append({"year": 2025, "metric": "北海道_延べ宿泊者数", "value": annual["total_stays"],
                        "unit": "人泊", "source": "観光庁 宿泊旅行統計調査 2025年確報"})
        records.append({"year": 2025, "metric": "北海道_外国人延べ宿泊者数", "value": annual["foreign_stays"],
                        "unit": "人泊", "source": "観光庁 宿泊旅行統計調査 2025年確報"})

    # 月別の延べ宿泊者数
    monthly = stays_df[stays_df["period"] != "年計"].copy()
    for _, row in monthly.iterrows():
        records.append({"year": 2025, "metric": f"北海道_{row['period']}_延べ宿泊者数",
                        "value": row["total_stays"], "unit": "人泊", "source": "観光庁"})
        records.append({"year": 2025, "metric": f"北海道_{row['period']}_外国人宿泊者数",
                        "value": row["foreign_stays"], "unit": "人泊", "source": "観光庁"})

    # 稼働率（客室稼働率のみ）
    occ_room = occ_df[occ_df["rate_type"] == "客室稼働率"]
    annual_occ = occ_room[occ_room["period"] == "年計"]
    if len(annual_occ) > 0:
        r = annual_occ.iloc[0]
        records.append({"year": 2025, "metric": "北海道_客室稼働率_全体", "value": r["total"],
                        "unit": "%", "source": "観光庁"})
        records.append({"year": 2025, "metric": "北海道_客室稼働率_ビジネスホテル", "value": r["business_hotel"],
                        "unit": "%", "source": "観光庁"})
        records.append({"year": 2025, "metric": "北海道_客室稼働率_シティホテル", "value": r["city_hotel"],
                        "unit": "%", "source": "観光庁"})
        records.append({"year": 2025, "metric": "北海道_客室稼働率_簡易宿所", "value": r["simple_inn"],
                        "unit": "%", "source": "観光庁"})

    # 月別稼働率
    monthly_occ = occ_room[occ_room["period"] != "年計"]
    for _, row in monthly_occ.iterrows():
        records.append({"year": 2025, "metric": f"北海道_{row['period']}_客室稼働率",
                        "value": row["total"], "unit": "%", "source": "観光庁"})

    # 既存データとマージ
    existing_csv = OUTPUT_DIR / "hokkaido_accommodation_stats.csv"
    if existing_csv.exists():
        existing = pd.read_csv(existing_csv)
        # 2024年以前のデータは保持
        existing = existing[existing["year"] < 2025]
        new_df = pd.concat([existing, pd.DataFrame(records)], ignore_index=True)
    else:
        new_df = pd.DataFrame(records)

    new_df.to_csv(existing_csv, index=False, encoding="utf-8-sig")
    print(f"  → hokkaido_accommodation_stats.csv ({len(new_df)} records)")


def main():
    print("=" * 60)
    print("  観光庁Excel パース")
    print("=" * 60)

    if not EXCEL_PATH.exists():
        print(f"ERROR: Excel not found: {EXCEL_PATH}")
        print("Run fetch_government_stats.py first")
        sys.exit(1)

    xls = pd.ExcelFile(EXCEL_PATH)
    stays_df = extract_monthly_stays(xls)
    occ_df = extract_monthly_occupancy(xls)
    update_summary_csv(stays_df, occ_df)

    print("\n" + "=" * 60)
    print("  パース完了!")
    print("=" * 60)


if __name__ == "__main__":
    main()
