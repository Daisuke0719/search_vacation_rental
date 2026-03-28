"""民泊エリア選定のための基本統計量分析スクリプト

内部データ（民泊施設一覧、賃貸物件DB）と外部データ（観光庁統計、Airbnb）を
組み合わせて、札幌市10区別の基本統計量を集計・可視化する。
"""

import sqlite3
import sys
from pathlib import Path

import folium
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import openpyxl
import pandas as pd
import seaborn as sns
from matplotlib import rcParams

# ── パス設定 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
SAPPORO_DIR = BASE_DIR.parent

EXCEL_PATH = SAPPORO_DIR / "00_ref" / "札幌市内の民泊施設一覧.xlsx"
DB_PATH = SAPPORO_DIR / "05_rental_search" / "db" / "rental_search.db"
AIRBNB_DB_PATH = BASE_DIR / "external_data" / "airbnb_listings.db"
GOV_STATS_DIR = BASE_DIR / "external_data"
OUTPUT_DIR = BASE_DIR / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# ── 日本語フォント・デザインシステム ────────────────────────────────
matplotlib.use("Agg")
for font_name in ["Yu Gothic", "Meiryo", "MS Gothic", "Hiragino Sans"]:
    try:
        rcParams["font.family"] = font_name
        break
    except Exception:
        continue
rcParams["axes.unicode_minus"] = False

# ── Design System ──────────────────────────────────────────
# Brand colors
C_PRIMARY = "#1B2838"       # 深いネイビー（タイトル・テキスト）
C_ACCENT = "#E84855"        # 鮮やかな赤（強調・ハイライト）
C_ACCENT2 = "#2B9EB3"       # ティール（セカンダリ）
C_ACCENT3 = "#F9A620"       # ゴールド（第3アクセント）
C_BG = "#FAFBFC"            # 背景
C_GRID = "#E8ECF0"          # グリッド
C_TEXT_MUTED = "#6B7B8D"    # サブテキスト

# 区のカラーパレット（施設数順、暖色→寒色グラデーション）
WARD_COLORS = {
    "中央区": "#E84855",
    "豊平区": "#F26B38",
    "白石区": "#F9A620",
    "北区":   "#7BC950",
    "東区":   "#2B9EB3",
    "西区":   "#4A7DCC",
    "南区":   "#6C5CE7",
    "手稲区": "#A78BFA",
    "厚別区": "#C4B5FD",
    "清田区": "#D4D4D8",
}

# 季節カラー
SEASON_COLORS = {
    "最繁忙期": "#E84855",
    "繁忙期": "#F26B38",
    "準繁忙期": "#2B9EB3",
    "閑散期": "#C4D4E0",
}


def _setup_axes(ax, title="", subtitle="", xlabel="", ylabel="",
                hide_top_right=True, grid_axis="y"):
    """統一されたAxesスタイリング"""
    if title:
        ax.set_title(title, fontsize=16, fontweight="bold", color=C_PRIMARY,
                     pad=20, loc="left")
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes,
                fontsize=10, color=C_TEXT_MUTED, va="bottom")
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=11, color=C_TEXT_MUTED, labelpad=10)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11, color=C_TEXT_MUTED, labelpad=10)
    if hide_top_right:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(C_GRID)
        ax.spines["bottom"].set_color(C_GRID)
    if grid_axis:
        ax.grid(axis=grid_axis, color=C_GRID, linewidth=0.5, alpha=0.8)
        ax.set_axisbelow(True)
    ax.tick_params(colors=C_TEXT_MUTED, labelsize=10)


def _savefig(fig, filename):
    """統一された保存処理"""
    fig.patch.set_facecolor(C_BG)
    fig.savefig(FIGURES_DIR / filename, dpi=200, bbox_inches="tight",
                facecolor=C_BG, edgecolor="none", pad_inches=0.3)
    plt.close()
    print(f"  → {filename}")

# ── 区の参照データ ──────────────────────────────────────────
WARD_DATA = pd.DataFrame([
    {"ward": "中央区", "area_km2": 46.42, "population": 252000, "lat": 43.055, "lon": 141.341},
    {"ward": "北区",   "area_km2": 63.57, "population": 290000, "lat": 43.091, "lon": 141.341},
    {"ward": "東区",   "area_km2": 56.97, "population": 265000, "lat": 43.076, "lon": 141.376},
    {"ward": "白石区", "area_km2": 34.47, "population": 213000, "lat": 43.042, "lon": 141.409},
    {"ward": "豊平区", "area_km2": 46.23, "population": 226000, "lat": 43.032, "lon": 141.383},
    {"ward": "南区",   "area_km2": 657.48, "population": 130000, "lat": 42.990, "lon": 141.353},
    {"ward": "西区",   "area_km2": 75.10, "population": 218000, "lat": 43.074, "lon": 141.299},
    {"ward": "厚別区", "area_km2": 24.38, "population": 125000, "lat": 43.038, "lon": 141.476},
    {"ward": "手稲区", "area_km2": 56.77, "population": 142000, "lat": 43.117, "lon": 141.247},
    {"ward": "清田区", "area_km2": 59.87, "population": 113000, "lat": 43.003, "lon": 141.449},
])

# 月別季節性データ（03_市場競合調査.md より）
SEASONALITY = pd.DataFrame([
    {"month": 1,  "demand_level": 4, "label": "繁忙期", "note": "年末年始・スキー"},
    {"month": 2,  "demand_level": 5, "label": "最繁忙期", "note": "雪まつり"},
    {"month": 3,  "demand_level": 2, "label": "閑散期", "note": "端境期"},
    {"month": 4,  "demand_level": 2, "label": "閑散期", "note": "端境期"},
    {"month": 5,  "demand_level": 3, "label": "準繁忙期", "note": "GW・ライラック"},
    {"month": 6,  "demand_level": 3, "label": "準繁忙期", "note": "よさこい"},
    {"month": 7,  "demand_level": 4, "label": "繁忙期", "note": "夏季観光"},
    {"month": 8,  "demand_level": 4, "label": "繁忙期", "note": "夏季観光"},
    {"month": 9,  "demand_level": 3, "label": "準繁忙期", "note": "紅葉・グルメ"},
    {"month": 10, "demand_level": 2, "label": "閑散期", "note": "紅葉終了後"},
    {"month": 11, "demand_level": 2, "label": "閑散期", "note": "スキー前"},
    {"month": 12, "demand_level": 4, "label": "繁忙期", "note": "スキー開幕"},
])

# ADR推計データ（03_市場競合調査.md + 04_収支シミュレーション.md より）
ADR_ESTIMATES = {
    "中央区": {"peak": 12000, "normal": 8000, "low": 6000, "annual_avg": 8500, "occupancy": 0.68},
    "北区":   {"peak": 10000, "normal": 7000, "low": 5000, "annual_avg": 7200, "occupancy": 0.55},
    "東区":   {"peak": 9000,  "normal": 6500, "low": 4500, "annual_avg": 6600, "occupancy": 0.50},
    "白石区": {"peak": 9000,  "normal": 6500, "low": 4500, "annual_avg": 6600, "occupancy": 0.50},
    "豊平区": {"peak": 10000, "normal": 7000, "low": 5000, "annual_avg": 7200, "occupancy": 0.55},
    "南区":   {"peak": 8000,  "normal": 5500, "low": 4000, "annual_avg": 5800, "occupancy": 0.40},
    "西区":   {"peak": 8500,  "normal": 6000, "low": 4500, "annual_avg": 6300, "occupancy": 0.45},
    "厚別区": {"peak": 8000,  "normal": 5500, "low": 4000, "annual_avg": 5800, "occupancy": 0.38},
    "手稲区": {"peak": 8000,  "normal": 5500, "low": 4000, "annual_avg": 5800, "occupancy": 0.35},
    "清田区": {"peak": 7500,  "normal": 5000, "low": 3500, "annual_avg": 5300, "occupancy": 0.33},
}


# =====================================================================
# データ読み込み
# =====================================================================

def load_minpaku_excel() -> pd.DataFrame:
    """民泊施設一覧Excelを読み込み"""
    print(f"  Loading: {EXCEL_PATH.name}")
    df = pd.read_excel(EXCEL_PATH, header=0)
    df.columns = ["address", "registration_number", "registration_date", "fire_violation"]

    # 区を抽出
    df["ward"] = df["address"].str.extract(r"(中央区|北区|東区|白石区|豊平区|南区|西区|厚別区|手稲区|清田区)")
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    df["year"] = df["registration_date"].dt.year
    df["year_month"] = df["registration_date"].dt.to_period("M")

    print(f"  → {len(df)} records loaded, {df['ward'].notna().sum()} with ward info")
    return df


def load_rental_listings() -> pd.DataFrame:
    """賃貸物件DBからlistingsを読み込み"""
    if not DB_PATH.exists():
        print(f"  SKIP: DB not found at {DB_PATH}")
        return pd.DataFrame()

    print(f"  Loading: rental_search.db")
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query(
        """SELECT l.*, b.building_name, b.ward, b.address_base
           FROM listings l
           JOIN buildings b ON l.building_id = b.id
           WHERE l.is_active = 1""",
        conn,
    )
    conn.close()
    print(f"  → {len(df)} active listings loaded")
    return df


def load_airbnb_listings() -> pd.DataFrame:
    """AirbnbスクレイピングDBを読み込み"""
    if not AIRBNB_DB_PATH.exists():
        print(f"  SKIP: Airbnb DB not found (run scrapers/airbnb.py first)")
        return pd.DataFrame()

    print(f"  Loading: airbnb_listings.db")
    conn = sqlite3.connect(str(AIRBNB_DB_PATH))
    try:
        df = pd.read_sql_query("SELECT * FROM airbnb_listings", conn)
    except Exception as e:
        print(f"  WARNING: Could not read airbnb_listings: {e}")
        df = pd.DataFrame()
    conn.close()

    if not df.empty and "nightly_price" in df.columns:
        # Airbnbはデフォルト検索で合計金額（複数泊分）を表示することがある
        # 中央値が30,000円超の場合、合計金額と判断して1泊あたりに変換
        median_price = df["nightly_price"].median()
        if median_price and median_price > 30000:
            # Airbnbのデフォルト検索は通常5泊分の合計を表示
            est_nights = round(median_price / 8000)  # 札幌の平均ADR約8,000円で推計
            est_nights = max(3, min(7, est_nights))   # 3-7泊の範囲
            print(f"  → 価格を{est_nights}泊分の合計と推定し、1泊あたりに変換")
            df["nightly_price"] = (df["nightly_price"] / est_nights).astype(int)

    print(f"  → {len(df)} Airbnb listings loaded")
    return df


def load_government_stats() -> dict:
    """観光庁統計データを読み込み（CSVがあれば）"""
    stats = {}
    hokkaido_csv = GOV_STATS_DIR / "hokkaido_accommodation_stats.csv"
    if hokkaido_csv.exists():
        print(f"  Loading: {hokkaido_csv.name}")
        stats["accommodation"] = pd.read_csv(hokkaido_csv)
    else:
        print(f"  SKIP: Government stats not found (run fetch_government_stats.py first)")

    minpaku_csv = GOV_STATS_DIR / "minpaku_operation_stats.csv"
    if minpaku_csv.exists():
        print(f"  Loading: {minpaku_csv.name}")
        stats["minpaku_ops"] = pd.read_csv(minpaku_csv)

    # 月別延べ宿泊者数
    monthly_stays_csv = GOV_STATS_DIR / "hokkaido_monthly_stays.csv"
    if monthly_stays_csv.exists():
        print(f"  Loading: {monthly_stays_csv.name}")
        stats["monthly_stays"] = pd.read_csv(monthly_stays_csv)

    # 月別稼働率
    occupancy_csv = GOV_STATS_DIR / "hokkaido_occupancy.csv"
    if occupancy_csv.exists():
        print(f"  Loading: {occupancy_csv.name}")
        stats["occupancy"] = pd.read_csv(occupancy_csv)

    return stats


# =====================================================================
# 分析関数
# =====================================================================

def analyze_supply_by_ward(df_minpaku: pd.DataFrame) -> pd.DataFrame:
    """区別の供給サイド基本統計量"""
    # 施設数
    ward_counts = df_minpaku.groupby("ward").agg(
        facility_count=("registration_number", "count"),
        fire_violations=("fire_violation", lambda x: (x.notna() & (x != "")).sum()),
    ).reset_index()

    # WARD_DATAとマージ
    result = WARD_DATA.merge(ward_counts, on="ward", how="left").fillna(0)

    # 比率・密度
    total = result["facility_count"].sum()
    result["facility_pct"] = (result["facility_count"] / total * 100).round(1)
    result["density_per_km2"] = (result["facility_count"] / result["area_km2"]).round(2)
    result["density_per_1000pop"] = (result["facility_count"] / result["population"] * 1000).round(2)

    # YoY成長率（2024→2025）
    for year in [2024, 2025]:
        yearly = df_minpaku[df_minpaku["year"] == year].groupby("ward").size().reset_index(name=f"reg_{year}")
        result = result.merge(yearly, on="ward", how="left").fillna(0)

    result["yoy_growth_rate"] = np.where(
        result["reg_2024"] > 0,
        ((result["reg_2025"] - result["reg_2024"]) / result["reg_2024"] * 100).round(1),
        0,
    )

    # ADR・稼働率推計
    result["est_adr"] = result["ward"].map(lambda w: ADR_ESTIMATES.get(w, {}).get("annual_avg", 0))
    result["est_occupancy"] = result["ward"].map(lambda w: ADR_ESTIMATES.get(w, {}).get("occupancy", 0))
    result["est_monthly_revenue"] = (result["est_adr"] * result["est_occupancy"] * 30).astype(int)

    return result.sort_values("facility_count", ascending=False)


def analyze_registration_trend(df_minpaku: pd.DataFrame) -> pd.DataFrame:
    """区別の年間登録推移"""
    trend = df_minpaku.groupby(["year", "ward"]).size().reset_index(name="count")
    trend = trend[trend["year"].notna() & (trend["year"] >= 2018)]
    trend["year"] = trend["year"].astype(int)
    return trend


def analyze_rental_costs(df_listings: pd.DataFrame) -> pd.DataFrame:
    """賃貸物件の基本統計量"""
    if df_listings.empty:
        return pd.DataFrame()

    df = df_listings[df_listings["rent_price"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # 間取り別統計
    stats = df.groupby("floor_plan")["rent_price"].describe().reset_index()
    stats.columns = ["floor_plan", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]

    # 面積あたり単価
    df_area = df[df["area_sqm"].notna() & (df["area_sqm"] > 0)].copy()
    if not df_area.empty:
        df_area["rent_per_sqm"] = df_area["rent_price"] / df_area["area_sqm"]

    return stats


def analyze_airbnb_by_ward(df_airbnb: pd.DataFrame) -> pd.DataFrame:
    """Airbnbデータの区別統計"""
    if df_airbnb.empty:
        return pd.DataFrame()

    stats = df_airbnb.groupby("ward").agg(
        listing_count=("listing_url", "count"),
        avg_price=("nightly_price", "mean"),
        median_price=("nightly_price", "median"),
        min_price=("nightly_price", "min"),
        max_price=("nightly_price", "max"),
        std_price=("nightly_price", "std"),
        avg_rating=("rating", "mean"),
        avg_reviews=("review_count", "mean"),
        superhost_pct=("superhost", "mean"),
    ).reset_index()

    return stats.round(1)


# =====================================================================
# 可視化関数
# =====================================================================

def plot_facility_count(supply_df: pd.DataFrame):
    """1. 区別 民泊施設数 横棒グラフ"""
    fig, ax = plt.subplots(figsize=(11, 7))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    df = supply_df.sort_values("facility_count")
    colors = [WARD_COLORS.get(w, "#999") for w in df["ward"]]

    bars = ax.barh(df["ward"], df["facility_count"], color=colors,
                   height=0.65, edgecolor="white", linewidth=0.5)

    # 値ラベル（バーの中 or 外）
    max_val = df["facility_count"].max()
    for bar, (_, row) in zip(bars, df.iterrows()):
        val = int(row["facility_count"])
        pct = row["facility_pct"]
        if val > max_val * 0.15:
            ax.text(bar.get_width() - max_val * 0.02,
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:,}", va="center", ha="right",
                    fontsize=12, fontweight="bold", color="white")
            ax.text(bar.get_width() + max_val * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f"{pct}%", va="center", ha="left",
                    fontsize=10, color=C_TEXT_MUTED)
        else:
            ax.text(bar.get_width() + max_val * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    f"{val:,}  ({pct}%)", va="center", ha="left",
                    fontsize=10, color=C_PRIMARY)

    _setup_axes(ax, title="区別 民泊届出施設数",
                subtitle="札幌市 2026年3月時点  |  全2,756件",
                xlabel="施設数", grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.set_xlim(0, max_val * 1.15)
    _savefig(fig, "01_facility_count_by_ward.png")


def plot_registration_trend(trend_df: pd.DataFrame):
    """2. 区別 年間登録推移 積み上げエリアチャート"""
    if trend_df.empty:
        return

    pivot = trend_df.pivot_table(index="year", columns="ward", values="count", fill_value=0)
    order = pivot.sum().sort_values(ascending=False).index
    pivot = pivot[order]
    ward_palette = [WARD_COLORS.get(w, "#999") for w in order]

    fig, ax = plt.subplots(figsize=(13, 6.5))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    pivot.plot.area(ax=ax, alpha=0.75, stacked=True, color=ward_palette, linewidth=0.5)

    _setup_axes(ax, title="民泊新規登録数の推移",
                subtitle="札幌市 2018-2026年  |  区別・年次推移",
                xlabel="年", ylabel="新規登録数")

    # 凡例をシンプルに
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[:5], labels[:5], loc="upper left", frameon=False,
              fontsize=9, ncol=1, labelcolor=C_TEXT_MUTED)

    # 2025年にアノテーション
    if 2025 in pivot.index:
        total_2025 = pivot.loc[2025].sum()
        ax.annotate(f"2025年\n{int(total_2025):,}件",
                    xy=(2025, total_2025), xytext=(2024.2, total_2025 * 0.85),
                    fontsize=10, fontweight="bold", color=C_ACCENT,
                    arrowprops=dict(arrowstyle="->", color=C_ACCENT, lw=1.5))

    ax.set_xlim(pivot.index.min() - 0.3, pivot.index.max() + 0.3)
    _savefig(fig, "02_registration_trend.png")


def plot_rent_distribution(df_listings: pd.DataFrame):
    """3. 賃料分布 間取り別ボックスプロット"""
    if df_listings.empty:
        return

    df = df_listings[df_listings["rent_price"].notna()].copy()
    if df.empty:
        return

    top_plans = df["floor_plan"].value_counts().head(8).index
    df = df[df["floor_plan"].isin(top_plans)]

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    df["rent_万円"] = df["rent_price"] / 10000

    palette = [C_ACCENT2, "#3DB8AD", "#4AC29A", "#5BCC88",
               C_ACCENT3, "#E8943A", C_ACCENT, "#C23B48"][:len(top_plans)]

    bp = sns.boxplot(data=df, x="floor_plan", y="rent_万円", hue="floor_plan",
                     ax=ax, palette=palette, legend=False,
                     width=0.55, linewidth=1.2, fliersize=4,
                     boxprops=dict(alpha=0.85),
                     medianprops=dict(color=C_PRIMARY, linewidth=2))

    # 中央値ラベル
    for i, plan in enumerate(top_plans):
        subset = df[df["floor_plan"] == plan]["rent_万円"]
        if not subset.empty:
            median_val = subset.median()
            ax.text(i, median_val + 0.3, f"{median_val:.1f}万",
                    ha="center", fontsize=9, fontweight="bold", color=C_PRIMARY)

    _setup_axes(ax, title="間取り別 賃料分布",
                subtitle="賃貸物件DB（中央区中心）  |  中央値を表示",
                xlabel="間取り", ylabel="賃料（万円/月）")
    _savefig(fig, "03_rent_distribution.png")


def plot_airbnb_prices(df_airbnb: pd.DataFrame):
    """4. Airbnb ADR分布 区別"""
    if df_airbnb.empty:
        return

    df = df_airbnb[df_airbnb["nightly_price"].notna()].copy()
    if len(df) < 5:
        return

    fig, ax = plt.subplots(figsize=(13, 6.5))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    ward_order = df.groupby("ward")["nightly_price"].median().sort_values(ascending=False).index
    palette = [WARD_COLORS.get(w, "#999") for w in ward_order]

    # ストリッププロット + ボックスプロット（洗練された組み合わせ）
    sns.boxplot(data=df, x="ward", y="nightly_price", order=ward_order,
                hue="ward", palette=palette, legend=False,
                ax=ax, width=0.45, linewidth=1, fliersize=0,
                boxprops=dict(alpha=0.3),
                medianprops=dict(color=C_PRIMARY, linewidth=2.5),
                whiskerprops=dict(color=C_TEXT_MUTED),
                capprops=dict(color=C_TEXT_MUTED))
    sns.stripplot(data=df, x="ward", y="nightly_price", order=ward_order,
                  hue="ward", palette=palette, legend=False,
                  ax=ax, size=5, alpha=0.5, jitter=0.2)

    # 中央値ラベル
    for i, w in enumerate(ward_order):
        subset = df[df["ward"] == w]["nightly_price"]
        median_val = subset.median()
        ax.text(i, median_val + subset.max() * 0.03,
                f"¥{median_val:,.0f}", ha="center", fontsize=9,
                fontweight="bold", color=C_PRIMARY,
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                          edgecolor=C_GRID, alpha=0.9))

    _setup_axes(ax, title="Airbnb 区別 宿泊単価分布",
                subtitle=f"スクレイピング結果（{len(df)}件）  |  中央値を表示  |  1泊あたり推計額",
                xlabel="区", ylabel="1泊あたり推計料金（円）")
    _savefig(fig, "04_airbnb_price_distribution.png")


def plot_seasonality():
    """5. 月別宿泊需要（季節性）"""
    from matplotlib.patches import Patch, FancyBboxPatch

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    colors = [SEASON_COLORS[label] for label in SEASONALITY["label"]]

    # 角丸バー風（通常barを使い、丸みは太さで表現）
    bars = ax.bar(SEASONALITY["month"], SEASONALITY["demand_level"],
                  color=colors, alpha=0.85, width=0.7,
                  edgecolor="white", linewidth=1.5, zorder=3)

    # 各バーの上にアイコンテキスト
    icons = {
        1: "ski", 2: "snowflake", 3: "", 4: "", 5: "flower",
        6: "dancer", 7: "sun", 8: "sun", 9: "leaf", 10: "", 11: "", 12: "ski"
    }
    for _, row in SEASONALITY.iterrows():
        y_pos = row["demand_level"] + 0.2
        ax.text(row["month"], y_pos, row["note"],
                ha="center", fontsize=8.5, color=C_PRIMARY, fontweight="medium",
                bbox=dict(boxstyle="round,pad=0.15", facecolor="white",
                          edgecolor=C_GRID, alpha=0.8))

    ax.set_xticks(range(1, 13))
    ax.set_xticklabels([f"{m}月" for m in range(1, 13)], fontsize=11)
    ax.set_ylim(0, 6.5)
    ax.set_yticks([1, 2, 3, 4, 5])
    ax.set_yticklabels(["1\n低", "2", "3\n中", "4", "5\n高"], fontsize=10)

    _setup_axes(ax, title="月別 宿泊需要の季節性",
                subtitle="札幌市  |  1(低)〜5(高)の需要レベル",
                ylabel="需要レベル", grid_axis="y")

    # 凡例（下部に横並び）
    legend_elements = [Patch(facecolor=c, edgecolor="white", label=l)
                       for l, c in SEASON_COLORS.items()]
    ax.legend(handles=legend_elements, loc="upper center",
              bbox_to_anchor=(0.5, -0.08), ncol=4, frameon=False,
              fontsize=10, labelcolor=C_TEXT_MUTED)

    _savefig(fig, "05_seasonality.png")


def plot_gov_stats(gov_stats: dict):
    """5b. 観光庁統計: 月別宿泊者数・稼働率"""
    monthly_stays = gov_stats.get("monthly_stays")
    occupancy = gov_stats.get("occupancy")

    if monthly_stays is None or monthly_stays.empty:
        return

    df = monthly_stays[monthly_stays["month"].notna()].copy()
    df["month"] = df["month"].astype(int)

    fig, ax1 = plt.subplots(figsize=(14, 7))
    fig.set_facecolor(C_BG)
    ax1.set_facecolor(C_BG)

    bar_w = 0.35
    x = np.array(df["month"])

    # 全体（メインバー）
    ax1.bar(x - bar_w / 2, df["total_stays"] / 10000, width=bar_w,
            color=C_ACCENT2, alpha=0.8, label="延べ宿泊者数（全体）",
            edgecolor="white", linewidth=0.8, zorder=3)
    # 外国人（セカンダリバー）
    ax1.bar(x + bar_w / 2, df["foreign_stays"] / 10000, width=bar_w,
            color=C_ACCENT3, alpha=0.8, label="うち外国人",
            edgecolor="white", linewidth=0.8, zorder=3)

    ax1.set_xticks(range(1, 13))
    ax1.set_xticklabels([f"{m}月" for m in range(1, 13)], fontsize=11)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["left"].set_color(C_GRID)
    ax1.spines["bottom"].set_color(C_GRID)
    ax1.grid(axis="y", color=C_GRID, linewidth=0.5, alpha=0.8)
    ax1.set_axisbelow(True)
    ax1.tick_params(colors=C_TEXT_MUTED, labelsize=10)
    ax1.set_ylabel("延べ宿泊者数（万人泊）", fontsize=11, color=C_TEXT_MUTED)
    ax1.legend(loc="upper left", frameon=False, fontsize=10, labelcolor=C_TEXT_MUTED)

    # 稼働率（第2軸）
    if occupancy is not None and not occupancy.empty:
        occ_room = occupancy[(occupancy["rate_type"] == "客室稼働率") & (occupancy["month"].notna())].copy()
        occ_room["month"] = occ_room["month"].astype(int)

        ax2 = ax1.twinx()
        ax2.plot(occ_room["month"], occ_room["total"], "o-",
                 color=C_ACCENT, linewidth=2.5, markersize=7,
                 markerfacecolor="white", markeredgewidth=2.5,
                 label="客室稼働率", zorder=5)
        if "simple_inn" in occ_room.columns:
            ax2.plot(occ_room["month"], occ_room["simple_inn"], "s--",
                     color="#6C5CE7", linewidth=1.8, markersize=5,
                     markerfacecolor="white", markeredgewidth=2,
                     label="簡易宿所稼働率", zorder=5)
        ax2.set_ylabel("稼働率（%）", fontsize=11, color=C_TEXT_MUTED)
        ax2.set_ylim(35, 90)
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.spines["right"].set_color(C_GRID)
        ax2.tick_params(colors=C_TEXT_MUTED, labelsize=10)
        ax2.legend(loc="upper right", frameon=False, fontsize=10, labelcolor=C_TEXT_MUTED)

        # ピーク月をハイライト
        peak_month = occ_room.loc[occ_room["total"].idxmax(), "month"]
        peak_val = occ_room["total"].max()
        ax2.annotate(f"{peak_val:.1f}%",
                     xy=(peak_month, peak_val), xytext=(peak_month + 0.8, peak_val + 3),
                     fontsize=11, fontweight="bold", color=C_ACCENT,
                     arrowprops=dict(arrowstyle="->", color=C_ACCENT, lw=1.5))

    ax1.set_title("月別 延べ宿泊者数と稼働率",
                   fontsize=16, fontweight="bold", color=C_PRIMARY, pad=20, loc="left")
    ax1.text(0, 1.02, "北海道 2025年  |  観光庁 宿泊旅行統計調査",
             transform=ax1.transAxes, fontsize=10, color=C_TEXT_MUTED, va="bottom")

    _savefig(fig, "05b_gov_monthly_stats.png")


def plot_supply_demand_matrix(supply_df: pd.DataFrame):
    """6. 供給-需要マトリクス 散布図"""
    fig, ax = plt.subplots(figsize=(12, 9))
    fig.set_facecolor(C_BG)
    ax.set_facecolor(C_BG)

    df = supply_df.copy()
    x = df["est_monthly_revenue"]
    y = df["facility_count"]
    sizes = df["yoy_growth_rate"].clip(lower=0) * 5 + 80
    ward_colors = [WARD_COLORS.get(w, "#999") for w in df["ward"]]

    # バブル
    for i, (_, row) in enumerate(df.iterrows()):
        ax.scatter(row["est_monthly_revenue"], row["facility_count"],
                   s=sizes.iloc[i], color=ward_colors[i], alpha=0.7,
                   edgecolors="white", linewidth=2, zorder=5)

    # ラベル（重ならないよう調整）
    for _, row in df.iterrows():
        offset_x, offset_y = 12, 8
        if row["ward"] == "中央区":
            offset_x, offset_y = -15, -25
        ax.annotate(row["ward"],
                    (row["est_monthly_revenue"], row["facility_count"]),
                    fontsize=11, fontweight="bold", color=C_PRIMARY,
                    xytext=(offset_x, offset_y), textcoords="offset points",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                              edgecolor=C_GRID, alpha=0.85))

    # 象限ライン + ラベル
    med_x, med_y = x.median(), y.median()
    ax.axhline(y=med_y, color=C_GRID, linestyle="-", linewidth=1.5, zorder=1)
    ax.axvline(x=med_x, color=C_GRID, linestyle="-", linewidth=1.5, zorder=1)

    # 象限ラベル
    x_range = x.max() - x.min()
    y_range = y.max() - y.min()
    quadrant_style = dict(fontsize=9, color=C_TEXT_MUTED, alpha=0.6, fontstyle="italic")
    ax.text(med_x + x_range * 0.3, med_y + y_range * 0.35, "高需要・高競争",
            ha="center", **quadrant_style)
    ax.text(med_x - x_range * 0.2, med_y + y_range * 0.35, "低需要・高競争",
            ha="center", **quadrant_style)
    ax.text(med_x + x_range * 0.3, med_y - y_range * 0.15, "高需要・低競争\n(狙い目)",
            ha="center", fontsize=10, color=C_ACCENT2, alpha=0.8, fontweight="bold")
    ax.text(med_x - x_range * 0.2, med_y - y_range * 0.15, "低需要・低競争",
            ha="center", **quadrant_style)

    _setup_axes(ax, title="供給-需要マトリクス",
                subtitle="バブルサイズ = YoY成長率  |  推計値に基づく分析",
                xlabel="推計月間売上（円）", ylabel="民泊施設数（競争度）")

    # X軸フォーマット
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"¥{v/10000:.0f}万"))

    _savefig(fig, "06_supply_demand_matrix.png")


def plot_density_comparison(supply_df: pd.DataFrame):
    """7. 施設密度比較（/km² と /千人）"""
    fig, axes = plt.subplots(1, 2, figsize=(15, 7))
    fig.set_facecolor(C_BG)

    for ax_i in axes:
        ax_i.set_facecolor(C_BG)

    # 左: 面積あたり
    df1 = supply_df.sort_values("density_per_km2", ascending=True)
    colors1 = [WARD_COLORS.get(w, "#999") for w in df1["ward"]]
    bars1 = axes[0].barh(df1["ward"], df1["density_per_km2"], color=colors1,
                         height=0.6, edgecolor="white", linewidth=0.8)
    for bar in bars1:
        w = bar.get_width()
        axes[0].text(w + 0.3, bar.get_y() + bar.get_height() / 2,
                     f"{w:.1f}", va="center", fontsize=9, color=C_PRIMARY, fontweight="bold")
    _setup_axes(axes[0], title="面積あたり施設密度", xlabel="施設数 / km²", grid_axis="x")
    axes[0].spines["left"].set_visible(False)
    axes[0].tick_params(axis="y", length=0)

    # 右: 人口あたり
    df2 = supply_df.sort_values("density_per_1000pop", ascending=True)
    colors2 = [WARD_COLORS.get(w, "#999") for w in df2["ward"]]
    bars2 = axes[1].barh(df2["ward"], df2["density_per_1000pop"], color=colors2,
                         height=0.6, edgecolor="white", linewidth=0.8)
    for bar in bars2:
        w = bar.get_width()
        axes[1].text(w + 0.05, bar.get_y() + bar.get_height() / 2,
                     f"{w:.1f}", va="center", fontsize=9, color=C_PRIMARY, fontweight="bold")
    _setup_axes(axes[1], title="人口あたり施設密度", xlabel="施設数 / 千人", grid_axis="x")
    axes[1].spines["left"].set_visible(False)
    axes[1].tick_params(axis="y", length=0)

    fig.suptitle("区別 民泊施設密度比較", fontsize=16, fontweight="bold",
                 color=C_PRIMARY, x=0.05, ha="left", y=0.98)
    fig.text(0.05, 0.94, "※南区は面積657km²（山岳地帯含む）のため密度が低く見える",
             fontsize=9, color=C_TEXT_MUTED)

    plt.subplots_adjust(top=0.88, wspace=0.35)
    _savefig(fig, "07_density_comparison.png")


def create_heatmap(supply_df: pd.DataFrame, df_airbnb: pd.DataFrame):
    """8. エリアヒートマップ（folium地図）"""
    m = folium.Map(
        location=[43.055, 141.345],
        zoom_start=11,
        tiles="cartodbpositron",
        control_scale=True,
    )

    max_count = supply_df["facility_count"].max()

    for _, row in supply_df.iterrows():
        ward = row["ward"]
        color = WARD_COLORS.get(ward, "#999")

        # Airbnbデータ
        airbnb_html = ""
        if not df_airbnb.empty and "ward" in df_airbnb.columns:
            ward_airbnb = df_airbnb[df_airbnb["ward"] == ward]
            if len(ward_airbnb) > 0:
                avg_price = ward_airbnb["nightly_price"].mean()
                med_price = ward_airbnb["nightly_price"].median()
                airbnb_html = f"""
                <tr><td colspan="2" style="padding-top: 8px; border-top: 1px solid #eee;">
                    <span style="font-size: 10px; color: #888; text-transform: uppercase;
                          letter-spacing: 1px;">Airbnb実データ</span></td></tr>
                <tr><td>平均単価</td><td>¥{avg_price:,.0f}/泊</td></tr>
                <tr><td>中央値</td><td>¥{med_price:,.0f}/泊</td></tr>
                <tr><td>掲載数</td><td>{len(ward_airbnb)}件</td></tr>
                """

        # ポップアップ（モダンデザイン）
        popup_html = f"""
        <div style="font-family: -apple-system, 'Segoe UI', sans-serif; width: 280px; padding: 4px;">
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <div style="width: 12px; height: 12px; border-radius: 50%;
                     background: {color}; margin-right: 8px;"></div>
                <h3 style="margin: 0; font-size: 18px; color: #1B2838;">{ward}</h3>
            </div>
            <table style="font-size: 13px; width: 100%; border-collapse: collapse; line-height: 1.8;">
                <tr><td style="color: #6B7B8D;">民泊施設数</td>
                    <td style="font-weight: 700; text-align: right; font-size: 15px;">{int(row['facility_count'])}
                    <span style="font-size: 11px; color: #888; font-weight: 400;">({row['facility_pct']}%)</span></td></tr>
                <tr><td style="color: #6B7B8D;">密度</td>
                    <td style="text-align: right;">{row['density_per_km2']}/km² ・ {row['density_per_1000pop']}/千人</td></tr>
                <tr><td style="color: #6B7B8D;">YoY成長率</td>
                    <td style="text-align: right; color: {'#2B9EB3' if row['yoy_growth_rate'] > 0 else '#E84855'};">
                    {'+' if row['yoy_growth_rate'] > 0 else ''}{row['yoy_growth_rate']}%</td></tr>
                <tr><td colspan="2" style="padding-top: 8px; border-top: 1px solid #eee;">
                    <span style="font-size: 10px; color: #888; text-transform: uppercase;
                          letter-spacing: 1px;">収益推計</span></td></tr>
                <tr><td style="color: #6B7B8D;">ADR</td>
                    <td style="text-align: right;">¥{int(row['est_adr']):,}/泊</td></tr>
                <tr><td style="color: #6B7B8D;">稼働率</td>
                    <td style="text-align: right;">{row['est_occupancy']*100:.0f}%</td></tr>
                <tr><td style="color: #6B7B8D;">月間売上</td>
                    <td style="text-align: right; font-weight: 700; color: #1B2838;">
                    ¥{int(row['est_monthly_revenue']):,}</td></tr>
                {airbnb_html}
            </table>
        </div>
        """

        radius = max(600, int(np.sqrt(row["facility_count"]) * 220))

        folium.Circle(
            location=[row["lat"], row["lon"]],
            radius=radius,
            popup=folium.Popup(popup_html, max_width=320),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.35,
            weight=2.5,
            opacity=0.8,
        ).add_to(m)

        # 区名ラベル（クリーンなデザイン）
        label_size = max(11, min(16, int(10 + row["facility_count"] / 200)))
        folium.Marker(
            location=[row["lat"], row["lon"]],
            icon=folium.DivIcon(
                html=f'''<div style="font-family: -apple-system, 'Segoe UI', sans-serif;
                          text-align: center; pointer-events: none;">
                    <span style="font-size: {label_size}px; font-weight: 700; color: {color};
                          text-shadow: 0 0 4px white, 0 0 4px white, 0 0 4px white;">{ward}</span><br>
                    <span style="font-size: 10px; color: #555; background: rgba(255,255,255,0.85);
                          padding: 1px 5px; border-radius: 8px;">{int(row["facility_count"]):,}件</span>
                </div>''',
                icon_size=(100, 40),
                icon_anchor=(50, 20),
            ),
        ).add_to(m)

    # 凡例（モダンデザイン）
    legend_html = """
    <div style="position: fixed; bottom: 24px; left: 24px; z-index: 999;
         background: rgba(255,255,255,0.95); backdrop-filter: blur(10px);
         padding: 16px 20px; border-radius: 12px;
         box-shadow: 0 2px 12px rgba(0,0,0,0.1);
         font-family: -apple-system, 'Segoe UI', sans-serif; font-size: 12px;">
        <div style="font-weight: 700; font-size: 13px; color: #1B2838; margin-bottom: 8px;">
            民泊施設の分布</div>
        <div style="color: #6B7B8D; line-height: 1.6;">
            <span style="display: inline-block; width: 10px; height: 10px;
                  border-radius: 50%; background: #E84855; margin-right: 6px;
                  vertical-align: middle;"></span>高密度エリア<br>
            <span style="display: inline-block; width: 10px; height: 10px;
                  border-radius: 50%; background: #2B9EB3; margin-right: 6px;
                  vertical-align: middle;"></span>中密度エリア<br>
            <span style="display: inline-block; width: 10px; height: 10px;
                  border-radius: 50%; background: #C4B5FD; margin-right: 6px;
                  vertical-align: middle;"></span>低密度エリア
        </div>
        <div style="margin-top: 8px; font-size: 10px; color: #999;">
            円の大きさ = 施設数<br>クリックで詳細表示</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # タイトル
    title_html = """
    <div style="position: fixed; top: 12px; left: 50%; transform: translateX(-50%);
         z-index: 999; background: rgba(255,255,255,0.95); backdrop-filter: blur(10px);
         padding: 10px 24px; border-radius: 12px;
         box-shadow: 0 2px 12px rgba(0,0,0,0.1);
         font-family: -apple-system, 'Segoe UI', sans-serif;">
        <span style="font-size: 16px; font-weight: 700; color: #1B2838;">
            札幌市 民泊エリアマップ</span>
        <span style="font-size: 12px; color: #6B7B8D; margin-left: 12px;">
            2026年3月時点 | 全2,756件</span>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    output_path = OUTPUT_DIR / "area_heatmap.html"
    m.save(str(output_path))
    print(f"  → area_heatmap.html")


# =====================================================================
# Excel出力
# =====================================================================

def export_to_excel(supply_df: pd.DataFrame, trend_df: pd.DataFrame,
                    rental_stats: pd.DataFrame, airbnb_stats: pd.DataFrame,
                    gov_stats: dict = None):
    """集計結果をExcelに出力"""
    output_path = OUTPUT_DIR / "area_stats_summary.xlsx"

    with pd.ExcelWriter(str(output_path), engine="xlsxwriter") as writer:
        workbook = writer.book

        # フォーマット定義
        header_fmt = workbook.add_format({
            "bold": True, "bg_color": "#4472C4", "font_color": "white",
            "border": 1, "text_wrap": True, "align": "center",
        })
        number_fmt = workbook.add_format({"num_format": "#,##0", "border": 1})
        pct_fmt = workbook.add_format({"num_format": "0.0%", "border": 1})
        float_fmt = workbook.add_format({"num_format": "#,##0.0", "border": 1})

        # Sheet1: 区別サマリー
        cols = ["ward", "facility_count", "facility_pct", "density_per_km2",
                "density_per_1000pop", "yoy_growth_rate", "est_adr",
                "est_occupancy", "est_monthly_revenue", "area_km2", "population"]
        headers_ja = ["区", "施設数", "構成比(%)", "密度(/km²)",
                      "密度(/千人)", "YoY成長率(%)", "推計ADR(円/泊)",
                      "推計稼働率", "推計月間売上(円)", "面積(km²)", "人口"]

        df_out = supply_df[cols].copy()
        df_out.to_excel(writer, sheet_name="区別サマリー", index=False, startrow=1)

        ws = writer.sheets["区別サマリー"]
        for i, h in enumerate(headers_ja):
            ws.write(0, i, h, header_fmt)
        ws.set_column("A:A", 10)
        ws.set_column("B:K", 14)

        # Sheet2: 賃料詳細
        if not rental_stats.empty:
            rental_stats.to_excel(writer, sheet_name="賃料詳細（間取り別）", index=False, startrow=1)
            ws2 = writer.sheets["賃料詳細（間取り別）"]
            for i, h in enumerate(rental_stats.columns):
                ws2.write(0, i, h, header_fmt)

        # Sheet3: Airbnb価格分布
        if not airbnb_stats.empty:
            airbnb_stats.to_excel(writer, sheet_name="Airbnb価格分布", index=False, startrow=1)
            ws3 = writer.sheets["Airbnb価格分布"]
            for i, h in enumerate(airbnb_stats.columns):
                ws3.write(0, i, h, header_fmt)

        # Sheet4: 登録推移
        if not trend_df.empty:
            pivot = trend_df.pivot_table(
                index="year", columns="ward", values="count", fill_value=0
            ).reset_index()
            pivot.to_excel(writer, sheet_name="年別登録推移", index=False, startrow=1)
            ws4 = writer.sheets["年別登録推移"]
            for i, h in enumerate(pivot.columns):
                ws4.write(0, i, str(h), header_fmt)

        # Sheet5: 季節性
        SEASONALITY.to_excel(writer, sheet_name="月別季節性", index=False, startrow=1)
        ws5 = writer.sheets["月別季節性"]
        for i, h in enumerate(["月", "需要レベル", "区分", "備考"]):
            ws5.write(0, i, h, header_fmt)

        # Sheet6: 観光庁統計（月別宿泊者数）
        if gov_stats and "monthly_stays" in gov_stats:
            ms = gov_stats["monthly_stays"]
            ms.to_excel(writer, sheet_name="月別宿泊者数（観光庁）", index=False, startrow=1)
            ws6 = writer.sheets["月別宿泊者数（観光庁）"]
            for i, h in enumerate(ms.columns):
                ws6.write(0, i, h, header_fmt)

        # Sheet7: 観光庁統計（月別稼働率）
        if gov_stats and "occupancy" in gov_stats:
            occ = gov_stats["occupancy"]
            occ.to_excel(writer, sheet_name="月別稼働率（観光庁）", index=False, startrow=1)
            ws7 = writer.sheets["月別稼働率（観光庁）"]
            for i, h in enumerate(occ.columns):
                ws7.write(0, i, h, header_fmt)

    print(f"  → area_stats_summary.xlsx")


# =====================================================================
# メイン実行
# =====================================================================

def main():
    print("=" * 60)
    print("  民泊エリア選定 基本統計量分析")
    print("=" * 60)

    # ── データ読み込み ──
    print("\n[1/5] データ読み込み...")
    df_minpaku = load_minpaku_excel()
    df_listings = load_rental_listings()
    df_airbnb = load_airbnb_listings()
    gov_stats = load_government_stats()

    # ── 分析 ──
    print("\n[2/5] 基本統計量を算出...")
    supply_df = analyze_supply_by_ward(df_minpaku)
    trend_df = analyze_registration_trend(df_minpaku)
    rental_stats = analyze_rental_costs(df_listings)
    airbnb_stats = analyze_airbnb_by_ward(df_airbnb)

    # サマリー表示
    print("\n  ── 区別サマリー ──")
    display_cols = ["ward", "facility_count", "facility_pct", "density_per_km2",
                    "est_adr", "est_occupancy", "est_monthly_revenue"]
    print(supply_df[display_cols].to_string(index=False))

    # ── 可視化 ──
    print("\n[3/5] チャート生成...")
    plot_facility_count(supply_df)
    plot_registration_trend(trend_df)
    plot_rent_distribution(df_listings)
    plot_airbnb_prices(df_airbnb)
    plot_seasonality()
    plot_gov_stats(gov_stats)
    plot_supply_demand_matrix(supply_df)
    plot_density_comparison(supply_df)

    # ── 地図 ──
    print("\n[4/5] エリアヒートマップ生成...")
    create_heatmap(supply_df, df_airbnb)

    # ── Excel出力 ──
    print("\n[5/5] Excel出力...")
    export_to_excel(supply_df, trend_df, rental_stats, airbnb_stats, gov_stats)

    print("\n" + "=" * 60)
    print("  分析完了!")
    print(f"  出力先: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
