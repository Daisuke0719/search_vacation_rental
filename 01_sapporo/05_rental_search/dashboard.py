"""Streamlit ダッシュボード - 民泊物件 賃貸掲載リサーチ

Usage:
    streamlit run dashboard.py
"""

import sys
from pathlib import Path

# PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from config import DB_PATH
from models.database import get_db

st.set_page_config(
    page_title="民泊物件 賃貸掲載リサーチ",
    page_icon="🏠",
    layout="wide",
)

st.title("民泊物件 賃貸掲載リサーチ ダッシュボード")


# --- データ読み込み ---

@st.cache_data(ttl=300)
def load_data():
    """DBからデータを読み込み"""
    if not DB_PATH.exists():
        return None, None, None, None

    with get_db() as conn:
        # 掲載一覧
        listings_df = pd.read_sql_query(
            """SELECT l.*, b.building_name, b.ward, b.address_base, b.unit_count
               FROM listings l
               JOIN buildings b ON l.building_id = b.id
               ORDER BY l.first_seen_at DESC""",
            conn,
        )
        # 建物一覧
        buildings_df = pd.read_sql_query(
            "SELECT * FROM buildings ORDER BY ward, building_name", conn
        )
        # 検索実行履歴
        runs_df = pd.read_sql_query(
            "SELECT * FROM search_runs ORDER BY id DESC LIMIT 30", conn
        )
        # 検索ログ（直近のエラー）
        errors_df = pd.read_sql_query(
            """SELECT sl.*, b.building_name
               FROM search_log sl
               JOIN buildings b ON sl.building_id = b.id
               WHERE sl.search_status = 'error'
               ORDER BY sl.searched_at DESC LIMIT 100""",
            conn,
        )

    return listings_df, buildings_df, runs_df, errors_df


listings_df, buildings_df, runs_df, errors_df = load_data()

if listings_df is None or buildings_df is None:
    st.warning(
        "データベースが見つかりません。先に `python main.py` を実行してください。"
    )
    st.stop()


# --- サイドバー フィルタ ---

st.sidebar.header("フィルタ")

# 区フィルタ
all_wards = sorted(listings_df["ward"].unique()) if len(listings_df) > 0 else []
selected_wards = st.sidebar.multiselect("区", all_wards, default=all_wards)

# サイトフィルタ
all_sites = sorted(listings_df["site_name"].unique()) if len(listings_df) > 0 else []
selected_sites = st.sidebar.multiselect("サイト", all_sites, default=all_sites)

# アクティブのみ
active_only = st.sidebar.checkbox("アクティブのみ", value=True)

# 家賃範囲
if len(listings_df) > 0 and listings_df["rent_price"].notna().any():
    min_rent = int(listings_df["rent_price"].min() or 0)
    max_rent = int(listings_df["rent_price"].max() or 500000)
    rent_range = st.sidebar.slider(
        "家賃範囲（円）",
        min_value=0,
        max_value=max(max_rent, 500000),
        value=(0, max(max_rent, 500000)),
        step=10000,
    )
else:
    rent_range = (0, 500000)

# フィルタ適用
filtered_df = listings_df.copy()
if len(filtered_df) > 0:
    if selected_wards:
        filtered_df = filtered_df[filtered_df["ward"].isin(selected_wards)]
    if selected_sites:
        filtered_df = filtered_df[filtered_df["site_name"].isin(selected_sites)]
    if active_only:
        filtered_df = filtered_df[filtered_df["is_active"] == 1]
    if filtered_df["rent_price"].notna().any():
        filtered_df = filtered_df[
            (filtered_df["rent_price"].isna())
            | (
                (filtered_df["rent_price"] >= rent_range[0])
                & (filtered_df["rent_price"] <= rent_range[1])
            )
        ]


# === タブ構成 ===

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "概要", "物件一覧", "建物詳細", "価格分析", "検索状況"
])


# --- Tab 1: 概要 ---

with tab1:
    # KPI カード
    col1, col2, col3, col4 = st.columns(4)

    total_buildings = len(buildings_df) if buildings_df is not None else 0
    buildings_with_listings = (
        filtered_df["building_id"].nunique() if len(filtered_df) > 0 else 0
    )
    total_listings = len(filtered_df)

    today = datetime.now().strftime("%Y-%m-%d")
    new_today = 0
    if len(filtered_df) > 0 and "first_seen_at" in filtered_df.columns:
        new_today = len(
            filtered_df[filtered_df["first_seen_at"].str[:10] == today]
        )

    col1.metric("検索対象建物数", f"{total_buildings:,}")
    col2.metric("掲載あり建物数", f"{buildings_with_listings:,}")
    col3.metric("掲載件数", f"{total_listings:,}")
    col4.metric("本日新着", f"{new_today:,}")

    if len(filtered_df) > 0:
        st.subheader("サイト別掲載件数")
        site_counts = filtered_df["site_name"].value_counts().reset_index()
        site_counts.columns = ["サイト", "件数"]
        fig1 = px.bar(site_counts, x="サイト", y="件数", color="サイト")
        fig1.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig1, use_container_width=True)

        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("区別掲載件数")
            ward_counts = filtered_df["ward"].value_counts().reset_index()
            ward_counts.columns = ["区", "件数"]
            fig2 = px.bar(ward_counts, x="区", y="件数", color="区")
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

        with col_b:
            st.subheader("新着推移（直近30日）")
            if "first_seen_at" in filtered_df.columns:
                daily = filtered_df.copy()
                daily["date"] = pd.to_datetime(daily["first_seen_at"]).dt.date
                daily_counts = daily.groupby("date").size().reset_index(name="件数")
                daily_counts = daily_counts.tail(30)
                fig3 = px.line(daily_counts, x="date", y="件数")
                fig3.update_layout(height=350, xaxis_title="日付")
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("掲載データがまだありません。検索を実行してください。")


# --- Tab 2: 物件一覧 ---

with tab2:
    st.subheader("掲載物件一覧")

    if len(filtered_df) > 0:
        display_cols = [
            "building_name", "ward", "site_name", "rent_price",
            "floor_plan", "area_sqm", "floor_number",
            "listing_title", "listing_url", "first_seen_at",
        ]
        display_cols = [c for c in display_cols if c in filtered_df.columns]
        display_df = filtered_df[display_cols].copy()
        display_df.columns = [
            c.replace("building_name", "建物名")
            .replace("ward", "区")
            .replace("site_name", "サイト")
            .replace("rent_price", "家賃")
            .replace("floor_plan", "間取り")
            .replace("area_sqm", "面積㎡")
            .replace("floor_number", "階数")
            .replace("listing_title", "タイトル")
            .replace("listing_url", "URL")
            .replace("first_seen_at", "初回検出")
            for c in display_cols
        ]

        st.dataframe(
            display_df,
            use_container_width=True,
            height=600,
            column_config={
                "URL": st.column_config.LinkColumn("URL"),
                "家賃": st.column_config.NumberColumn("家賃", format="%d円"),
            },
        )

        # Excel DL
        csv = display_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "CSVダウンロード",
            csv,
            file_name=f"listings_{today}.csv",
            mime="text/csv",
        )
    else:
        st.info("掲載データがありません。")


# --- Tab 3: 建物詳細 ---

with tab3:
    st.subheader("建物詳細")

    if buildings_df is not None and len(buildings_df) > 0:
        building_options = [
            f"{row['building_name']} ({row['ward']}) - {row['unit_count']}ユニット"
            for _, row in buildings_df.iterrows()
        ]
        selected_idx = st.selectbox(
            "建物を選択",
            range(len(building_options)),
            format_func=lambda i: building_options[i],
        )

        if selected_idx is not None:
            building = buildings_df.iloc[selected_idx]
            bid = building["id"]

            st.write(f"**住所**: {building['address_base']}")
            st.write(f"**区**: {building['ward']}")
            st.write(f"**民泊登録ユニット数**: {building['unit_count']}")

            # この建物の掲載
            bld_listings = listings_df[listings_df["building_id"] == bid]
            if len(bld_listings) > 0:
                st.write(f"**掲載数**: {len(bld_listings)} 件 (アクティブ: {len(bld_listings[bld_listings['is_active']==1])})")

                # サイト別比較
                st.subheader("サイト別掲載")
                for site in bld_listings["site_name"].unique():
                    site_data = bld_listings[bld_listings["site_name"] == site]
                    with st.expander(f"{site} ({len(site_data)}件)"):
                        st.dataframe(site_data[[
                            "listing_title", "rent_price", "floor_plan",
                            "area_sqm", "listing_url", "is_active",
                        ]])

                # 家賃比較
                if bld_listings["rent_price"].notna().any():
                    st.subheader("サイト別家賃比較")
                    fig = px.box(
                        bld_listings[bld_listings["rent_price"].notna()],
                        x="site_name", y="rent_price",
                        labels={"site_name": "サイト", "rent_price": "家賃（円）"},
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("この建物の賃貸掲載は見つかっていません。")


# --- Tab 4: 価格分析 ---

with tab4:
    st.subheader("価格分析")

    rent_df = filtered_df[filtered_df["rent_price"].notna()].copy()

    if len(rent_df) > 0:
        col_x, col_y = st.columns(2)

        with col_x:
            st.subheader("家賃分布")
            fig4 = px.histogram(
                rent_df, x="rent_price", nbins=30,
                labels={"rent_price": "家賃（円）"},
            )
            fig4.update_layout(height=400)
            st.plotly_chart(fig4, use_container_width=True)

        with col_y:
            st.subheader("区別家賃")
            fig5 = px.box(
                rent_df, x="ward", y="rent_price",
                labels={"ward": "区", "rent_price": "家賃（円）"},
            )
            fig5.update_layout(height=400)
            st.plotly_chart(fig5, use_container_width=True)

        if rent_df["area_sqm"].notna().any():
            st.subheader("面積 vs 家賃")
            fig6 = px.scatter(
                rent_df[rent_df["area_sqm"].notna()],
                x="area_sqm", y="rent_price", color="ward",
                labels={"area_sqm": "面積（㎡）", "rent_price": "家賃（円）"},
                hover_data=["building_name", "site_name"],
            )
            fig6.update_layout(height=500)
            st.plotly_chart(fig6, use_container_width=True)

        # 平均家賃テーブル
        st.subheader("区×間取り別 平均家賃")
        if "floor_plan" in rent_df.columns and rent_df["floor_plan"].notna().any():
            pivot = rent_df.pivot_table(
                values="rent_price", index="ward", columns="floor_plan",
                aggfunc="mean",
            ).round(0)
            st.dataframe(pivot.style.format("{:,.0f}"), use_container_width=True)
    else:
        st.info("家賃データがありません。")


# --- Tab 5: 検索状況 ---

with tab5:
    st.subheader("検索実行履歴")

    if runs_df is not None and len(runs_df) > 0:
        st.dataframe(
            runs_df[[
                "id", "run_started_at", "run_finished_at",
                "buildings_searched", "total_listings_found",
                "new_listings_count", "errors_count", "status",
            ]],
            use_container_width=True,
        )

    if errors_df is not None and len(errors_df) > 0:
        st.subheader("直近のエラー")
        st.dataframe(
            errors_df[["building_name", "site_name", "error_message", "searched_at"]],
            use_container_width=True,
        )

    # データリロードボタン
    if st.button("データを再読み込み"):
        st.cache_data.clear()
        st.rerun()
