"""賃貸物件マップ生成ツール

Excelの掲載一覧データをインタラクティブな地図HTMLとして出力する。
国土地理院APIで住所→座標変換し、Leaflet.jsベースのマップを生成。

Usage:
    python generate_map.py
    python generate_map.py --excel path/to/file.xlsx
"""

import argparse
import json
import math
import re
import time
import webbrowser
from pathlib import Path

import pandas as pd
import requests

from config import OUTPUT_DIR

# ジオコーディングキャッシュ
GEOCODE_CACHE_PATH = OUTPUT_DIR / "geocode_cache.json"
# 出力HTMLパス
MAP_OUTPUT_PATH = OUTPUT_DIR / "rental_map.html"

# 国土地理院 ジオコーディングAPI
GSI_GEOCODE_URL = "https://msearch.gsi.go.jp/address-search/AddressSearch"


def load_listings(excel_path: str, sheet_name: str = "掲載一覧") -> pd.DataFrame:
    """Excelから掲載一覧を読み込む"""
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    print(f"  {len(df)}件の物件を読み込みました")
    return df


def geocode_address(address: str, session: requests.Session) -> tuple | None:
    """国土地理院APIで住所を座標に変換する。失敗時は住所を段階的に短縮して再試行。"""
    # 試行する住所パターン（段階的に短縮）
    candidates = [address]

    # 番地以降を除去したパターン
    shortened = re.sub(r'[\d０-９]+[-ー−][\d０-９]+.*$', '', address)
    if shortened != address:
        candidates.append(shortened)

    # 号を除去したパターン
    shortened2 = re.sub(r'[\d０-９]+号.*$', '', address)
    if shortened2 != address and shortened2 not in candidates:
        candidates.append(shortened2)

    # 丁目までのパターン
    shortened3 = re.sub(r'(丁目|条[東西南北]?).*$', r'\1', address)
    if shortened3 != address and shortened3 not in candidates:
        candidates.append(shortened3)

    for candidate in candidates:
        try:
            resp = session.get(GSI_GEOCODE_URL, params={"q": candidate}, timeout=10)
            resp.raise_for_status()
            results = resp.json()
            if results:
                lng, lat = results[0]["geometry"]["coordinates"]
                return (lat, lng)
        except Exception:
            continue

    return None


def geocode_all(df: pd.DataFrame) -> dict:
    """全ユニーク住所をジオコーディング（キャッシュ活用）"""
    # キャッシュ読み込み
    cache = {}
    if GEOCODE_CACHE_PATH.exists():
        with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
            cache = json.load(f)

    unique_addresses = df["住所"].dropna().unique()
    new_count = 0
    failed = []

    session = requests.Session()

    for addr in unique_addresses:
        if addr in cache:
            continue

        time.sleep(0.5)  # レート制限
        coords = geocode_address(addr, session)
        if coords:
            cache[addr] = list(coords)
            new_count += 1
            print(f"    OK {addr[:30]}... -> ({coords[0]:.6f}, {coords[1]:.6f})")
        else:
            failed.append(addr)
            print(f"    NG {addr[:30]}... -> geocoding failed")

    # キャッシュ保存
    if new_count > 0:
        with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"  {new_count}件を新規ジオコーディング、キャッシュ保存完了")

    if failed:
        print(f"  {len(failed)}件のジオコーディングに失敗")

    return cache


def get_floor_plan_category(madori: str) -> str:
    """間取り文字列をカテゴリに分類"""
    if not madori or pd.isna(madori):
        return "other"
    madori = str(madori).upper()
    if any(k in madori for k in ["ワンルーム", "1R", "1K", "1DK"]):
        return "1R-1DK"
    if "1LDK" in madori:
        return "1LDK"
    if any(k in madori for k in ["2LDK", "2DK", "2K"]):
        return "2K-2LDK"
    if any(k in madori for k in ["3LDK", "3DK", "4LDK", "4DK", "5LDK"]):
        return "3LDK+"
    return "other"


CATEGORY_COLORS = {
    "1R-1DK": "#3498db",
    "1LDK": "#2ecc71",
    "2K-2LDK": "#e67e22",
    "3LDK+": "#e74c3c",
    "other": "#95a5a6",
}

CATEGORY_LABELS = {
    "1R-1DK": "ワンルーム/1K/1DK",
    "1LDK": "1LDK",
    "2K-2LDK": "2K/2DK/2LDK",
    "3LDK+": "3LDK以上",
    "other": "その他",
}


def format_yen(value) -> str:
    """数値を円表示にフォーマット"""
    if pd.isna(value):
        return "−"
    try:
        return f"¥{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


def load_evaluation_scores() -> dict:
    """評価エンジンの結果を読み込み、listing_url → スコアdictのマッピングを返す。"""
    try:
        import subprocess, sys as _sys
        eval_script = str(Path(__file__).resolve().parent.parent / "07_property_evaluation" / "evaluate.py")
        eval_output = Path(__file__).resolve().parent.parent / "07_property_evaluation" / "output"

        # evaluate.pyを別プロセスで実行（config衝突回避）
        result = subprocess.run(
            [_sys.executable, eval_script],
            capture_output=True, text=True, timeout=120,
            cwd=str(Path(eval_script).parent),
        )

        # 最新のExcelから結果を読む
        eval_excels = sorted(eval_output.glob("evaluation_*.xlsx"))
        if not eval_excels:
            print(f"  評価Excelが見つかりません")
            return {}

        # Excel列名: 総合ランキングシート
        results_df = pd.read_excel(eval_excels[-1], sheet_name="総合ランキング")
        scores = {}
        for _, row in results_df.iterrows():
            url = row.get("掲載URL", "")
            if url and not pd.isna(url):
                scores[str(url)] = {
                    "total_score": round(float(row.get("総合スコア", 0)), 1),
                    "score_profitability": round(float(row.get("収益性", 0)), 1),
                    "score_location": round(float(row.get("立地", 0)), 1),
                    "score_demand": round(float(row.get("需要安定", 0)), 1),
                    "score_quality": round(float(row.get("物件適合", 0)), 1),
                    "score_risk": round(float(row.get("リスク", 0)), 1),
                    "annual_profit": int(row.get("年間利益", 0)),
                    "annual_roi": round(float(row.get("ROI(%)", 0)), 1),
                    "weighted_avg_adr": int(row.get("平均ADR", 0)),
                }
        print(f"  評価スコア読み込み: {len(scores)}件")
        return scores
    except Exception as e:
        print(f"  評価スコア読み込み失敗（スキップ）: {e}")
        return {}


def _score_to_color(score: float) -> str:
    """総合スコアに応じたマーカーカラーを返す。"""
    if score >= 68:
        return "#10b981"  # emerald - 優良
    if score >= 65:
        return "#3b82f6"  # blue - 良好
    if score >= 60:
        return "#f59e0b"  # amber - 普通
    if score >= 50:
        return "#f97316"  # orange - 注意
    return "#ef4444"      # red - 要検討


def _score_to_rank(score: float) -> str:
    """総合スコアからランクラベルを返す。"""
    if score >= 68:
        return "A"
    if score >= 65:
        return "B"
    if score >= 60:
        return "C"
    if score >= 50:
        return "D"
    return "E"


def build_map_data(df: pd.DataFrame, coords: dict, scores: dict | None = None) -> tuple:
    """物件データと座標をマージし、マップ用データを構築"""
    if scores is None:
        scores = {}
    properties = []
    unmapped = []

    # 同一住所のオフセット用カウンター
    addr_count = {}

    for _, row in df.iterrows():
        addr = row.get("住所", "")
        if pd.isna(addr) or addr not in coords:
            unmapped.append({
                "building": str(row.get("建物名", "")),
                "address": str(addr) if not pd.isna(addr) else "",
                "rent": format_yen(row.get("家賃（円）")),
                "layout": str(row.get("間取り", "")),
                "url": str(row.get("URL", "")),
            })
            continue

        lat, lng = coords[addr]

        # 同一住所の物件を微小オフセット
        addr_count[addr] = addr_count.get(addr, 0) + 1
        count = addr_count[addr]
        if count > 1:
            angle = (count - 1) * 2.399  # 黄金角でスパイラル配置
            r = 0.00008 * math.sqrt(count - 1)
            lat += r * math.cos(angle)
            lng += r * math.sin(angle)

        category = get_floor_plan_category(row.get("間取り"))
        rent = row.get("家賃（円）")
        rent_num = int(rent) if not pd.isna(rent) else 0

        # 評価スコアを統合
        url = str(row.get("URL", ""))
        eval_data = scores.get(url, {})
        total_score = eval_data.get("total_score", 0)
        has_score = total_score > 0

        # スコアがあればスコアベースの色、なければ間取りベースの色
        if has_score:
            marker_color = _score_to_color(total_score)
        else:
            marker_color = CATEGORY_COLORS[category]

        prop = {
            "lat": round(lat, 7),
            "lng": round(lng, 7),
            "building": str(row.get("建物名", "")),
            "title": str(row.get("物件タイトル", "")) if not pd.isna(row.get("物件タイトル")) else "",
            "address": str(addr),
            "ward": str(row.get("区", "")),
            "rent": rent_num,
            "rent_fmt": format_yen(rent),
            "mgmt_fee": format_yen(row.get("管理費（円）")),
            "deposit": str(row.get("敷金", "−")) if not pd.isna(row.get("敷金")) else "−",
            "key_money": str(row.get("礼金", "−")) if not pd.isna(row.get("礼金")) else "−",
            "layout": str(row.get("間取り", "")) if not pd.isna(row.get("間取り")) else "",
            "area": f'{row.get("面積（㎡）", 0):.1f}㎡' if not pd.isna(row.get("面積（㎡）")) else "",
            "floor": str(row.get("階数", "")) if not pd.isna(row.get("階数")) else "",
            "age": str(row.get("築年数", "")) if not pd.isna(row.get("築年数")) else "",
            "station": str(row.get("最寄駅", "")) if not pd.isna(row.get("最寄駅")) else "",
            "walk_min": str(row.get("徒歩（分）", "")) if not pd.isna(row.get("徒歩（分）")) else "",
            "site": str(row.get("サイト", "")) if not pd.isna(row.get("サイト")) else "",
            "url": url,
            "category": category,
            "color": marker_color,
            # 評価データ
            "score": total_score,
            "rank": _score_to_rank(total_score) if has_score else "",
            "s_profit": eval_data.get("score_profitability", 0),
            "s_loc": eval_data.get("score_location", 0),
            "s_demand": eval_data.get("score_demand", 0),
            "s_quality": eval_data.get("score_quality", 0),
            "s_risk": eval_data.get("score_risk", 0),
            "profit": eval_data.get("annual_profit", 0),
            "roi": eval_data.get("annual_roi", 0),
            "adr": eval_data.get("weighted_avg_adr", 0),
        }
        properties.append(prop)

    return properties, unmapped


def render_html(properties: list, unmapped: list, output_path: Path):
    """Leaflet.jsベースのインタラクティブマップHTMLを生成（評価スコア統合版）"""
    props_json = json.dumps(properties, ensure_ascii=False)
    unmapped_json = json.dumps(unmapped, ensure_ascii=False)
    category_colors_json = json.dumps(CATEGORY_COLORS, ensure_ascii=False)
    category_labels_json = json.dumps(CATEGORY_LABELS, ensure_ascii=False)

    # 評価スコアの有無
    has_scores = any(p.get("score", 0) > 0 for p in properties)

    # 家賃の範囲を算出
    rents = [p["rent"] for p in properties if p["rent"] > 0]
    rent_min = min(rents) if rents else 0
    rent_max = max(rents) if rents else 500000
    # スライダーのステップを1万円単位に
    slider_min = (rent_min // 10000) * 10000
    slider_max = ((rent_max // 10000) + 1) * 10000

    # スコアレンジ凡例データ
    score_ranges_json = json.dumps([
        {"min": 68, "label": "A 優良", "color": "#10b981"},
        {"min": 65, "label": "B 良好", "color": "#3b82f6"},
        {"min": 60, "label": "C 普通", "color": "#f59e0b"},
        {"min": 50, "label": "D 注意", "color": "#f97316"},
        {"min": 0,  "label": "E 要検討", "color": "#ef4444"},
    ], ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>民泊物件評価マップ - 札幌</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif; }}

#map {{ position: absolute; top: 0; left: 0; right: 0; bottom: 0; z-index: 1; }}

/* サイドバー */
#sidebar {{
  position: absolute; top: 0; left: 0; bottom: 0; width: 360px;
  background: #fff; z-index: 1000; overflow: hidden;
  display: flex; flex-direction: column;
  box-shadow: 2px 0 12px rgba(0,0,0,0.15);
  transition: transform 0.3s ease;
}}
#sidebar.collapsed {{ transform: translateX(-360px); }}

#sidebar-toggle {{
  position: absolute; top: 12px; z-index: 1001;
  left: 360px; transition: left 0.3s ease;
  background: #fff; border: none; border-radius: 0 8px 8px 0;
  padding: 10px 8px; cursor: pointer;
  box-shadow: 2px 2px 6px rgba(0,0,0,0.2);
  font-size: 18px; line-height: 1;
}}
#sidebar.collapsed ~ #sidebar-toggle {{ left: 0; }}

#sidebar-header {{
  padding: 16px; background: #2c3e50; color: #fff;
  flex-shrink: 0;
}}
#sidebar-header h1 {{ font-size: 16px; font-weight: 600; margin-bottom: 4px; }}
#count-badge {{
  font-size: 13px; opacity: 0.85;
}}

#property-list {{
  flex: 1; overflow-y: auto; padding: 0;
}}
.property-item {{
  padding: 12px 16px; border-bottom: 1px solid #eee;
  cursor: pointer; transition: background 0.15s;
}}
.property-item:hover {{ background: #f0f7ff; }}
.property-item.active {{ background: #e3f2fd; border-left: 3px solid #2196F3; }}
.prop-name {{
  font-size: 14px; font-weight: 600; color: #333;
  display: flex; align-items: center; gap: 6px;
}}
.prop-name .dot {{
  display: inline-block; width: 10px; height: 10px;
  border-radius: 50%; flex-shrink: 0;
}}
.prop-meta {{
  font-size: 12px; color: #666; margin-top: 3px;
  display: flex; gap: 12px; flex-wrap: wrap;
}}
.prop-meta span {{ white-space: nowrap; }}

/* フィルタパネル */
#filter-panel {{
  position: absolute; top: 12px; right: 12px; z-index: 1000;
  background: #fff; border-radius: 10px; padding: 16px;
  box-shadow: 0 2px 12px rgba(0,0,0,0.15);
  min-width: 240px; max-width: 300px;
  font-size: 13px;
}}
#filter-panel h3 {{
  font-size: 14px; margin-bottom: 12px; color: #2c3e50;
  display: flex; justify-content: space-between; align-items: center;
}}
#filter-toggle {{
  background: none; border: none; cursor: pointer;
  font-size: 16px; color: #666;
}}
#filter-body {{ transition: max-height 0.3s ease; overflow: hidden; }}
#filter-body.collapsed {{ max-height: 0 !important; }}

.filter-section {{ margin-bottom: 14px; }}
.filter-section label {{
  display: flex; align-items: center; gap: 6px;
  padding: 3px 0; cursor: pointer; color: #444;
}}
.filter-section input[type="checkbox"] {{ accent-color: #2196F3; }}
.filter-label {{ font-size: 12px; font-weight: 600; color: #888; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }}

.rent-slider-wrap {{ display: flex; flex-direction: column; gap: 6px; }}
.rent-slider-wrap input[type="range"] {{ width: 100%; accent-color: #2196F3; }}
.rent-values {{ display: flex; justify-content: space-between; font-size: 12px; color: #666; }}

#reset-btn {{
  width: 100%; padding: 7px; background: #f5f5f5; border: 1px solid #ddd;
  border-radius: 6px; cursor: pointer; font-size: 12px; color: #555;
  transition: background 0.15s;
}}
#reset-btn:hover {{ background: #e8e8e8; }}

/* 凡例 */
#legend {{
  position: absolute; bottom: 24px; right: 12px; z-index: 1000;
  background: rgba(255,255,255,0.95); border-radius: 8px; padding: 12px 14px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.12);
  font-size: 12px;
}}
#legend h4 {{ font-size: 12px; margin-bottom: 6px; color: #555; }}
.legend-item {{
  display: flex; align-items: center; gap: 6px; padding: 2px 0;
}}
.legend-dot {{
  width: 12px; height: 12px; border-radius: 50%;
  border: 2px solid rgba(255,255,255,0.8);
  box-shadow: 0 0 3px rgba(0,0,0,0.3);
}}

/* ポップアップ */
.leaflet-popup-content-wrapper {{
  border-radius: 10px !important;
  box-shadow: 0 4px 16px rgba(0,0,0,0.2) !important;
}}
.leaflet-popup-content {{
  margin: 14px 16px !important;
  font-size: 13px; line-height: 1.6;
  min-width: 240px;
}}
.popup-title {{
  font-size: 15px; font-weight: 700; color: #2c3e50;
  margin-bottom: 8px; border-bottom: 2px solid #eee; padding-bottom: 6px;
}}
.popup-row {{
  display: flex; gap: 6px; padding: 2px 0;
}}
.popup-row .label {{ color: #888; min-width: 60px; flex-shrink: 0; }}
.popup-row .value {{ color: #333; font-weight: 500; }}
.popup-rent {{
  font-size: 18px; font-weight: 700; color: #e74c3c;
  margin: 6px 0;
}}
.popup-rent .mgmt {{ font-size: 12px; color: #999; font-weight: 400; }}
.popup-link {{
  display: block; margin-top: 10px; padding: 8px 0;
  background: #4CAF50; color: #fff !important; text-align: center;
  text-decoration: none; border-radius: 6px; font-size: 13px; font-weight: 600;
  transition: background 0.15s;
}}
.popup-link:hover {{ background: #43A047; }}

/* 未マッピングテーブル */
#unmapped-panel {{
  display: none; position: absolute; bottom: 0; left: 0; right: 0;
  z-index: 1000; background: #fff; max-height: 200px;
  overflow-y: auto; box-shadow: 0 -2px 12px rgba(0,0,0,0.15);
  padding: 12px 20px; font-size: 12px;
}}
#unmapped-panel h4 {{ margin-bottom: 8px; color: #e74c3c; }}
#unmapped-panel table {{ width: 100%; border-collapse: collapse; }}
#unmapped-panel th, #unmapped-panel td {{
  padding: 4px 8px; text-align: left; border-bottom: 1px solid #eee;
}}

/* 評価スコア: ポップアップ */
.popup-eval {{
  margin: 8px 0; padding: 8px; background: #f8fafc;
  border-radius: 8px; border: 1px solid #e2e8f0;
}}
.popup-eval-header {{
  display: flex; align-items: flex-start; gap: 10px; margin-bottom: 4px;
}}
.popup-score-badge {{
  display: flex; align-items: center; gap: 6px;
  border: 2px solid; border-radius: 8px; padding: 4px 8px; flex-shrink: 0;
}}
.popup-score-badge .rank {{
  color: #fff; font-weight: 800; font-size: 16px;
  padding: 2px 7px; border-radius: 4px; line-height: 1;
}}
.popup-score-badge .pts {{
  font-size: 18px; font-weight: 700; color: #1e293b;
}}
.popup-score-badge .pts small {{ font-size: 11px; color: #94a3b8; font-weight: 400; }}
.popup-eval-kpi {{
  font-size: 11px; display: flex; flex-direction: column; gap: 2px; flex: 1;
}}
.popup-eval-kpi .kpi-label {{ color: #94a3b8; margin-right: 4px; }}
.popup-eval-kpi .kpi-value {{ font-weight: 600; color: #334155; }}
.popup-eval-kpi .kpi-value.positive {{ color: #10b981; }}
.popup-eval-kpi .kpi-value.negative {{ color: #ef4444; }}

/* 評価スコア: サイドバー */
.list-rank {{
  display: inline-block; color: #fff; font-weight: 700; font-size: 10px;
  padding: 1px 5px; border-radius: 3px; margin-right: 4px; vertical-align: middle;
}}
.list-score {{
  font-weight: 600; color: #3b82f6; font-size: 11px;
}}
.list-profit {{
  font-size: 11px; font-weight: 600;
}}
.list-profit.positive {{ color: #10b981; }}
.list-profit.negative {{ color: #ef4444; }}

/* ソートセレクト */
#sort-select {{
  width: 100%; padding: 5px 8px; border: 1px solid #ddd;
  border-radius: 6px; font-size: 12px; color: #333;
  background: #fff; cursor: pointer;
}}

/* レスポンシブ */
@media (max-width: 768px) {{
  #sidebar {{ width: 100%; }}
  #sidebar.collapsed {{ transform: translateX(-100%); }}
  #sidebar-toggle {{ left: 100%; }}
  #sidebar.collapsed ~ #sidebar-toggle {{ left: 0; }}
  #filter-panel {{ top: 8px; right: 8px; min-width: 200px; max-width: 240px; padding: 12px; }}
  #legend {{ bottom: 16px; right: 8px; }}
}}
</style>
</head>
<body>

<div id="map"></div>

<!-- サイドバー -->
<div id="sidebar">
  <div id="sidebar-header">
    <h1>民泊物件評価マップ</h1>
    <div id="count-badge">表示中: <span id="visible-count">0</span> / <span id="total-count">0</span>件</div>
  </div>
  <div id="property-list"></div>
</div>
<button id="sidebar-toggle" title="物件リスト切替">☰</button>

<!-- フィルタパネル -->
<div id="filter-panel">
  <h3>フィルタ <button id="filter-toggle" title="折りたたみ">▼</button></h3>
  <div id="filter-body">
    <div class="filter-section">
      <div class="filter-label">間取り</div>
      <div id="layout-filters"></div>
    </div>
    <div class="filter-section">
      <div class="filter-label">家賃</div>
      <div class="rent-slider-wrap">
        <input type="range" id="rent-min" min="{slider_min}" max="{slider_max}" value="{slider_min}" step="10000">
        <input type="range" id="rent-max" min="{slider_min}" max="{slider_max}" value="{slider_max}" step="10000">
        <div class="rent-values">
          <span id="rent-min-label">{format_yen(slider_min)}</span>
          <span>〜</span>
          <span id="rent-max-label">{format_yen(slider_max)}</span>
        </div>
      </div>
    </div>
    <div class="filter-section" id="score-filter-section" style="display:{'block' if has_scores else 'none'}">
      <div class="filter-label">評価ランク</div>
      <div id="rank-filters"></div>
    </div>
    <div class="filter-section" id="sort-section" style="display:{'block' if has_scores else 'none'}">
      <div class="filter-label">並び替え</div>
      <select id="sort-select">
        <option value="score-desc">スコア高い順</option>
        <option value="rent-asc">家賃安い順</option>
        <option value="rent-desc">家賃高い順</option>
        <option value="profit-desc">利益高い順</option>
        <option value="roi-desc">ROI高い順</option>
      </select>
    </div>
    <button id="reset-btn">すべて表示</button>
  </div>
</div>

<!-- 凡例 -->
<div id="legend">
  <h4>間取り</h4>
  <div id="legend-items"></div>
</div>

<!-- 未マッピング -->
<div id="unmapped-panel">
  <h4>⚠ 地図に表示できなかった物件</h4>
  <table id="unmapped-table"><thead><tr><th>建物名</th><th>住所</th><th>家賃</th><th>間取り</th><th>リンク</th></tr></thead><tbody></tbody></table>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
(function() {{
  const properties = {props_json};
  const unmapped = {unmapped_json};
  const categoryColors = {category_colors_json};
  const categoryLabels = {category_labels_json};
  const hasScores = {str(has_scores).lower()};
  const scoreRanges = {score_ranges_json};

  // マップ初期化
  const map = L.map('map').setView([43.055, 141.345], 13);
  L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 19
  }}).addTo(map);

  // マーカークラスター
  const clusterGroup = L.markerClusterGroup({{
    maxClusterRadius: 40,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    iconCreateFunction: function(cluster) {{
      const count = cluster.getChildCount();
      let size = count < 5 ? 'small' : count < 15 ? 'medium' : 'large';
      return L.divIcon({{
        html: '<div><span>' + count + '</span></div>',
        className: 'marker-cluster marker-cluster-' + size,
        iconSize: L.point(40, 40)
      }});
    }}
  }});

  // カスタムマーカーアイコン生成
  function createIcon(color) {{
    return L.divIcon({{
      className: 'custom-marker',
      html: `<div style="
        width:14px;height:14px;border-radius:50%;
        background:${{color}};
        border:2.5px solid #fff;
        box-shadow:0 1px 4px rgba(0,0,0,0.4);
      "></div>`,
      iconSize: [14, 14],
      iconAnchor: [7, 7],
      popupAnchor: [0, -10]
    }});
  }}

  // スコア用SVGレーダーチャート生成
  function radarSvg(p) {{
    if (!p.score) return '';
    const dims = [
      {{ label: '収益', val: p.s_profit, max: 30 }},
      {{ label: '立地', val: p.s_loc, max: 20 }},
      {{ label: '需要', val: p.s_demand, max: 15 }},
      {{ label: '品質', val: p.s_quality, max: 20 }},
      {{ label: 'リスク', val: p.s_risk, max: 15 }},
    ];
    const cx = 60, cy = 60, r = 42;
    const n = dims.length;
    function xy(i, ratio) {{
      const a = (Math.PI * 2 * i / n) - Math.PI / 2;
      return [cx + r * ratio * Math.cos(a), cy + r * ratio * Math.sin(a)];
    }}
    // 背景の同心五角形
    let bg = '';
    for (const lvl of [0.25, 0.5, 0.75, 1.0]) {{
      const pts = dims.map((_, i) => xy(i, lvl).join(',')).join(' ');
      bg += `<polygon points="${{pts}}" fill="none" stroke="#e2e8f0" stroke-width="0.5"/>`;
    }}
    // 軸線
    let axes = '';
    dims.forEach((_, i) => {{
      const [x, y] = xy(i, 1);
      axes += `<line x1="${{cx}}" y1="${{cy}}" x2="${{x}}" y2="${{y}}" stroke="#e2e8f0" stroke-width="0.5"/>`;
    }});
    // データポリゴン
    const dataPts = dims.map((d, i) => xy(i, d.val / d.max).join(',')).join(' ');
    // ラベル
    let labels = '';
    dims.forEach((d, i) => {{
      const [x, y] = xy(i, 1.28);
      const pct = Math.round(d.val / d.max * 100);
      labels += `<text x="${{x}}" y="${{y}}" text-anchor="middle" dominant-baseline="central" font-size="8" fill="#64748b">${{d.label}}</text>`;
    }});
    return `<svg width="120" height="120" viewBox="0 0 120 120" style="display:block;margin:4px auto">
      ${{bg}}${{axes}}
      <polygon points="${{dataPts}}" fill="rgba(59,130,246,0.2)" stroke="#3b82f6" stroke-width="1.5"/>
      ${{labels}}
    </svg>`;
  }}

  // スコアバッジ
  function scoreBadge(p) {{
    if (!p.score) return '';
    const colors = {{ A:'#10b981', B:'#3b82f6', C:'#f59e0b', D:'#f97316', E:'#ef4444' }};
    const c = colors[p.rank] || '#94a3b8';
    return `<div class="popup-score-badge" style="border-color:${{c}}">
      <span class="rank" style="background:${{c}}">${{p.rank}}</span>
      <span class="pts">${{p.score}}<small>/100</small></span>
    </div>`;
  }}

  // ポップアップHTML生成
  function popupHtml(p) {{
    let rows = '';
    if (p.layout) rows += `<div class="popup-row"><span class="label">間取り</span><span class="value">${{p.layout}}${{p.area ? ' / ' + p.area : ''}}</span></div>`;
    if (p.floor) rows += `<div class="popup-row"><span class="label">階数</span><span class="value">${{p.floor}}</span></div>`;
    if (p.age) rows += `<div class="popup-row"><span class="label">築年数</span><span class="value">${{p.age}}</span></div>`;
    rows += `<div class="popup-row"><span class="label">敷/礼</span><span class="value">${{p.deposit}} / ${{p.key_money}}</span></div>`;
    if (p.station) rows += `<div class="popup-row"><span class="label">最寄駅</span><span class="value">${{p.station}}${{p.walk_min ? '（徒歩' + p.walk_min + '分）' : ''}}</span></div>`;
    rows += `<div class="popup-row"><span class="label">住所</span><span class="value">${{p.address}}</span></div>`;
    if (p.site) rows += `<div class="popup-row"><span class="label">サイト</span><span class="value">${{p.site}}</span></div>`;

    // 評価セクション
    let evalSection = '';
    if (p.score) {{
      evalSection = `
        <div class="popup-eval">
          <div class="popup-eval-header">
            ${{scoreBadge(p)}}
            <div class="popup-eval-kpi">
              <div><span class="kpi-label">年間利益</span><span class="kpi-value ${{p.profit >= 0 ? 'positive' : 'negative'}}">${{p.profit >= 0 ? '+' : ''}}${{Number(p.profit).toLocaleString()}}円</span></div>
              <div><span class="kpi-label">ROI</span><span class="kpi-value">${{p.roi}}%</span></div>
              <div><span class="kpi-label">想定ADR</span><span class="kpi-value">¥${{Number(p.adr).toLocaleString()}}</span></div>
            </div>
          </div>
          ${{radarSvg(p)}}
        </div>`;
    }}

    return `
      <div class="popup-title">${{p.building}}</div>
      <div class="popup-rent">${{p.rent_fmt}} <span class="mgmt">+ 管理費 ${{p.mgmt_fee}}</span></div>
      ${{evalSection}}
      ${{rows}}
      ${{p.url ? `<a class="popup-link" href="${{p.url}}" target="_blank" rel="noopener">詳細を見る →</a>` : ''}}
    `;
  }}

  // マーカー作成
  const markers = [];
  const activeCategories = new Set(Object.keys(categoryColors));

  properties.forEach((p, i) => {{
    const marker = L.marker([p.lat, p.lng], {{
      icon: createIcon(p.color)
    }});
    marker.bindPopup(popupHtml(p), {{ maxWidth: 300, closeButton: true }});
    const ttScore = p.score ? ` [${{p.rank}}${{p.score}}pt]` : '';
    marker.bindTooltip(`${{p.building}} - ${{p.rent_fmt}} - ${{p.layout}}${{ttScore}}`, {{
      direction: 'top', offset: [0, -8], opacity: 0.92
    }});
    marker._propData = p;
    marker._propIndex = i;
    markers.push(marker);
    clusterGroup.addLayer(marker);
  }});

  map.addLayer(clusterGroup);

  // 全マーカーが収まるようにズーム
  if (markers.length > 0) {{
    const group = L.featureGroup(markers);
    map.fitBounds(group.getBounds().pad(0.1));
  }}

  // --- サイドバー ---
  const listEl = document.getElementById('property-list');
  const totalCountEl = document.getElementById('total-count');
  const visibleCountEl = document.getElementById('visible-count');
  totalCountEl.textContent = properties.length;

  function renderList(filtered) {{
    listEl.innerHTML = '';
    filtered.forEach((p, idx) => {{
      const item = document.createElement('div');
      item.className = 'property-item';
      const rankBadge = p.rank
        ? `<span class="list-rank" style="background:${{p.color}}">${{p.rank}}</span>`
        : `<span class="dot" style="background:${{p.color}}"></span>`;
      const scoreInfo = p.score
        ? `<span class="list-score">${{p.score}}pt</span><span class="list-profit ${{p.profit >= 0 ? 'positive' : 'negative'}}">${{p.profit >= 0 ? '+' : ''}}${{Math.round(p.profit/10000)}}万/年</span>`
        : '';
      item.innerHTML = `
        <div class="prop-name">${{rankBadge}}${{p.building}}</div>
        <div class="prop-meta">
          <span>${{p.rent_fmt}}</span>
          <span>${{p.layout}}</span>
          ${{p.area ? '<span>' + p.area + '</span>' : ''}}
          ${{scoreInfo}}
        </div>
      `;
      item.addEventListener('click', () => {{
        const m = markers[p._origIndex];
        if (m) {{
          map.setView([p.lat, p.lng], 17);
          clusterGroup.zoomToShowLayer(m, () => {{ m.openPopup(); }});
          document.querySelectorAll('.property-item.active').forEach(el => el.classList.remove('active'));
          item.classList.add('active');
        }}
      }});
      listEl.appendChild(item);
    }});
    visibleCountEl.textContent = filtered.length;
  }}

  // --- フィルタ ---
  const layoutFiltersEl = document.getElementById('layout-filters');
  Object.keys(categoryLabels).forEach(cat => {{
    const label = document.createElement('label');
    label.innerHTML = `<input type="checkbox" data-cat="${{cat}}" checked>
      <span class="dot" style="background:${{categoryColors[cat]}};width:10px;height:10px;border-radius:50%;display:inline-block"></span>
      ${{categoryLabels[cat]}}`;
    layoutFiltersEl.appendChild(label);
  }});

  const rentMinSlider = document.getElementById('rent-min');
  const rentMaxSlider = document.getElementById('rent-max');
  const rentMinLabel = document.getElementById('rent-min-label');
  const rentMaxLabel = document.getElementById('rent-max-label');

  function formatYen(v) {{ return '¥' + Number(v).toLocaleString(); }}

  function applyFilters() {{
    const checkedCats = new Set();
    document.querySelectorAll('#layout-filters input[type=checkbox]:checked').forEach(cb => {{
      checkedCats.add(cb.dataset.cat);
    }});

    // ランクフィルタ
    const checkedRanks = new Set();
    document.querySelectorAll('#rank-filters input[type=checkbox]:checked').forEach(cb => {{
      checkedRanks.add(cb.dataset.rank);
    }});

    let rMin = parseInt(rentMinSlider.value);
    let rMax = parseInt(rentMaxSlider.value);
    if (rMin > rMax) {{ [rMin, rMax] = [rMax, rMin]; }}
    rentMinLabel.textContent = formatYen(rMin);
    rentMaxLabel.textContent = formatYen(rMax);

    clusterGroup.clearLayers();
    const filtered = [];

    properties.forEach((p, i) => {{
      const catMatch = checkedCats.has(p.category);
      const rentMatch = (p.rent === 0) || (p.rent >= rMin && p.rent <= rMax);
      const rankMatch = !hasScores || checkedRanks.size === 0 || !p.rank || checkedRanks.has(p.rank);
      if (catMatch && rentMatch && rankMatch) {{
        clusterGroup.addLayer(markers[i]);
        filtered.push({{ ...p, _origIndex: i }});
      }}
    }});

    // ソート
    const sortVal = sortSelect ? sortSelect.value : 'score-desc';
    const sortFns = {{
      'score-desc': (a, b) => (b.score || 0) - (a.score || 0),
      'rent-asc':   (a, b) => a.rent - b.rent,
      'rent-desc':  (a, b) => b.rent - a.rent,
      'profit-desc': (a, b) => (b.profit || 0) - (a.profit || 0),
      'roi-desc':   (a, b) => (b.roi || 0) - (a.roi || 0),
    }};
    if (sortFns[sortVal]) filtered.sort(sortFns[sortVal]);

    renderList(filtered);
  }}

  // ランクフィルタ生成
  if (hasScores) {{
    const rankFiltersEl = document.getElementById('rank-filters');
    scoreRanges.forEach(sr => {{
      const label = document.createElement('label');
      label.innerHTML = `<input type="checkbox" data-rank="${{sr.label[0]}}" checked>
        <span class="dot" style="background:${{sr.color}};width:10px;height:10px;border-radius:50%;display:inline-block"></span>
        ${{sr.label}}`;
      rankFiltersEl.appendChild(label);
    }});
    document.querySelectorAll('#rank-filters input').forEach(cb => {{
      cb.addEventListener('change', applyFilters);
    }});
  }}

  document.querySelectorAll('#layout-filters input').forEach(cb => {{
    cb.addEventListener('change', applyFilters);
  }});
  rentMinSlider.addEventListener('input', applyFilters);
  rentMaxSlider.addEventListener('input', applyFilters);

  // ソート
  const sortSelect = document.getElementById('sort-select');
  if (sortSelect) {{
    sortSelect.addEventListener('change', applyFilters);
  }}

  document.getElementById('reset-btn').addEventListener('click', () => {{
    document.querySelectorAll('#layout-filters input').forEach(cb => {{ cb.checked = true; }});
    document.querySelectorAll('#rank-filters input').forEach(cb => {{ cb.checked = true; }});
    rentMinSlider.value = rentMinSlider.min;
    rentMaxSlider.value = rentMaxSlider.max;
    if (sortSelect) sortSelect.value = 'score-desc';
    applyFilters();
  }});

  // 初期表示
  applyFilters();

  // --- サイドバートグル ---
  const sidebar = document.getElementById('sidebar');
  document.getElementById('sidebar-toggle').addEventListener('click', () => {{
    sidebar.classList.toggle('collapsed');
    setTimeout(() => map.invalidateSize(), 350);
  }});

  // --- フィルタ折りたたみ ---
  const filterBody = document.getElementById('filter-body');
  const filterToggle = document.getElementById('filter-toggle');
  filterToggle.addEventListener('click', () => {{
    filterBody.classList.toggle('collapsed');
    filterToggle.textContent = filterBody.classList.contains('collapsed') ? '▶' : '▼';
  }});

  // --- 凡例 ---
  const legendItems = document.getElementById('legend-items');
  if (hasScores) {{
    scoreRanges.forEach(sr => {{
      const item = document.createElement('div');
      item.className = 'legend-item';
      item.innerHTML = `<span class="legend-dot" style="background:${{sr.color}}"></span>${{sr.label}}`;
      legendItems.appendChild(item);
    }});
  }} else {{
    Object.keys(categoryLabels).forEach(cat => {{
      const item = document.createElement('div');
      item.className = 'legend-item';
      item.innerHTML = `<span class="legend-dot" style="background:${{categoryColors[cat]}}"></span>${{categoryLabels[cat]}}`;
      legendItems.appendChild(item);
    }});
  }}

  // --- 未マッピング物件 ---
  if (unmapped.length > 0) {{
    const panel = document.getElementById('unmapped-panel');
    panel.style.display = 'block';
    const tbody = panel.querySelector('tbody');
    unmapped.forEach(u => {{
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${{u.building}}</td><td>${{u.address}}</td><td>${{u.rent}}</td><td>${{u.layout}}</td>
        <td>${{u.url ? '<a href="' + u.url + '" target="_blank">リンク</a>' : '−'}}</td>`;
      tbody.appendChild(tr);
    }});
  }}

  // モバイル: 初期状態でサイドバー閉じる
  if (window.innerWidth <= 768) {{
    sidebar.classList.add('collapsed');
  }}
}})();
</script>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  マップHTMLを生成: {output_path}")


def find_latest_excel() -> Path | None:
    """output/ディレクトリから最新のExcelファイルを検索"""
    excels = sorted(OUTPUT_DIR.glob("rental_search_results_*.xlsx"))
    return excels[-1] if excels else None


def main():
    parser = argparse.ArgumentParser(description="賃貸物件マップ生成")
    parser.add_argument("--excel", type=str, help="入力Excelファイルパス")
    args = parser.parse_args()

    print("=" * 50)
    print("  賃貸物件マップ生成ツール")
    print("=" * 50)

    # Excelファイル特定
    if args.excel:
        excel_path = Path(args.excel)
    else:
        excel_path = find_latest_excel()
    if not excel_path or not excel_path.exists():
        print("エラー: Excelファイルが見つかりません")
        return

    print(f"\n[1/4] Excelデータ読み込み: {excel_path.name}")
    df = load_listings(excel_path)

    print(f"\n[2/4] ジオコーディング（国土地理院API）")
    coords = geocode_all(df)

    print(f"\n[3/5] 評価スコア読み込み")
    scores = load_evaluation_scores()

    print(f"\n[4/5] マップデータ構築")
    properties, unmapped = build_map_data(df, coords, scores)
    print(f"  マップ表示: {len(properties)}件, 未マッピング: {len(unmapped)}件")

    print(f"\n[5/5] HTML生成")
    render_html(properties, unmapped, MAP_OUTPUT_PATH)

    print(f"\n完了! ブラウザで開きます...")
    webbrowser.open(str(MAP_OUTPUT_PATH))


if __name__ == "__main__":
    main()
