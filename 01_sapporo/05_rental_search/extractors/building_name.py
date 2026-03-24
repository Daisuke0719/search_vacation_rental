"""住所文字列から建物名・部屋番号を抽出するモジュール"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from config import EXCEL_PATH, BUILDINGS_CSV


@dataclass
class BuildingInfo:
    """建物情報"""
    building_name: str
    address_base: str  # 番地まで（部屋番号なし）
    ward: str  # 区名
    room_number: Optional[str] = None
    full_address: str = ""
    registration_number: str = ""
    registration_date: Optional[str] = None
    fire_violation: Optional[str] = None


@dataclass
class BuildingGroup:
    """重複排除後の建物グループ"""
    building_name: str
    address_base: str
    ward: str
    unit_count: int = 0
    registrations: list[BuildingInfo] = field(default_factory=list)


def normalize_fullwidth(text: str) -> str:
    """全角英数字・記号を半角に正規化（カタカナはそのまま）"""
    # NFKC: 全角英数字→半角、半角カナ→全角カナ、互換文字の統一
    text = unicodedata.normalize("NFKC", text)
    # 連続スペースを1つに
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_ward(address: str) -> Optional[str]:
    """住所から区名を抽出"""
    m = re.search(r"札幌市(.+?区)", address)
    return m.group(1) if m else None


def extract_building_info(address: str) -> Optional[BuildingInfo]:
    """
    住所文字列から建物情報を抽出する。

    パターン例:
      北海道札幌市厚別区厚別中央4条6丁目1-10 朝日プラザ新札幌 102
      北海道札幌市中央区大通東8丁目1番地123 ハミルトンイースト 302
      北海道札幌市中央区南8条西3丁目7番地 茶やビル 1階
      北海道札幌市清田区真栄270番地
      北海道札幌市中央区南2条西13丁目319 2F
    """
    if not address or not isinstance(address, str):
        return None

    # 全角英数字を半角に正規化してから処理
    address = normalize_fullwidth(address)

    ward = extract_ward(address)
    if not ward:
        return None

    # 住所部分と残り（建物名＋部屋番号）を分離
    # パターン1: 丁目+番地番号 の後にスペース
    # パターン2: 番地+番号 の後にスペース
    # パターン3: 丁目+番号のみ（建物名なし）
    patterns = [
        # 丁目{数字-数字} {remainder}
        r"^(北海道札幌市.+?丁目[\d][\d\-]*)\s+(.+)$",
        # 番地{数字} {remainder}
        r"^(北海道札幌市.+?番地[\d][\d\-]*)\s+(.+)$",
        # 番地 {remainder} (番地の後に数字なし)
        r"^(北海道札幌市.+?番地)\s+(.+)$",
    ]

    address_base = None
    remainder = None

    for pat in patterns:
        m = re.match(pat, address)
        if m:
            address_base = m.group(1).strip()
            remainder = m.group(2).strip()
            break

    if not address_base:
        # 建物名なし（戸建てまたは住所のみ）
        return None

    # remainder から建物名と部屋番号を分離
    building_name, room_number = _split_building_and_room(remainder)

    if not building_name:
        return None

    # 建物名が実質フロア指定のみの場合は戸建て扱い
    # "1F", "2F", "1階", "2階", "2階3階" 等
    if re.match(r"^\d+[FfBb]$", building_name):
        return None
    if re.match(r"^(\d+階)+$", building_name):
        return None

    return BuildingInfo(
        building_name=building_name,
        address_base=address_base,
        ward=ward,
        room_number=room_number,
        full_address=address,
    )


def _split_building_and_room(remainder: str) -> tuple[str, Optional[str]]:
    """
    建物名＋部屋番号の文字列を分離する。

    例:
      "朝日プラザ新札幌 102"       -> ("朝日プラザ新札幌", "102")
      "AMSタワー 1406"             -> ("AMSタワー", "1406")
      "ドエル札幌南6条Ⅱ 507号室"  -> ("ドエル札幌南6条Ⅱ", "507号室")
      "茶やビル 1階"               -> ("茶やビル", "1階")
      "マーシャル14 4F"            -> ("マーシャル14", "4F")
      "ティアラローズB 棟 10号室"  -> ("ティアラローズB棟", "10号室")
      "1F"                         -> ("1F", None) -- 戸建て
      "THE LODGE IN SAPPORO（札幌の山小屋）" -> ("THE LODGE IN SAPPORO", None)
      "cietaruga（チェタルーガ）201" -> ("cietaruga（チェタルーガ）", "201")
      "ラフィーネ月寒 南側1F2F"    -> ("ラフィーネ月寒", "南側1F2F")
    """
    if not remainder:
        return ("", None)

    # 特殊ケース: "棟" が分離されている場合を修正
    # "ティアラローズB 棟 10号室" -> "ティアラローズB棟 10号室"
    remainder = re.sub(r"(\S)\s+棟\s+", r"\1棟 ", remainder)

    # 最後のスペースで分割を試みる
    parts = remainder.rsplit(" ", 1)

    if len(parts) == 1:
        # スペースなし: 建物名のみ（部屋番号なし）
        # ただし括弧内の別名の後に数字がある場合
        # 例: "cietaruga（チェタルーガ）201"
        m = re.match(r"^(.+?[）\)])(\d+)$", remainder)
        if m:
            return (m.group(1), m.group(2))
        return (remainder, None)

    candidate_name = parts[0]
    candidate_room = parts[1]

    # 部屋番号パターンの判定
    room_patterns = [
        r"^\d{1,5}$",           # 102, 1406, 2031
        r"^\d{1,5}号室?$",      # 507号室, 22号室
        r"^\d+[A-Za-z]$",       # 3A
        r"^\d+[Ff]$",           # 4F, 3f
        r"^\d+[Bb]$",           # 2B
        r"^\d+階$",             # 1階
        r"^南側\d+[Ff]\d+[Ff]$",  # 南側1F2F
        r"^\d+\(\d+[Ff]\)$",   # 31(3F)
    ]

    for pat in room_patterns:
        if re.match(pat, candidate_room):
            return (candidate_name, candidate_room)

    # 部屋番号に見えない場合は、全体を建物名とする
    # ただし "2F" のような場合は確認（建物名+フロアの場合）
    return (remainder, None)


def _clean_building_name_for_search(name: str) -> str:
    """検索用に建物名をクリーニング"""
    # 括弧内の別名を除去
    name = re.sub(r"[（(].+?[）)]", "", name)
    # 全角英数字を半角に
    name = normalize_fullwidth(name)
    # 前後の空白を除去
    name = name.strip()
    return name


def load_and_extract(excel_path: Optional[str] = None) -> list[BuildingGroup]:
    """
    Excelファイルを読み込み、建物名を抽出して重複排除する。

    Returns:
        BuildingGroup のリスト（ユニーク建物ごと）
    """
    path = excel_path or str(EXCEL_PATH)
    df = pd.read_excel(path, engine="openpyxl")

    addr_col = df.columns[0]   # 届出住宅の住所
    reg_col = df.columns[1]    # 届出番号
    date_col = df.columns[2]   # 届出日
    fire_col = df.columns[3]   # 消防法令違反

    groups: dict[tuple[str, str], BuildingGroup] = {}
    skipped = 0

    for _, row in df.iterrows():
        address = str(row[addr_col])
        info = extract_building_info(address)

        if info is None:
            skipped += 1
            continue

        info.full_address = address
        info.registration_number = str(row[reg_col]) if pd.notna(row[reg_col]) else ""
        info.registration_date = (
            row[date_col].strftime("%Y-%m-%d")
            if pd.notna(row[date_col]) else None
        )
        info.fire_violation = str(row[fire_col]) if pd.notna(row[fire_col]) else None

        key = (info.building_name, info.address_base)
        if key not in groups:
            groups[key] = BuildingGroup(
                building_name=info.building_name,
                address_base=info.address_base,
                ward=info.ward,
            )
        groups[key].registrations.append(info)
        groups[key].unit_count = len(groups[key].registrations)

    return list(groups.values())


def export_buildings_csv(groups: list[BuildingGroup], csv_path: Optional[str] = None) -> str:
    """
    BuildingGroupリストをCSVに出力する。

    出力列: building_name, address_base, ward, unit_count
    Returns: 出力先ファイルパス
    """
    path = csv_path or str(BUILDINGS_CSV)
    rows = []
    for g in groups:
        rows.append({
            "building_name": g.building_name,
            "address_base": g.address_base,
            "ward": g.ward,
            "unit_count": g.unit_count,
        })
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def load_buildings_csv(csv_path: Optional[str] = None) -> list[BuildingGroup]:
    """
    CSVからBuildingGroupリストを読み込む。

    Returns:
        BuildingGroup のリスト
    """
    path = csv_path or str(BUILDINGS_CSV)
    df = pd.read_csv(path, encoding="utf-8-sig")

    groups = []
    for _, row in df.iterrows():
        groups.append(BuildingGroup(
            building_name=str(row["building_name"]),
            address_base=str(row["address_base"]),
            ward=str(row["ward"]),
            unit_count=int(row["unit_count"]),
        ))
    return groups


def get_search_name(building_name: str) -> str:
    """検索用の建物名を返す"""
    return _clean_building_name_for_search(building_name)


if __name__ == "__main__":
    # テスト実行
    import sys
    sys.path.insert(0, str(__file__).rsplit("extractors", 1)[0])

    groups = load_and_extract()
    print(f"Total unique buildings: {len(groups)}")
    print(f"\nTop 20 by unit count:")
    for g in sorted(groups, key=lambda x: -x.unit_count)[:20]:
        print(f"  {g.unit_count:3d} units: {g.building_name} ({g.ward})")

    # Ward distribution
    ward_counts: dict[str, int] = {}
    for g in groups:
        ward_counts[g.ward] = ward_counts.get(g.ward, 0) + 1
    print(f"\nBuildings by ward:")
    for ward, cnt in sorted(ward_counts.items(), key=lambda x: -x[1]):
        print(f"  {ward}: {cnt}")

    # Test search name cleaning
    test_names = [
        "THE LODGE IN SAPPORO（札幌の山小屋）",
        "Ｃｕｌｔｕｒｅ２４",
        "ドエル札幌南6条Ⅱ",
    ]
    print(f"\nSearch name cleaning:")
    for name in test_names:
        print(f"  {name} -> {get_search_name(name)}")
