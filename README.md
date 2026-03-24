# 札幌 民泊事業プロジェクト

札幌市での民泊事業の立ち上げに向けた調査・分析・ツール群をまとめたリポジトリです。

## ディレクトリ構成

```
民泊/
├── README.md
├── CLAUDE.md
├── requirements.txt
├── venv/                          # Python仮想環境
└── 01_sapporo/
    ├── 00_ref/                    # 参考資料
    │   ├── url.md
    │   └── 札幌市内の民泊施設一覧.xlsx
    ├── 01_法規制調査.md            # 民泊新法・条例・消防法令等
    ├── 02_既存施設分析.md          # 札幌市内の民泊施設データ分析
    ├── 03_市場競合調査.md          # エリア別需要・競合分析
    ├── 04_収支シミュレーション.md   # 収支計画・損益分岐点
    ├── タスク計画_札幌民泊事業.md   # 全体タスク計画（Phase 1〜6）
    └── 05_rental_search/          # 賃貸掲載自動リサーチツール
        ├── main.py                # メインバッチ実行
        ├── config.py              # 設定（検索対象サイト、レート制限等）
        ├── dashboard.py           # Streamlit ダッシュボード
        ├── scheduler.py           # Windowsタスクスケジューラ登録
        ├── scrapers/              # サイト別スクレイパー
        ├── extractors/            # 建物名抽出ロジック
        ├── models/                # DB操作（SQLite）
        ├── notifications/         # LINE通知
        ├── exporters/             # Excel出力
        ├── db/                    # SQLiteデータベース
        ├── output/                # 出力ファイル（CSV等）
        └── logs/                  # 実行ログ
```

## セットアップ

```bash
# 仮想環境の作成（初回のみ）
python -m venv venv

# 仮想環境の有効化
source venv/Scripts/activate

# 依存パッケージのインストール
pip install -r requirements.txt

# Playwrightブラウザのインストール（初回のみ）
playwright install
```

## 賃貸掲載リサーチツール

民泊施設一覧（Excel）に記載された建物名をもとに、SUUMO・HOME'S等の賃貸サイトで掲載状況を自動検索するツールです。新着物件はLINEで通知できます。

### 環境変数

`01_sapporo/05_rental_search/.env` に以下を設定:

```
LINE_CHANNEL_TOKEN=your_token_here
LINE_USER_ID=your_user_id_here
```

### 実行方法

```bash
source venv/Scripts/activate

# 全サイト・全建物で実行
python 01_sapporo/05_rental_search/main.py

# SUUMOのみ
python 01_sapporo/05_rental_search/main.py --sites suumo

# 中央区のみ
python 01_sapporo/05_rental_search/main.py --ward 中央区

# テスト（最初の10棟のみ）
python 01_sapporo/05_rental_search/main.py --limit 10

# 前回の中断から再開
python 01_sapporo/05_rental_search/main.py --resume

# LINE通知なし
python 01_sapporo/05_rental_search/main.py --no-notify

# Excel出力なし
python 01_sapporo/05_rental_search/main.py --no-export

# 建物リストCSVをExcelから再生成
python 01_sapporo/05_rental_search/main.py --rebuild-csv
```

### ダッシュボード

```bash
source venv/Scripts/activate
streamlit run 01_sapporo/05_rental_search/dashboard.py
```

### 定期実行（タスクスケジューラ）

```bash
source venv/Scripts/activate

# 毎日午前2時に自動実行するタスクを登録
python 01_sapporo/05_rental_search/scheduler.py install

# タスクの状態確認
python 01_sapporo/05_rental_search/scheduler.py status

# タスクの削除
python 01_sapporo/05_rental_search/scheduler.py uninstall
```

## 調査ドキュメント

| ファイル | 内容 |
|---------|------|
| 01_法規制調査.md | 住宅宿泊事業法、札幌市条例、消防法令、用途地域 |
| 02_既存施設分析.md | 札幌市内の民泊施設データの分析結果 |
| 03_市場競合調査.md | エリア別の需要・供給・競合状況 |
| 04_収支シミュレーション.md | 初期投資、ランニングコスト、損益分岐点 |
| タスク計画_札幌民泊事業.md | Phase 1〜6 の全体タスク計画 |
