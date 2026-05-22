# Trading Backtesting

## プロジェクト概要
**runner-first research bundle** と高速バックテストを中心とした戦略ツール。研究定義は domain / runner に実装し、VectorBT基盤の高速ベクトル化バックテスト、必要時のみ Nautilus verification、結果確認は runner bundle / canonical docs / static HTML report に寄せる。**FastAPI サーバー (:3002) が唯一の API バックエンド**。フロントエンドは `apps/ts/packages/web/` に集約済み。

## 重要原則
- **表面的なごまかしを絶対に行わない。根本的な解決ができないときは、「今は○○の理由で解決できなかった」と素直に言う**
- **Simple is BEST !!!**
- 可能な限り型安全性を追求する。pydanticを積極的に使用する
- **pandasのSeriesを使うときは、必ず何の型のseriesかを明記する**（例：`pd.Series[float]`, `pd.Series[int]`, `pd.Series[str]`等）
- vectorbt関連の処理が発生したとき
  - すぐにlocalのdocuments(docs/vectorbt/)を参照する
  - localに情報がなければ、速やかに公式document(https://vectorbt.dev/api/)を参照する
- **future leak / point-in-time contamination を最優先で疑う**
  - snapshot / universe / fundamentals / ranking は必ず `as_of_date` 基準で切る
  - `src/shared/utils/pit_guard.py` の helper を優先利用する
  - `latest per code/group` は as-of filtering の後にだけ取る
  - 研究・分析ロジック変更時は PIT stability test か future-row exclusion test を追加する
- **ライブラリ活用**: 車輪の再発明を避け、既存機能を最大活用
- **設計原則**: Single Responsibility Principle, DRY原則の厳守
- unittestを必ず作成する

## 🛡️ 本番環境対応・信頼性強化

### API / Data Access 境界
**詳細**: `../../.codex/skills/bt-api-architecture/SKILL.md`
- **外部APIクライアント**: `src/infrastructure/external_api/` は J-Quants / moomoo など外部接続を担当する
- **ローカルData Plane**: backtest / analytics / screening は `DatasetSnapshotReader` / `MarketDbReader` を直接使い、FastAPI 内部HTTPを経由しない
- **公開API境界**: ts/web との接続は FastAPI (:3002) + OpenAPI contracts を SoT にする
- **旧実装削除**: `src/data/database.py` や Hono / ts API サーバーの互換経路を再導入しない

### Kelly基準数値安全性
- **ゼロ除算対策**: Kelly係数計算時の分母チェック（b > 0 検証）
- **NaN/Inf検証**: リターン比較時の異常値検出・安全なフォールバック処理
- **実装箇所**: `src/domains/strategy/core/mixins/portfolio_analyzer_mixin_kelly.py:168-171`

### 並列処理タイムアウト
- **ProcessPoolExecutor タイムアウト**: 1組み合わせあたり600秒（10分）制限
- **ハングアップ防止**: 無限ループ・デッドロック検出による自動スキップ
- **実装箇所**: `src/domains/optimization/engine.py:462-498`

### データ品質検証
- **空DataFrame検出**: Close/Volume データの全NaNチェック
- **早期エラー通知**: データ品質問題を処理開始前に検出
- **実装箇所**: `src/domains/strategy/signals/processor.py:220-228`

### テスト品質
- **カバレッジ計測**: pytest-cov導入済み、CI の coverage gate を SoT にする
- **包括的テストスイート**: unit / integration / server / domain tests を変更範囲に合わせて実行する
- **古い件数固定禁止**: test count や coverage 数値は固定文書化せず、現在の CI / coverage 出力で確認する

## 🎯 統一Signalsシステム
**詳細**: `../../.codex/skills/bt-signal-system/SKILL.md`

### 概念統一の原則
- **Entry Filters（絞り込み）**: 基本エントリー条件を**AND条件で絞り込む**（`entry_filter_params`）
- **Exit Triggers（発火）**: 基本エグジット条件に**OR条件で追加発火**（`exit_trigger_params`）
- **35種類シグナル統合**: breakout/volume/trading_value/beta/fundamental/rsi_threshold/sector_strength等
- **両用設計**: 同一シグナル関数でエントリー・エグジット両対応（direction/condition切り替え）

## 🚨 重要なVectorBT設定

### 多資産ポートフォリオ設定
**詳細**: `docs/vectorbt/portfolio-optimization.md`
- VectorBT `cash_sharing=True` + `group_by=True`の正しい設定方法
- 2段階資金配分最適化システム（398銘柄→77銘柄で5.2倍改善）

### Kelly基準ポートフォリオ最適化（デフォルト有効）
**詳細**: `docs/kelly-criterion-allocation.md`
- **Kelly基準のみ使用**: 従来手法は完全廃止、Kelly基準に統一
- **資金配分最適化**: 半Kelly・分数Kelly（f=0.5推奨）による安全性重視の配分戦略

### β値シグナルのベンチマークデータ自動ロード
**詳細**: `docs/fixes/beta-filter-benchmark-data-loading.md`
- **自動判定システム**: β値シグナルが有効な場合にベンチマークデータを自動ロード

## 技術スタック

### 主要ライブラリ
- **vectorbt** (>=1.0.0): 高速ベクトル化バックテストフレームワーク
- **pydantic** (>=2.0.0): データバリデーション
- **pandas/numpy**: データ処理・数値計算
- **fastapi** (>=0.136.1): バックテストAPI サーバー
- **uvicorn** (>=0.47.0): ASGIサーバー
- **typer/rich**: CLIフレームワーク・美化

### 開発ツール
- **pyright/mypy**: 型チェック・静的解析
- **ruff**: リンター・フォーマッター
- **pytest**: テストフレームワーク

## 戦略YAML の二重ソース構造（重要）

戦略設定YAMLは**コードベース内**と**XDG準拠ディレクトリ**の2箇所に存在する。

| カテゴリ | 格納場所 | 性質 |
|---|---|---|
| `production` | `config/strategies/production/` | Git管理・読み取り専用 |
| `reference` | `config/strategies/reference/` | Git管理・読み取り専用 |
| `legacy` | `config/strategies/legacy/` | Git管理・読み取り専用 |
| `experimental` | `~/.local/share/trading25/strategies/experimental/` | **ユーザー編集可能・Git管理外** |

- **検索優先順**: experimental（外部）→ production → reference → legacy（`src/shared/paths/constants.py: SEARCH_ORDER`）
- **書き込み先**: 新規作成・複製・リネームは常に `experimental`（外部ディレクトリ）に保存
- **環境変数**: `TRADING25_DATA_DIR` / `TRADING25_STRATEGIES_DIR` でベースパス変更可能
- **実装**: `src/shared/paths/resolver.py`（検索・マージ）、`src/domains/strategy/runtime/loader.py`（読み書き）

## プロジェクト構成

```
<repo-root>/apps/bt/
├── config/                    # YAML設定システム
│   ├── strategies/
│   │   ├── production/       # 本番環境用戦略（Git管理）
│   │   ├── legacy/          # レガシー戦略（Git管理）
│   │   └── reference/       # リファレンス・テンプレート（Git管理）
│   └── default.yaml         # デフォルト設定
├── notebooks/
│   ├── templates/           # テンプレートNotebook
│   └── playground/          # ad hoc notebook playground（SoT ではない）
├── docs/                    # プロジェクトドキュメント
├── tests/                   # pytest suite
└── src/                     # ソースコード（5層構成）
    ├── entrypoints/         # 実行入口（http/cli）
    │   ├── http/            # FastAPI app, routes, middleware, schemas
    │   └── cli/             # `bt` Typer CLI
    ├── application/         # アプリケーションサービス層
    │   └── services/
    ├── domains/             # ドメインロジック
    │   ├── analytics/
    │   ├── backtest/
    │   ├── lab_agent/
    │   ├── optimization/
    │   └── strategy/
    ├── infrastructure/      # I/O・外部接続
    │   ├── data_access/
    │   ├── db/
    │   └── external_api/
    └── shared/              # 共有モジュール
        ├── config/
        ├── models/
        ├── observability/
        ├── paths/
        └── utils/

~/.local/share/trading25/          # XDG準拠 外部データディレクトリ
├── strategies/
│   └── experimental/              # 実験的戦略（ユーザー編集可能・Git管理外）
├── backtest/
│   ├── results/{strategy}/        # バックテスト結果HTML
│   └── optimization/{strategy}/   # 最適化結果
└── cache/                         # キャッシュ
```

**詳細**: `src/`配下の詳細はコードまたは各Skillsを参照

## 基本使用方法

### 環境セットアップ
```bash
uv sync
```

### バックテストCLI（bt コマンド）
**詳細**: `../../.codex/skills/bt-cli-commands/SKILL.md`

```bash
# バックテスト実行
uv run bt backtest range_break_v5

# パラメータ最適化
uv run bt backtest range_break_v6 --optimize

# 戦略一覧
uv run bt list

# 設定検証
uv run bt validate range_break_v5

# APIサーバー起動
uv run bt server
uv run bt server --port 3002 --reload
```

### ポートフォリオCLI
`apps/bt` の `portfolio` コマンドは廃止済み。ポートフォリオ操作は `apps/ts/packages/web` の
Portfolio UI から実行する。

### 開発・検証
```bash
# リント
uv run ruff check src/

# 型チェック
uv run pyright src/

# テスト実行
uv run pytest tests/
```

## 詳細情報

### Skills（on-demand loading）
- **Agent System**: `../../.codex/skills/bt-agent-system/SKILL.md`（戦略自動生成・最適化）
- **API Architecture**: `../../.codex/skills/bt-api-architecture/SKILL.md`
- **Signal System**: `../../.codex/skills/bt-signal-system/SKILL.md`
- **CLI Commands**: `../../.codex/skills/bt-cli-commands/SKILL.md`
- **Optimization**: `../../.codex/skills/bt-optimization/SKILL.md`
- **Research Workflow**: `../../.codex/skills/bt-research-workflow/SKILL.md`
- **Strategy Config**: `../../.codex/skills/bt-strategy-config/SKILL.md`

### User-Level Skills
ユーザーレベルの process skill（`~/.agents/skills/`）も利用可能。bt 作業では必ず root `AGENTS.md` と上記 project-specific skills を優先する。
- **`aicheck`** — review / validation / commit 前確認。bt では対象 domain skill と focused `ruff` / `pyright` / `pytest` / research guardrail を組み合わせる。
- **`finish`** — 検証済み変更の docs 確認、明示 staging、commit / push / PR handoff。`scripts/prepush-ci.sh` が必要な場面ではこれを優先する。
- **`gh-pr-review-merge`** — GitHub PR のレビュー、required checks 確認、明示確認後の merge。

### Issue管理
- **SoT**: GitHub Issues (`https://github.com/d25037/trading25/issues`)
- **運用**: 新規 task は GitHub Issue を作成し、PR では `Closes #<number>` / `Fixes #<number>` を使う
- **ローカル**: `../../issues/done/` と `../../issues/archive/migrated-to-github/` は過去の local issue archive。active backlog として扱わない
- **/finish 時**: 関連する GitHub Issue を確認し、該当するものがあれば close または進捗コメントを残す

### ドキュメント
- **戦略一覧**: `docs/strategies.md`
- **コマンド詳細**: `docs/commands.md`
- **最適化システム**: `docs/parameter-optimization-system-v2.md`
- **VectorBT**: `docs/vectorbt/`

## アーキテクチャ特徴

### Runner-first 研究実行
- reproducible bundle を runner script から保存する
- notebook は latest bundle を読む viewer-only surface として扱う
- 研究内の高速会計は `vectorbt`、上位候補の verification は `Nautilus` に寄せる

### 統一システム設計
- **Runner First**: domain -> runner -> bundle -> optional notebook viewer を既定にする
- **型安全性重視**: Python標準型ヒントによる軽量・高速バリデーション
- **YAML完全制御**: 戦略実装パッケージ完全削除（1,000+ lines削減）・`entry_filter_params`/`exit_trigger_params`による統一管理
- **Data Plane統合**: runtime のデータ取得は DuckDB-backed reader を SoT にし、FastAPI は ts/web 向け公開境界として扱う

### VectorBT使用の利点
- 100倍以上の高速化を実現
- ベクトル化処理による大幅な性能向上
- 大規模データセット対応

この設計により、**runner-first research**・**VectorBT高速化**・**Nautilus verification**・**統一モデル管理**・**統一シグナルシステム**を組み合わせた、スケーラブルで型安全な戦略分析プラットフォームを構築しています。
