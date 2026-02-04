# Trading Backtesting System

**Marimo Notebook実行システム**を中心とした高速バックテスト戦略ツール。
VectorBT基盤の高速ベクトル化バックテスト・Marimoによる戦略Notebook自動実行・YAML設定による柔軟なパラメータ管理を提供。

## Features

- **VectorBT高速バックテスト**: 100倍以上の高速化を実現するベクトル化処理
- **統一CLIシステム**: git風のコマンド体系（`bt`, `portfolio`）
- **YAML完全制御**: 戦略パラメータ・シグナル設定の一元管理
- **Kelly基準最適化**: 資金配分最適化による安全性重視のポートフォリオ構築
- **Marimo自動実行**: Notebookベースの分析・可視化ワークフロー
- **統一Signalsシステム**: 20種類のシグナル（breakout/volume/beta/fundamental等）
- **型安全性**: Pydanticによるデータバリデーション

## Quick Start

```bash
# Setup
uv sync

# List available strategies
uv run bt list

# Run backtest
uv run bt backtest production/sma_cross

# Run backtest with parameter optimization
uv run bt backtest range_break_v6 --optimize

# Validate strategy configuration
uv run bt validate range_break_v5

# Help
uv run bt --help
```

## CLI Commands

### バックテストCLI (`bt`)

```bash
# 戦略一覧表示
uv run bt list

# バックテスト実行
uv run bt backtest <strategy_name>

# パラメータ最適化付きバックテスト
uv run bt backtest <strategy_name> --optimize

# 設定検証
uv run bt validate <strategy_name>

# 古い結果のクリーンアップ
uv run bt cleanup --days 7
```

### ポートフォリオCLI (`portfolio`)

```bash
# ポートフォリオ分析
uv run portfolio --help
```

## Project Structure

```
trading25-bt/
├── config/                    # YAML設定システム（3層構造）
│   ├── strategies/
│   │   ├── production/       # 本番環境用戦略
│   │   ├── experimental/     # 実験的戦略
│   │   ├── legacy/          # レガシー戦略
│   │   └── reference/       # リファレンス・テンプレート
│   ├── optimization/        # パラメータ最適化グリッド設定
│   └── default.yaml         # デフォルト設定
├── notebooks/
│   ├── templates/           # テンプレートNotebook
│   └── generated/           # (廃止) 生成結果はXDG準拠パスへ移行
├── docs/                    # プロジェクトドキュメント
├── tests/                   # テストスイート（631 tests）
└── src/                    # ソースコード
    ├── agent/              # 戦略自動生成・最適化（GA/Optuna）
    ├── cli_bt/             # バックテストCLI（bt コマンド）
    ├── cli_portfolio/      # ポートフォリオCLI（portfolio コマンド）
    ├── api/                # REST APIクライアント
    ├── analysis/           # ポートフォリオ分析（PCA/リスク分析）
    ├── data/               # データ処理・ローダーシステム
    ├── backtest/           # バックテストシステム（Marimo実行）
    ├── strategy_config/    # 戦略設定管理（YAML）
    ├── optimization/       # パラメータ最適化システム
    ├── strategies/         # 戦略実装
    ├── models/             # 統一モデル（Pydantic型安全性）
    └── utils/              # ユーティリティ
```

## Tech Stack

### Core Libraries

| Library | Version | Description |
|---------|---------|-------------|
| vectorbt | >=0.26.0 | 高速ベクトル化バックテストフレームワーク |
| marimo | >=0.10.0 | 静的HTML出力対応Notebookフレームワーク |
| pydantic | >=2.0.0 | データバリデーション・型安全性 |
| pandas/numpy | - | データ処理・数値計算 |
| typer/rich | - | CLIフレームワーク・リッチ出力 |

### Development Tools

| Tool | Description |
|------|-------------|
| pyright/mypy | 型チェック・静的解析 |
| ruff | リンター・フォーマッター |
| pytest | テストフレームワーク |
| uv | 高速パッケージマネージャー |

## Architecture

### Signals System

統一Signalsシステムによる柔軟なエントリー・エグジット制御：

- **Entry Filters（絞り込み）**: 基本エントリー条件をAND条件で絞り込み
- **Exit Triggers（発火）**: 基本エグジット条件にOR条件で追加発火
- **20種類シグナル統合**: breakout/volume/trading_value/beta/fundamental/rsi_threshold等

### Kelly Criterion Portfolio Optimization

Kelly基準による資金配分最適化：

- **半Kelly・分数Kelly**: f=0.5推奨による安全性重視の配分戦略
- **多資産ポートフォリオ**: VectorBT `cash_sharing=True` + `group_by=True`

### Marimo Execution System

- テンプレートベース標準分析フロー
- YAML設定統合・結果管理
- セキュリティ（パス検証・入力サニタイズ）

## Development

```bash
# 型チェック・リント
uv run ruff check src/
uv run pyright src/

# テスト実行
uv run pytest tests/

# テスト実行（カバレッジ付き）
uv run pytest tests/ --cov=src
```

## Documentation

| Document | Description |
|----------|-------------|
| [CLAUDE.md](./CLAUDE.md) | プロジェクト詳細ドキュメント |
| [docs/strategies.md](./docs/strategies.md) | 戦略一覧 |
| [docs/commands.md](./docs/commands.md) | コマンド詳細 |
| [docs/vectorbt/](./docs/vectorbt/) | VectorBT関連ドキュメント |

## License

Private Project
