# 戦略設定・YAML管理システム

## 3層構造設定管理

**実装箇所**: `config/strategies/`

### ディレクトリ構造

```
config/strategies/
├── production/       # 本番環境用戦略（実運用可能な安定版）
│   ├── range_break_v15.yaml
│   └── range_break_v16.yaml
├── experimental/     # 実験的戦略（検証中・開発版）
│   ├── short_strategy.yaml
│   └── sma_break.yaml
├── legacy/          # レガシー戦略設定（過去バージョン退避）
│   ├── range_break_v8.yaml
│   ├── range_break_v11.yaml
│   └── range_break_v14.yaml
└── reference/       # リファレンス・テンプレート
    └── strategy_template.yaml  # 新戦略作成用テンプレート
```

### 自動探索順序

カテゴリ省略時は以下の順で自動探索：
1. `experimental/`
2. `production/`
3. `reference/`
4. `legacy/`

**実装箇所**: `ConfigLoader._infer_strategy_path()`

## YAML設定ローダー

**実装箇所**: `src/strategy_config/loader.py`

### 機能

- 戦略設定ローダー
- デフォルト設定マージ（`config/default.yaml`との統合）
- パス推測機能（3層構造対応）
- バリデーション機能

### 使用例

```python
from src.strategy_config.loader import ConfigLoader

# 設定ロード（カテゴリ省略）
config = ConfigLoader.load("range_break_v5")

# 設定ロード（カテゴリ明示）
config = ConfigLoader.load("production/range_break_v5")
```

## YAML完全制御アーキテクチャ

### 戦略実装パッケージ完全削除

- `implementations/`パッケージ完全削除（1,000+ lines削減）
- 全戦略で`YamlConfigurableStrategy`を直接使用
- 戦略固有ロジックは完全にYAML制御（`entry_filter_params`/`exit_trigger_params`）

### YamlConfigurableStrategy直接使用

**実装箇所**: `src/strategies/core/yaml_configurable_strategy.py`

- 具象クラス化完了（抽象メソッド削除・デフォルト実装追加）
- 全戦略で共通基底クラス使用
- YAML完全制御

### 戦略ファクトリシステム

**実装箇所**: `src/strategies/core/factory.py`

- 大幅簡素化（607→514 lines・動的インポート削除）
- SignalParams変換: YAML → Pydanticモデル自動変換

## YAML設定例

### 基本構造

```yaml
strategy:
  name: "range_break_v5"
  description: "レンジブレイク戦略 v5"

  # データ設定
  data:
    database: "sample-prime-A"
    start_date: "2018-01-04"
    end_date: "2024-09-30"

  # 資金管理
  portfolio:
    initial_cash: 10000000
    fees: 0.001
    slippage: 0.001

  # エントリーフィルター（AND結合）
  entry_filter_params:
    period_breakout:
      period: 100
    volume_surge:
      short_window: 5
      long_window: 20
      surge_threshold: 1.5
    beta:
      min_beta: 0.8
      max_beta: 1.5

  # エグジットトリガー（OR結合）
  exit_trigger_params:
    atr_support_break:
      atr_period: 14
      atr_multiplier: 2.0
    volume_drop:
      short_window: 5
      long_window: 20
      drop_threshold: 0.7
```

### パラメータ調整

**重要**: パラメータ変更は `config/strategies/production/` または `experimental/` 内のYAMLファイルを編集

### 新戦略作成

`config/strategies/reference/strategy_template.yaml` をベースに作成

## デフォルト設定マージ

**実装箇所**: `config/default.yaml`

### マージルール

1. 戦略固有設定が優先
2. デフォルト設定で不足分を補完
3. ネストされた設定も再帰的にマージ

### デフォルト設定例

```yaml
portfolio:
  initial_cash: 10000000
  fees: 0.001
  slippage: 0.001
  kelly_fraction: 0.5
  min_allocation: 0.01
  max_allocation: 0.15

parameter_optimization:
  enabled: false
  method: "grid_search"
  n_trials: 100
  n_jobs: 1
  scoring_weights:
    sharpe_ratio: 0.5
    calmar_ratio: 0.3
    total_return: 0.2
```

## バリデーション

### 設定検証コマンド

```bash
# カテゴリ省略可能
uv run bt validate range_break_v5

# カテゴリ明示も可能
uv run bt validate production/range_break_v5
```

### バリデーション項目

- YAML構文チェック
- 必須フィールドの存在確認
- データ型の検証
- シグナルパラメータの整合性チェック

## 統一モデルシステム

**実装箇所**: `src/models/`

### Pydantic型安全性

- **config.py**: 設定管理モデル（SharedConfig）
- **signals.py**: 統一シグナルモデル（Signals + SignalParams完全統合）

### SignalParams変換

YAML → Pydanticモデル自動変換

```python
# YAML
entry_filter_params:
  period_breakout:
    period: 100

# Pydantic変換後
signal_params.period_breakout = PeriodBreakoutParams(period=100)
```

## レガシー削除完了

- `models/filters.py` → 完全削除
- `models/triggers.py` → 完全削除
- `models/strategies.py` → 完全削除

## 関連ファイル

- `src/strategy_config/loader.py`
- `src/strategies/core/factory.py`
- `src/strategies/core/yaml_configurable_strategy.py`
- `src/models/config.py`
- `src/models/signals.py`
- `config/strategies/`（3層構造）
- `config/default.yaml`
- `tests/unit/strategy_config/test_loader.py`
