# パラメータ最適化システム

Kelly基準2段階最適化・複合スコアリング・並列処理・結果可視化対応

## システム概要

**実装箇所**: `src/optimization/`

### 最適化エンジン（engine.py）

- **YAML統合**: グリッド設定からパラメータ組み合わせ自動生成
- **Kelly基準2段階評価**: 等分配バックテスト → Kelly配分計算 → Kelly配分バックテスト
- **並列処理対応**: ProcessPoolExecutor による n_jobs 並列実行
- **自動パス推測**: 戦略名から experimental/production 自動検索
- **Notebook自動生成**: 最適化完了後に可視化Notebookを自動生成

### SignalParams動的ビルダー（param_builder.py）

- **ネストパラメータ構築**: YAML階層構造から SignalParams オブジェクト生成
- **ベース設定マージ**: グリッド指定値 + ベース戦略の固定値を統合
- **シグナル名検証**: グリッドYAML vs ベースYAMLのシグナル名整合性チェック
- **警告システム**: 存在しないシグナル名を最適化前に警告表示

### 最適化結果可視化Notebook生成（notebook_generator.py）

- **Papermill互換**: テンプレートベースNotebook自動生成
- **Top 10ランキング**: 複合スコア上位10件の詳細レポート
- **パラメータ分析**: 各パラメータの影響度可視化
- **DB名自動抽出**: データベースファイルパスから識別子を自動抽出
- **タイムスタンプ管理**: 実行日時ベースのファイル命名

## グリッドYAML設定

**実装箇所**: `config/optimization/`

### 設定構造

```yaml
description: "レンジブレイクv6パラメータ最適化"
base_config: "config/strategies/experimental/range_break_v6.yaml"

parameter_ranges:
  entry_filter_params:
    period_breakout:
      period: [50, 100, 200]  # 長期最高値期間
    bollinger_bands:
      std_dev: [2.0, 2.5, 3.0]  # ボリンジャーバンド標準偏差倍率
  exit_trigger_params:
    atr_support_break:
      atr_multiplier: [2.0, 2.5, 3.0]  # ATR倍率
```

### デカルト積生成

全組み合わせの自動生成（上記例: 3 × 3 × 3 = 27通り）

### 3層構造対応

- `legacy/`: レガシー戦略
- `production/`: 本番環境用戦略
- `experimental/`: 実験的戦略

## 複合スコアリング

Min-Max正規化による重み付け合計

### スコアリング指標

1. **sharpe_ratio**: シャープレシオ（リスク調整後リターン）- デフォルト重み0.5
2. **calmar_ratio**: カルマーレシオ（ドローダウン考慮）- デフォルト重み0.3
3. **total_return**: トータルリターン（絶対リターン）- デフォルト重み0.2

### 正規化処理

異なるスケールの指標を0-1範囲に統一

```python
normalized_score = (value - min_value) / (max_value - min_value)
composite_score = sum(weight * normalized_score for each metric)
```

## 最適化設定（config/default.yaml）

```yaml
parameter_optimization:
  enabled: false  # グローバル有効化フラグ（現在は bt backtest --optimize コマンド経由で実行）
  method: "grid_search"  # "grid_search" or "random_search"
  n_trials: 100  # ランダムサーチ用試行回数
  n_jobs: 1  # 並列処理数（1=シングルプロセス、-1=全CPUコア）

  # 複合スコアリング設定（正規化後の重み付け合計）
  scoring_weights:
    sharpe_ratio: 0.5  # シャープレシオ（リスク調整後リターン）
    calmar_ratio: 0.3  # カルマーレシオ（ドローダウン考慮）
    total_return: 0.2  # トータルリターン（絶対リターン）
```

## 可視化テンプレート

**実装箇所**: `notebooks/templates/optimization_analysis_template.ipynb`

### 機能

- **パラメータ化セル**: 最適化結果・戦略名・スコアリング重みを注入
- **ランキング表示**: Top 20パラメータ組み合わせ + Bottom 10ランキング（失敗パターン可視化）
- **パラメータ感度分析**: 相関係数ランキング + 上位パラメータの2D散布図（影響度可視化）
- **統計分析**: パラメータ別のパフォーマンス分布

## CLIサブコマンド

**実装箇所**: `src/cli_bt/optimize.py`

### 機能

- **Rich表示**: カラフルな進捗・ランキング・詳細表示
- **Top 10ランキング**: 複合スコア上位10件の表示
- **Notebookパス表示**: 生成された可視化NotebookのJupyter Lab起動コマンド表示
- **エラーハンドリング**: FileNotFoundError・ValueError・RuntimeError の適切な処理

### 使用例

```bash
# パラメータ最適化（--optimize / -O フラグ）
uv run bt backtest range_break_v6 --optimize
uv run bt backtest range_break_v6 -O  # 短縮形

# 詳細ログ出力（最適化モード時のみ有効）
uv run bt backtest range_break_v6 --optimize --verbose
uv run bt backtest range_break_v6 -O -v  # 短縮形
```

## 並列処理タイムアウト

**実装箇所**: `src/optimization/engine.py:307-341`

- **ProcessPoolExecutor タイムアウト**: 1組み合わせあたり600秒（10分）制限
- **ハングアップ防止**: 無限ループ・デッドロック検出による自動スキップ
- **エラーハンドリング**: TimeoutError・Exception の適切な捕捉と継続処理
- **ユーザー通知**: タイムアウト発生時の警告メッセージ表示

## 包括的テスト

**実装箇所**: `tests/unit/optimization/`

- **test_notebook_generator.py**: Notebook生成ロジックテスト（291行）
- **test_db_name_extraction.py**: DB名抽出ロジックテスト（71行）
- **test_executor_backtest_dir.py**: バックテストディレクトリ推測テスト（187行）

## 詳細ドキュメント

- `docs/parameter-optimization-system-v2.md`: システム概要
- `docs/parameter-optimization-system.md`: 詳細仕様
- `docs/parameter-optimization.md`: 実装ガイド

## 関連ファイル

- `src/optimization/engine.py`
- `src/optimization/param_builder.py`
- `src/optimization/notebook_generator.py`
- `src/cli_bt/optimize.py`
- `config/optimization/*_grid.yaml`
- `notebooks/templates/optimization_analysis_template.ipynb`
