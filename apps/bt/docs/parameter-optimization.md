# 戦略パラメータ最適化ガイド

## 概要

戦略のハイパーパラメータ（SMA期間、RSI閾値、ボリンジャーバンド幅等）を自動最適化し、インタラクティブな可視化Notebookを生成するシステム。

**新システムの特徴**:
- ✅ **YAML完全制御**: グリッドYAMLによるパラメータ範囲定義・再利用性重視
- ✅ **CLI並列処理**: リアルタイム進捗出力・高速化
- ✅ **2段階Kelly基準評価**: 初回等分配 → Kelly基準資金配分 → Kelly配分バックテスト（リスク調整）
- ✅ **インタラクティブ可視化**: 3D散布図・ヒートマップ等を含むNotebook自動生成
- ✅ **複合スコアリング**: 複数指標（Sharpe/Calmar/Total Return）を正規化・重み付けして総合評価

---

## なぜパラメータ最適化が必要か

### 手動調整の問題点

❌ **時間がかかる**: 数十〜数百のパラメータ組み合わせを手動で試すのは非現実的
❌ **局所最適に陥る**: 経験則で選んだパラメータが最適とは限らない
❌ **再現性が低い**: 調整プロセスが体系化されていない

### 自動最適化の利点

✅ **網羅的探索**: すべてのパラメータ組み合わせを試せる
✅ **客観的評価**: 数値指標（Kelly基準・複合スコアリング）による科学的な評価
✅ **再現可能**: 最適化プロセスが明確で再現可能
✅ **時間短縮**: 並列処理で大幅な時間短縮
✅ **リスク調整**: Kelly基準による銘柄間リスク差を適切に反映

---

## システムフロー

```
1. パラメータグリッド定義（YAML）
   ↓
2. CLI並列最適化実行
   ├─ ベース戦略YAML読み込み（シグナル有効化・固定パラメータ）
   ├─ グリッドYAMLマージ（最適化パラメータのみ上書き）
   ├─ SignalParams動的構築（ベース設定 + 最適化パラメータ）
   ├─ 並列バックテスト
   │  ├─ 初回等分配バックテスト（全銘柄均等配分）
   │  ├─ Kelly基準資金配分計算
   │  └─ Kelly配分バックテスト（最終評価）
   └─ リアルタイム進捗出力
   ↓
3. 最適化結果Notebook生成
   ├─ 複合スコアリングランキング順ソート
   ├─ 3D可視化Notebook自動生成
   └─ インタラクティブ分析環境
```

---

## グリッドYAML形式

### ディレクトリ構成

```
config/
└── optimization/
    ├── template_grid.yaml  # テンプレート
    ├── range_break_v6_grid.yaml
    ├── sma_cross_grid.yaml
    └── bnf_mean_reversion_grid.yaml
```

### 戦略YAML vs グリッドYAMLの責任分離

**重要な設計原則**: 戦略の**構造定義**と**パラメータ最適化**を分離します。

#### 戦略YAML (`config/strategies/experimental/range_break_v6.yaml`) の役割

- ✅ シグナルの有効化（`enabled: true/false`）
- ✅ シグナルの方向・条件（`direction: "high"`, `condition: "break"`）
- ✅ 固定パラメータ（最適化しないパラメータ）
- ✅ 戦略の基本構造・ロジック

**例**: レンジブレイク戦略の場合
```yaml
# config/strategies/experimental/range_break_v6.yaml（ベース戦略）
entry_filter_params:
  period_breakout:
    enabled: true        # ← 戦略YAMLで管理
    direction: "high"    # ← 戦略YAMLで管理
    condition: "break"   # ← 戦略YAMLで管理
    lookback_days: 10    # ← グリッドYAMLで最適化
    period: 100          # ← グリッドYAMLで最適化

  volume:
    enabled: true        # ← 戦略YAMLで管理
    direction: "surge"   # ← 戦略YAMLで管理
    threshold: 2.0       # ← グリッドYAMLで最適化
```

#### グリッドYAML (`config/optimization/range_break_v6_grid.yaml`) の役割

- ✅ **最適化対象パラメータの範囲のみ**
- ✅ `parameter_ranges`配下のパラメータ値リスト

```yaml
# config/optimization/range_break_v6_grid.yaml（最適化パラメータのみ）
parameter_ranges:
  entry_filter_params:
    period_breakout:
      lookback_days: [5, 10, 15, 20]  # 最適化対象
      period: [30, 50, 100, 200]      # 最適化対象

    volume:
      threshold: [1.5, 2.0, 2.5, 3.0]  # 最適化対象
```

#### 統合ルール

1. ベース戦略YAMLを読み込み（全シグナル設定）
2. グリッドYAMLで指定されたパラメータ**のみ**上書き
3. その他の設定（`enabled`, `direction`, `condition`等）はベース戦略YAMLから継承

### グリッドYAML仕様

**設計思想**:
- **最小限の記述**: パラメータ範囲のみ記述
- **自動推測**: ファイル名から戦略名・ベース設定を自動推測（`range_break_v6_grid.yaml` → `range_break_v6.yaml`）
- **デフォルト活用**: 最適化設定・共通設定はdefault.yamlから読み込み
- **規約ベース出力**: 結果ファイルは戦略名から自動生成

#### テンプレート

```yaml
# config/optimization/{strategy_name}_grid.yaml

description: "戦略の説明"  # オプション

# ===== 最適化するパラメータ範囲 =====
parameter_ranges:
  # entry_filter_params配下のパラメータ
  entry_filter_params:
    # シグナル名:
    #   param1: [value1, value2, value3]
    #   param2: [value1, value2]

  # exit_trigger_params配下のパラメータ
  exit_trigger_params:
    # シグナル名:
    #   param1: [value1, value2]
```

#### 具体例: レンジブレイクv6戦略

```yaml
# config/optimization/range_break_v6_grid.yaml

description: "レンジブレイクv6戦略のパラメータ最適化グリッド"

parameter_ranges:
  entry_filter_params:
    # Period Breakoutシグナル
    period_breakout:
      lookback_days: [5, 10, 15, 20]  # 短期最高値期間
      period: [30, 50, 100, 200]      # 長期最高値期間

    # Bollinger Bandsシグナル
    bollinger_bands:
      window: [10, 20, 30]  # BB期間
      alpha: [1.0, 1.5, 2.0]  # 標準偏差倍率

    # Volumeシグナル
    volume:
      threshold: [1.5, 2.0, 2.5, 3.0]  # 出来高倍率
      short_period: [10, 20]  # 短期平均期間
      long_period: [50, 100]  # 長期平均期間

  exit_trigger_params:
    # ATR Support Breakシグナル
    atr_support_break:
      lookback_period: [10, 20, 30]  # サポートライン期間
      atr_multiplier: [2.0, 2.5, 3.0]  # ATR倍率
```

**自動推測される設定**:
- **ベース戦略YAML**: `config/strategies/experimental/range_break_v6.yaml`（ファイル名から）
- **最適化設定**: `config/default.yaml`の`parameter_optimization`セクションから読み込み
- **共通設定**: `config/default.yaml`の`shared_config`セクションから読み込み
- **出力ファイル** (戦略別ディレクトリ + タイムスタンプ):
  - 可視化Notebook: `notebooks/generated/optimization/range_break_v6/20250112_143052.ipynb`

---

## CLIコマンド使用方法

### 基本的な使い方

```bash
# パラメータ最適化実行
# 自動的に config/optimization/{strategy_name}_grid.yaml を検索
uv run bt optimize range_break_v6
```

### 実行時の出力例

```
🚀 パラメータ最適化開始
戦略: range_break_v6
組み合わせ数: 96
並列処理数: 4

[1/96] lb=5, period=30, bb_win=10, vol_th=1.5: Sharpe=1.23, Calmar=0.89, Return=45.2%, Score=1.12
[2/96] lb=5, period=30, bb_win=10, vol_th=2.0: Sharpe=1.45, Calmar=1.02, Return=52.1%, Score=1.32
[3/96] lb=5, period=30, bb_win=10, vol_th=2.5: Sharpe=1.31, Calmar=0.95, Return=48.7%, Score=1.19
...
[96/96] lb=20, period=200, bb_win=30, vol_th=3.0: Sharpe=0.87, Calmar=0.65, Return=28.3%, Score=0.78

✅ 最適化完了!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 最適化結果（複合スコアランキング上位10件）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🥇 Rank 1 - 複合スコア: 1.65
  entry_filter_params.period_breakout.lookback_days: 10
  entry_filter_params.period_breakout.period: 100
  entry_filter_params.bollinger_bands.window: 20
  entry_filter_params.volume.threshold: 2.0
  → Sharpe: 1.87, Calmar: 1.35, Return: 68.5%

🥈 Rank 2 - 複合スコア: 1.58
  entry_filter_params.period_breakout.lookback_days: 10
  entry_filter_params.period_breakout.period: 100
  entry_filter_params.bollinger_bands.window: 30
  entry_filter_params.volume.threshold: 2.0
  → Sharpe: 1.79, Calmar: 1.28, Return: 65.3%

🥉 Rank 3 - 複合スコア: 1.52
  entry_filter_params.period_breakout.lookback_days: 15
  entry_filter_params.period_breakout.period: 100
  entry_filter_params.bollinger_bands.window: 20
  entry_filter_params.volume.threshold: 2.5
  → Sharpe: 1.72, Calmar: 1.22, Return: 62.1%

...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 可視化Notebook生成中...

✅ 可視化Notebook生成完了!
  📓 notebooks/generated/optimization/range_break_v6/20250112_143052.ipynb

次のステップ:
  jupyter lab notebooks/generated/optimization/range_break_v6/20250112_143052.ipynb
```

---

## 最適化指標の選び方

### ❌ Total Returnのみは危険

Total Returnだけで最適化すると、**リスクを無視**してしまいます。

**問題例**:
- 最大ドローダウン-80%でも高リターンのパラメータが選ばれる
- 実運用では心理的に耐えられない
- 破産リスクが高い

### ✅ 推奨アプローチ: 複合スコアリング

複数指標をバランスよく評価する**複合スコアリング**を推奨します。

#### 推奨指標の組み合わせ

| 指標 | 重み | 意味 | 重要度 |
|------|------|------|--------|
| **Sharpe Ratio** | 60% | リスク調整後リターン | ⭐⭐⭐ 最重要 |
| **Calmar Ratio** | 30% | ドローダウン考慮のリターン | ⭐⭐ 重要 |
| **Total Return** | 10% | 絶対リターン | ⭐ 参考 |

**計算式**:
```
複合スコア = 0.6 × 正規化Sharpe + 0.3 × 正規化Calmar + 0.1 × 正規化Total Return
```

**重要**: 異なるスケールの指標（Sharpe Ratio 0-3 vs Total Return 0-100%）を公平に比較するため、**正規化処理**（Min-Max正規化で0-1範囲に変換）を行います。

#### 各指標の詳細

**Sharpe Ratio（シャープレシオ）**:
```
Sharpe Ratio = (平均リターン - 無リスク金利) / リターンの標準偏差
```
- リスク（ボラティリティ）を考慮したリターン
- 値が大きいほど効率的
- **推奨: 主指標として60%の重み**

**Calmar Ratio（カルマーレシオ）**:
```
Calmar Ratio = 年率リターン / 最大ドローダウン（絶対値）
```
- 最大損失を考慮したリターン
- 実運用時の心理的負担を評価
- **推奨: 副指標として30%の重み**

**Total Return（トータルリターン）**:
```
Total Return = (最終資産 - 初期資産) / 初期資産
```
- 絶対的なリターン
- リスク無視のため参考程度
- **推奨: 参考指標として10%の重み**

**その他の指標**:
- **Max Drawdown**: 最大ドローダウン（最小化目標）
- **Win Rate**: 勝率（トレード成功率）
- **Profit Factor**: 勝ちトレード合計 / 負けトレード合計

### 複合スコアリング設定（`config/default.yaml`）

```yaml
# config/default.yaml
default:
  parameters:
    shared_config:
      parameter_optimization:
        enabled: true
        method: "grid_search"  # "grid_search" or "random_search"
        n_jobs: 4  # 並列処理数（-1で全CPUコア）

        # 複合スコアリング設定
        optimization_strategy: "composite"
        scoring_weights:
          sharpe_ratio: 0.6  # リスク調整後リターン
          calmar_ratio: 0.3  # ドローダウン考慮
          total_return: 0.1  # 絶対リターン
```

---

## 2段階Kelly基準評価プロセス

### なぜ2段階評価が必要か

**等分配の問題点**:
- ❌ すべての銘柄に均等配分すると、リスクの高い銘柄に過剰資金配分
- ❌ リスクの低い銘柄に十分な資金配分ができない
- ❌ 実運用に近い評価ができない

**Kelly基準による最適配分の利点**:
- ✅ 期待リターン・勝率・リスクに基づく動的資金配分
- ✅ リスクの高い銘柄への配分を自動的に抑制
- ✅ オーバーフィッティング防止（リスク調整後の安定配分）
- ✅ 実運用に近い資金配分でパラメータ最適化

### 評価フロー

```
1. ベース戦略YAML + グリッド最適化パラメータをマージ
   ↓
2. SignalParams動的構築
   ↓
3. 初回バックテスト実行（全銘柄等分配）
   ├─ Kelly基準計算用に均等配分でバックテスト
   └─ 各銘柄の期待リターン・勝率・リスクを計算
   ↓
4. Kelly基準による最適資金配分計算
   ├─ Kelly係数 = (期待値 / リスク) × kelly_fraction
   └─ 配分比率を自動調整（min_allocation〜max_allocation範囲内）
   ↓
5. Kelly配分バックテスト実行（最終評価）
   ├─ 最適配分でバックテストを再実行
   └─ リスク調整後の結果を評価
   ↓
6. 複合スコア計算（Kelly配分結果を使用）
   ├─ Sharpe Ratio、Calmar Ratio、Total Returnを計算
   ├─ 正規化処理（0-1範囲に変換）
   └─ 重み付け合計
```

**設定パラメータ**:
- `kelly_fraction`: Kelly係数調整（0.5=Half Kelly, 1.0=Full Kelly, 2.0=Double Kelly）
  - **推奨**: 0.5（Half Kelly）= 安全性重視
- `min_allocation`: 最小配分率（例: 0.01 = 1%）
- `max_allocation`: 最大配分率（例: 0.5 = 50%）

---

## 実装例

### 例1: SMAクロス戦略の最適化

```yaml
# config/optimization/sma_cross_grid.yaml
description: "SMAクロス戦略のパラメータ最適化グリッド"

parameter_ranges:
  entry_filter_params:
    crossover:
      indicator_type: ["sma"]  # 固定（1種類のみ）
      short_period: [5, 10, 15, 20]
      long_period: [50, 100, 150, 200]
```

```bash
# 実行
uv run bt optimize sma_cross
```

**組み合わせ数**: 4 × 4 = 16通り

### 例2: BNF平均回帰戦略の最適化

```yaml
# config/optimization/bnf_mean_reversion_grid.yaml
description: "BNF平均回帰戦略のパラメータ最適化グリッド"

parameter_ranges:
  entry_filter_params:
    # 平均回帰シグナル
    mean_reversion:
      ma_type: ["sma", "ema"]
      ma_period: [10, 20, 30, 50]
      deviation_threshold: [1.5, 2.0, 2.5]

    # RSI閾値シグナル
    rsi_threshold:
      period: [7, 14, 21]
      oversold: [20, 25, 30]
```

```bash
# 実行
uv run bt optimize bnf_mean_reversion
```

**組み合わせ数**: (2 × 4 × 3) × (3 × 3) = 24 × 9 = 216通り

### 例3: レンジブレイクv6戦略の最適化（エントリー・エグジット両方）

```yaml
# config/optimization/range_break_v6_grid.yaml
description: "レンジブレイクv6戦略のパラメータ最適化グリッド"

parameter_ranges:
  entry_filter_params:
    period_breakout:
      lookback_days: [5, 10, 15, 20]
      period: [30, 50, 100, 200]

    bollinger_bands:
      window: [10, 20, 30]
      alpha: [1.0, 1.5, 2.0]

    volume:
      threshold: [1.5, 2.0, 2.5, 3.0]
      short_period: [10, 20]
      long_period: [50, 100]

  exit_trigger_params:
    atr_support_break:
      lookback_period: [10, 20, 30]
      atr_multiplier: [2.0, 2.5, 3.0]
```

```bash
# 実行
uv run bt optimize range_break_v6
```

**組み合わせ数**: (4 × 4) × (3 × 3) × (4 × 2 × 2) × (3 × 3) = 16 × 9 × 16 × 9 = 20,736通り
**推奨**: 並列処理数を増やす（`n_jobs: 8`等）

---

## 可視化Notebook

### 自動生成される可視化内容

最適化完了後、以下の内容を含むインタラクティブNotebookが自動生成されます：

1. **複合スコアランキング表**（上位20件）
2. **3D散布図**（パラメータ vs スコア）
   - 例: X軸=短期SMA期間、Y軸=長期SMA期間、Z軸=複合スコア
3. **パラメータ感度分析**（ヒートマップ）
   - 各パラメータが複合スコアに与える影響
4. **指標別分布図**（Sharpe/Calmar/Return）
   - ヒストグラム・バイオリンプロット
5. **パラメータ相関行列**
   - どのパラメータが相互に影響を与えるか
6. **最適パラメータ詳細表**
   - 最良パラメータの全情報
7. **Kelly配分結果**
   - 銘柄別資金配分比率

### 使い方

```bash
# 可視化Notebookを開く
jupyter lab notebooks/generated/optimization/range_break_v6/20250112_143052.ipynb
```

---

## ベストプラクティス

### 1. パラメータ範囲の設定

❌ **間違い**: 極端すぎる範囲
```yaml
parameter_ranges:
  entry_filter_params:
    period_breakout:
      lookback_days: [1, 2, 3, 500]  # 極端
      period: [5, 10, 1000]  # 極端
```

✅ **正解**: 常識的な範囲
```yaml
parameter_ranges:
  entry_filter_params:
    period_breakout:
      lookback_days: [5, 10, 15, 20]  # 妥当
      period: [30, 50, 100, 200]  # 妥当
```

### 2. 最適化設定のカスタマイズ

最適化設定（並列処理数、スコアリング重み等）は`config/default.yaml`で管理します：

```yaml
# config/default.yaml
default:
  parameters:
    shared_config:
      parameter_optimization:
        enabled: true
        method: "grid_search"  # "grid_search" or "random_search"
        n_jobs: 4  # 並列処理数（-1で全CPUコア）

        # 複合スコアリング設定
        optimization_strategy: "composite"
        scoring_weights:
          sharpe_ratio: 0.6  # リスク調整後リターン
          calmar_ratio: 0.3  # ドローダウン考慮
          total_return: 0.1  # 絶対リターン

      # Kelly基準設定
      kelly_fraction: 0.5  # Half Kelly（安全性重視）
      min_allocation: 0.01  # 最小配分率（1%）
      max_allocation: 0.5   # 最大配分率（50%）
```

### 3. 組み合わせ数の管理

パラメータ組み合わせ数が多すぎる場合は、以下の対策を検討：

- **パラメータ範囲を絞る**: 候補値を減らす
- **並列処理数を増やす**: `n_jobs: 8`等
- **ランダムサーチに切り替え**: `method: "random_search"`（`n_trials: 100`で試行回数制限）
- **段階的最適化**: 重要なパラメータから順に最適化

### 4. 複数指標のバランス

❌ **間違い**: Total Returnのみ
```yaml
scoring_weights:
  total_return: 1.0  # リスク無視
```

✅ **正解**: Sharpe + Calmar + Total Return
```yaml
scoring_weights:
  sharpe_ratio: 0.6  # リスク調整後リターン
  calmar_ratio: 0.3  # ドローダウン考慮
  total_return: 0.1  # 絶対リターン
```

### 5. データ分割（今後実装予定）

❌ **間違い**: 全データで最適化
✅ **正解**: Train/Testに分割して過学習を防ぐ

```yaml
# Train期間で最適化（今後実装予定）
shared_config:
  start_date: "2020-01-01"
  end_date: "2022-12-31"  # Train期間
```

```yaml
# Test期間で検証（今後実装予定）
shared_config:
  start_date: "2023-01-01"
  end_date: "2024-12-31"  # Test期間
```

---

## グリッドサーチ vs ランダムサーチ

### グリッドサーチ（Grid Search）

**概要**: 全パラメータ組み合わせを網羅的に探索

**メリット**:
- ✅ 確実に最適解を見つけられる
- ✅ パラメータ空間全体を把握できる

**デメリット**:
- ❌ パラメータ数が増えると組み合わせ爆発
- ❌ 計算時間が長い

**推奨ケース**:
- パラメータ数が少ない（2〜3個）
- パラメータ範囲が狭い
- 確実に最適解を見つけたい

**設定**:
```yaml
# config/default.yaml
parameter_optimization:
  method: "grid_search"
  max_combinations: 100  # 最大組み合わせ数制限（オプション）
```

### ランダムサーチ（Random Search）

**概要**: パラメータをランダムサンプリングして探索

**メリット**:
- ✅ 高次元パラメータ空間でも高速
- ✅ 試行回数を制御できる

**デメリット**:
- ❌ 最適解を見逃す可能性がある
- ❌ 再現性が低い（ランダム性）

**推奨ケース**:
- パラメータ数が多い（4個以上）
- パラメータ範囲が広い
- 高速に良い解を見つけたい

**設定**:
```yaml
# config/default.yaml
parameter_optimization:
  method: "random_search"
  n_trials: 50  # 50回ランダムサンプリング
```

---

## トラブルシューティング

### Q1: 最適化が遅い

**原因**: パラメータ組み合わせ数が多い

**解決策**:
1. `config/default.yaml`で並列処理数を調整
   ```yaml
   parameter_optimization:
     n_jobs: 8  # CPUコア数に応じて並列処理数を増やす
   ```
2. パラメータ範囲を絞る（候補値を減らす）
3. ランダムサーチに切り替え
   ```yaml
   parameter_optimization:
     method: "random_search"
     n_trials: 50  # 試行回数制限
   ```

### Q2: すべてのパラメータで同じスコア

**原因**: 戦略が正しく実装されていない、またはシグナルが機能していない

**解決策**:
1. ベース戦略YAMLのシグナル有効化を確認
   ```yaml
   entry_filter_params:
     period_breakout:
       enabled: true  # ← 確認
   ```
2. シグナル生成ロジックをデバッグ（`printlog=True`でログ出力）
3. グリッドYAMLのパラメータ範囲を確認（有効な範囲か）

### Q3: 最適パラメータがTest期間で機能しない

**原因**: 過学習（Overfitting）

**解決策**:
1. パラメータ範囲を広げる（極端な値を避ける）
2. 複合スコアリングでロバスト性を重視
   ```yaml
   scoring_weights:
     sharpe_ratio: 0.6  # リスク調整重視
     calmar_ratio: 0.3  # ドローダウン考慮
     total_return: 0.1  # 参考程度
   ```
3. Kelly基準評価によるリスク調整（デフォルトで有効）
4. データ分割（今後実装予定）

### Q4: Kelly配分がうまくいかない

**原因**: Kelly係数が不適切、または銘柄数が少ない

**解決策**:
1. Kelly係数を調整（Half Kelly推奨）
   ```yaml
   kelly_fraction: 0.5  # 保守的な配分
   ```
2. 配分範囲を調整
   ```yaml
   min_allocation: 0.01  # 最小1%
   max_allocation: 0.5   # 最大50%
   ```
3. 銘柄数を増やす（リスク分散）

---

## 関連ドキュメント

- [`parameter-optimization-system.md`](parameter-optimization-system.md): 開発者向け実装詳細・アーキテクチャ
- [`kelly-criterion-allocation.md`](kelly-criterion-allocation.md): Kelly基準資金配分の詳細説明
- [`vectorbt/portfolio-optimization.md`](vectorbt/portfolio-optimization.md): VectorBT設定最適化
- [`strategies.md`](strategies.md): 実装済み戦略一覧

---

## まとめ

### 推奨設定

```yaml
# config/default.yaml
parameter_optimization:
  enabled: true
  method: "grid_search"  # パラメータ少ない場合
  # method: "random_search"  # パラメータ多い場合
  n_trials: 50
  n_jobs: 4

  optimization_strategy: "composite"  # 複合スコアリング推奨
  scoring_weights:
    sharpe_ratio: 0.6  # 主指標（リスク調整）
    calmar_ratio: 0.3  # ドローダウン考慮
    total_return: 0.1  # 参考

kelly_fraction: 0.5  # Half Kelly（安全性重視）
min_allocation: 0.01  # 最小配分率（1%）
max_allocation: 0.5   # 最大配分率（50%）
```

### 最適化フロー

1. **グリッドYAML作成**: `config/optimization/{strategy_name}_grid.yaml`
2. **パラメータ範囲定義**: 常識的な範囲で設定
3. **最適化実行**: `uv run bt optimize {strategy_name}`
4. **結果分析**: ランキング確認・可視化Notebook確認
5. **検証**: Test期間でパフォーマンス確認（今後実装予定）
6. **実運用**: 最適パラメータで本番運用

### 新システムの利点

| 項目 | 利点 |
|------|------|
| **YAML完全制御** | 設定の再利用・管理が容易 |
| **2段階Kelly基準評価** | リスク調整後の安定配分で評価 |
| **複合スコアリング** | リスク・リターン・ドローダウンをバランス良く評価 |
| **CLI並列処理** | 高速化・リアルタイム進捗表示 |
| **可視化Notebook自動生成** | インタラクティブ分析環境 |
| **責任分離** | 戦略構造定義とパラメータ最適化を分離 |

---

**🚀 自動最適化で戦略パフォーマンスを最大化しましょう！**
