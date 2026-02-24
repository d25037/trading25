# ケリー基準ポートフォリオ最適化ガイド

## 概要

**ケリー基準（Kelly Criterion）**を用いた実運用可能なポートフォリオ最適化システム。統合ポートフォリオ全体の勝率・平均リターン・リスクから、各銘柄への最適配分率を数学的に算出します。

## ケリー基準とは

### 基本理論

ケリー基準は、長期的に資産を最大化するための最適な賭け金比率を決定する数学的手法です。

**Full Kelly公式**:
```
f* = (bp - q) / b
```

パラメータ:
- `f*`: 最適な資金配分比率
- `p`: 勝率（勝ちトレードの確率）
- `q`: 負け率（1 - p）
- `b`: オッズ比（平均勝ちトレード / 平均負けトレード）

**簡略化公式**:
```
f* = (win_rate × b - (1 - win_rate)) / b
```

### Half Kelly（推奨）

実務では、リスク軽減のため**Half Kelly**（ケリー値の半分）を使用することが一般的です：

```
f_half* = f* / 2
```

**利点**:
- リスクの大幅削減（分散を1/4に低減）
- リターンの小幅低減（約25%減）
- より安定したポートフォリオ運用

## 実装の正しいアプローチ

### 統合ポートフォリオ全体からの計算

**✅ 実運用可能なアプローチ**:
```
統合ポートフォリオ全体の統計を計算:
- 戦略全体勝率: 55%
- 平均勝ちトレード: +12%
- 平均負けトレード: -6%

ケリー基準計算:
- オッズ比 b = 12 / 6 = 2.0
- Kelly = (0.55 × 2.0 - 0.45) / 2.0 = 27.5%
- Half Kelly = 13.75%

結論:
「この戦略では、各銘柄に資金の13.75%ずつ配分すべき」
```

**実運用での使い方**:
1. バックテストで「この戦略には14%配分が最適」と判明
2. 実運用: シグナルが出た銘柄に14%ずつ投資
3. どの銘柄にシグナルが出るかは戦略ロジックが決定（未来予知不要）

### ❌ 間違ったアプローチ（銘柄別計算）

**実運用不可能**:
```
銘柄別にケリー基準を計算:
- 銘柄1234: 勝率70% → 配分30%
- 銘柄5678: 勝率45% → 配分0%

問題点:
- バックテストで得られた銘柄別統計は過去データ
- 実運用では「どの銘柄が高勝率になるか」は事前に分からない
- これはカーブフィッティング（未来予知前提）
```

## 実装概要

### アーキテクチャ

```
src/strategies/core/mixins/portfolio_analyzer_mixin_kelly.py
└── PortfolioAnalyzerKellyMixin
    ├── optimize_allocation_kelly()          # 統合ポートフォリオ全体の配分最適化
    ├── _calculate_kelly_for_portfolio()     # 統合ポートフォリオケリー計算
    └── run_optimized_backtest_kelly()       # 2段階最適化バックテスト
```

### 2段階最適化フロー

```
1. 第1段階: 探索的バックテスト
   ├── N銘柄均等配分（1/N）
   └── 統合ポートフォリオ全体のトレード統計を収集

2. 最適化計算（統合ポートフォリオ全体）
   ├── 戦略全体の勝率・平均リターン・リスクを計算
   ├── ケリー基準公式適用
   └── 制約適用（min/max配分率）

3. 第2段階: 最適化バックテスト
   ├── 各銘柄に同じ配分率を適用
   └── パフォーマンス評価
```

## 使用方法

### 基本的な使い方

```python
from src.domains.strategy.core.mixins.portfolio_analyzer_mixin_kelly import PortfolioAnalyzerKellyMixin

class MyStrategy(PortfolioAnalyzerKellyMixin):
    # ... 戦略実装 ...

# ケリー基準2段階最適化バックテスト実行
initial_pf, final_pf, optimized_allocation, stats = strategy.run_optimized_backtest_kelly(
    kelly_fraction=0.5,      # Half Kelly（推奨）
    min_allocation=0.01,     # 最小配分1%
    max_allocation=0.5       # 最大配分50%
)

# 結果確認
print(f"第1段階リターン: {initial_pf.total_return():.1%}")
print(f"第2段階リターン: {final_pf.total_return():.1%}")

# 統合ポートフォリオ統計確認
print(f"戦略全体勝率: {stats['win_rate']:.1%}")
print(f"平均勝ちトレード: {stats['avg_win']:.2f}")
print(f"平均負けトレード: {stats['avg_loss']:.2f}")
print(f"最適配分率: {optimized_allocation:.1%}")

# 実運用での使い方
print(f"実運用: シグナルが出た銘柄に{optimized_allocation:.1%}ずつ投資")
```

### パラメータ調整

#### Kelly係数（kelly_fraction）

```python
# Full Kelly（積極的・高リスク）
optimized_allocation, stats = strategy.optimize_allocation_kelly(
    portfolio, kelly_fraction=1.0
)

# Half Kelly（推奨・バランス型）
optimized_allocation, stats = strategy.optimize_allocation_kelly(
    portfolio, kelly_fraction=0.5
)

# Quarter Kelly（保守的・低リスク）
optimized_allocation, stats = strategy.optimize_allocation_kelly(
    portfolio, kelly_fraction=0.25
)
```

#### 配分率制約

```python
# 配分率制約設定
optimized_allocation, stats = strategy.optimize_allocation_kelly(
    portfolio,
    kelly_fraction=0.5,
    min_allocation=0.05,    # 最小5%（リスク管理）
    max_allocation=0.3      # 最大30%（過剰集中防止）
)
```

## 実装詳細

### 統合ポートフォリオケリー基準計算

```python
def _calculate_kelly_for_portfolio(portfolio):
    # 1. 統合ポートフォリオ全体のトレード統計取得
    trades_df = portfolio.trades.records_readable
    pnl_series = trades_df["PnL"]  # 全トレード（銘柄フィルタなし）

    # 2. 戦略全体の勝率計算
    win_rate = (pnl_series > 0).sum() / len(pnl_series)

    # 3. 平均勝ちトレード・平均負けトレード
    avg_win = pnl_series[pnl_series > 0].mean()
    avg_loss = abs(pnl_series[pnl_series < 0].mean())

    # 4. ケリー基準計算
    b = avg_win / avg_loss  # オッズ比
    kelly = (win_rate * b - (1 - win_rate)) / b

    return kelly, stats
```

### 配分率の制約適用

```python
# ケリー基準適用
if kelly_value > 0:
    optimized_allocation = kelly_value * kelly_fraction
    # 制約適用
    optimized_allocation = max(min_allocation, min(max_allocation, optimized_allocation))
elif kelly_value == 0:
    # トレード0件などでケリー値が0の場合は均等配分
    optimized_allocation = 1.0 / len(stock_codes)
else:
    # 負のケリー値の場合は最小配分
    optimized_allocation = min_allocation
```

## 従来手法との比較

| 項目 | 従来手法（同時保有数ベース） | ケリー基準手法（統合ポートフォリオ） |
|------|----------------------------|-------------------------------|
| **配分基準** | 最大同時保有数M | 戦略全体の勝率・リターン・リスク |
| **配分率** | `(1.0 / M) * 5.0` | ケリー公式による計算 |
| **統計単位** | ポジション数 | トレード統計（勝率・期待値） |
| **実運用可能性** | ○ | ○ |
| **数学的根拠** | 経験則 | 資産最大化の理論的保証 |

### 期待される効果

#### ケリー基準の利点

1. **数学的最適化**: 長期的資産最大化の理論的保証
2. **リスク調整**: 勝率・リターンに基づく適切な配分
3. **実運用可能**: 統合ポートフォリオ全体の統計を使用
4. **柔軟性**: Kelly係数でリスク許容度を調整

#### 使い分け

- **従来手法**: シンプル・ポジション数ベース（基本戦略向け）
- **ケリー基準**: 高度・期待値ベース・数学的最適化（複雑戦略向け）

## 実運用シナリオ例

### バックテスト結果

```
戦略: MACD Cross v2
対象銘柄: 398銘柄（TOPIX500）
期間: 2020-2024

第1段階（均等配分）:
- 配分率: 1/398 = 0.25%
- 総リターン: +45%
- 統合ポートフォリオ統計:
  - 勝率: 55%
  - 平均勝ちトレード: +12%
  - 平均負けトレード: -6%
  - 全トレード数: 1,247件

ケリー基準計算:
- オッズ比 b = 12 / 6 = 2.0
- Full Kelly = (0.55 × 2.0 - 0.45) / 2.0 = 27.5%
- Half Kelly = 13.75%

第2段階（ケリー最適化）:
- 配分率: 13.75%（各銘柄）
- 総リターン: +120%
- 改善倍率: 2.67x
```

### 実運用での使い方

```python
# バックテスト結果
optimized_allocation = 0.1375  # 13.75%

# 実運用
def execute_strategy():
    signals = strategy.generate_signals()  # 戦略ロジックでシグナル生成

    for stock_code, signal in signals.items():
        if signal.entry:
            # シグナルが出た銘柄に13.75%配分
            position_size = available_capital * optimized_allocation
            buy(stock_code, position_size)

        if signal.exit:
            # エグジットシグナルで売却
            sell(stock_code)
```

**重要ポイント**:
- どの銘柄にシグナルが出るかは戦略ロジックが実行時に決定
- 配分率（13.75%）はバックテストで事前に決定
- 未来予知は不要（実運用可能）

## エッジケース処理

### 勝率0%（すべて負けトレード）

```python
# 負のケリー値 → 最小配分
if kelly_value < 0:
    allocation = min_allocation
```

### 勝率100%（すべて勝ちトレード）

```python
# 平均負けトレードが0 → ケリー値=勝率
if avg_loss == 0:
    kelly = win_rate
```

### トレードなし

```python
# トレード数0 → 均等配分
if total_trades == 0:
    allocation = 1.0 / len(stock_codes)
```

## テスト

### ユニットテスト実行

```bash
# ケリー基準ミックスインテスト実行
uv run pytest tests/unit/strategies/mixins/test_portfolio_analyzer_mixin_kelly.py -v

# 全テスト実行
uv run pytest tests/unit/strategies/mixins/ -v
```

### テストカバレッジ

- ✅ 統合ポートフォリオケリー基準計算の正確性
- ✅ 配分率制約（min/max）
- ✅ エッジケース処理（勝率0%, 100%, トレードなし）
- ✅ Half Kelly係数
- ✅ エラーハンドリング
- ✅ 2段階最適化統合
- ✅ 配分率出力形式（単一float値）

## パフォーマンス比較例

### ケース1: 高勝率戦略

**統合ポートフォリオ統計**: 勝率70%, 平均勝ち+15%, 平均負け-5%
```
b = 15 / 5 = 3.0
Kelly = (0.7 × 3.0 - 0.3) / 3.0 = 0.60
Half Kelly = 0.30 → 各銘柄に30%配分
```

### ケース2: 標準的戦略

**統合ポートフォリオ統計**: 勝率55%, 平均勝ち+12%, 平均負け-6%
```
b = 12 / 6 = 2.0
Kelly = (0.55 × 2.0 - 0.45) / 2.0 = 0.275
Half Kelly = 0.1375 → 各銘柄に13.75%配分
```

### ケース3: 低勝率戦略

**統合ポートフォリオ統計**: 勝率45%, 平均勝ち+10%, 平均負け-8%
```
b = 10 / 8 = 1.25
Kelly = (0.45 × 1.25 - 0.55) / 1.25 = -0.01
Half Kelly = -0.005 → 最小配分（この戦略は不採用）
```

## 推奨設定

| パラメータ | 推奨値 | 理由 |
|-----------|-------|------|
| `kelly_fraction` | 0.5 | リスク軽減（Half Kelly） |
| `min_allocation` | 0.01-0.05 | 最小リスク管理 |
| `max_allocation` | 0.3-0.5 | 過剰集中防止 |

## まとめ

**ケリー基準ポートフォリオ最適化**は、統合ポートフォリオ全体の統計から実運用可能な配分率を算出します。

### 実装の正しさ

- ✅ **統合ポートフォリオ全体**の統計を使用
- ✅ **実運用可能**: 未来予知不要
- ✅ **数学的根拠**: 長期的資産最大化
- ✅ **リスク管理**: Kelly係数で調整可能

### 実運用フロー

1. **バックテスト**: 統合ポートフォリオ全体からケリー基準を計算
2. **結果**: 「この戦略には14%配分が最適」と判明
3. **実運用**: シグナルが出た銘柄に14%ずつ投資
4. **銘柄選択**: 戦略ロジックが実行時に決定（未来予知不要）

### 次のステップ

1. **実戦テスト**: 実際の戦略で2段階最適化を実行
2. **パラメータ調整**: kelly_fraction/min/max_allocationの最適化
3. **パフォーマンス比較**: 従来手法との詳細比較
4. **リスク分析**: ドローダウン・ボラティリティの評価

---

**関連ドキュメント**:
- [VectorBT ポートフォリオ最適化ガイド](./vectorbt/portfolio-optimization.md)
- [戦略ミックスイン一覧](../src/strategies/core/mixins/README.md)
- [バックテスト実行ガイド](./commands.md)
