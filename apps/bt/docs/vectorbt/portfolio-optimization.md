# VectorBT ポートフォリオ最適化ガイド

## 多資産ポートフォリオの正しい設定

### 問題の本質
VectorBT `cash_sharing=True` + `group_by=True`使用時の誤った理解による取引数激減問題

- ❌ 間違い: `init_cash = total_cash / N` （各銘柄に1/N配分）
- ✅ 正解: `init_cash = total_cash` （共有キャッシュプール全体）

### 正しい実装パターン

```python
portfolio = vbt.Portfolio.from_signals(
    close=close_data,
    entries=all_entries,
    exits=all_exits,
    init_cash=self.initial_cash,  # 🔧 共有キャッシュプール全体
    size=allocation_per_asset,   # 🆕 各銘柄への配分率(1/N)
    size_type='percent',         # 🆕 パーセント指定
    cash_sharing=True,           # 資金共有有効
    group_by=True,              # 統合ポートフォリオ
    call_seq='auto',            # 🆕 実行順序最適化
    freq="D"
)
```

### パラメータの意味

- **`cash_sharing=True`**: 全銘柄が共有キャッシュプールから資金調達
- **`size=1/N, size_type='percent'`**: 各取引で総資産の1/N%を使用
- **`call_seq='auto'`**: 資金制約下で最適な実行順序を自動決定

### 効果の比較

- **修正前**: 10万円÷5銘柄=2万円/銘柄 → ほぼ取引不可
- **修正後**: 10万円共有プール、各取引20%配分 → 活発取引（1900+取引）

## Kelly基準による2段階最適化システム

### 概要
統合ポートフォリオ全体のトレード統計から、Kelly基準により最適配分率を計算する2段階最適化システム

### 実装場所
- `src/strategies/core/mixins/portfolio_analyzer_mixin_kelly.py`
  - `optimize_allocation_kelly()`: Kelly基準配分率計算
  - `run_optimized_backtest_kelly()`: Kelly基準2段階最適化実行
- `src/strategies/core/mixins/backtest_executor_mixin.py`
  - `run_optimized_backtest()`: Kelly基準統一インターフェース

### Kelly基準最適化フロー

1. **第1段階**: N銘柄均等配分（1/N）で探索実行
2. **Kelly基準計算**: 統合ポートフォリオ全体の勝率・平均損益比からKelly値算出
3. **第2段階**: Kelly基準配分率（f*×kelly_fraction）で最適実行

### Kelly基準パラメータ

```yaml
# config/default.yaml
kelly_fraction: 0.5    # ケリー係数（0.5=Half Kelly推奨、1.0=Full Kelly）
min_allocation: 0.01   # 最小配分率（1%）
max_allocation: 0.5    # 最大配分率（50%）
```

### 効果
- **統計ベース最適化**: 勝率・平均損益比に基づく科学的配分
- **リスク管理**: Half Kelly（f=0.5）による安全性重視の配分
- **動的配分**: 戦略のパフォーマンスに応じた最適配分率決定

詳細は [`docs/kelly-criterion-allocation.md`](../kelly-criterion-allocation.md) を参照