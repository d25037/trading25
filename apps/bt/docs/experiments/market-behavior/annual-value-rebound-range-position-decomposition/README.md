# Annual Value Rebound / Range Position Decomposition

[`annual-value-technical-feature-importance`](../annual-value-technical-feature-importance/README.md)
で強く出た `rebound_from_252d_low_pct` が、本当に「252日安値からの反発率」なのか、それとも「252日 high-low range 内の位置」を proxy しているだけなのかを切り分ける研究。

## Published Readout

### Decision

`rebound_from_252d_low_pct` は `range_position_252d` と同じものとして扱わない。今回の結果では、`rebound_from_252d_low_pct` は walk-forward overlay で両 score method とも平均 return を約 `+9.8pp` 改善した一方、`range_position_252d` は平均 return を約 `-1.9pp` 悪化させた。したがって次の additive technical score 研究では、`rebound_from_252d_low_pct` を独立 candidate とし、`range_position_252d` は優先度を下げる。

ただし、`rebound_from_252d_low_pct` は p10 を改善していないため、left-tail pruning ではなく「反転進捗 / rebound capture」候補として扱う。

### Why This Research Was Run

`rebound_from_252d_low_pct = (close / low_252 - 1) * 100` は直感的には「直近252日の値幅内でどの位置にいるか」に見えやすい。しかし range 内位置は `range_position_252d = (close - low_252) / (high_252 - low_252)` で、分母に `high_252 - low_252` を使う別物。両者が同じ signal を見ているだけなのかを、同じ annual value selection 上で比較した。

### Data Scope / PIT Assumptions

入力は positive-ratio value bundle `/tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive/`。分析対象は `standard` / `none` / top `10%` / `equal_weight`, `walkforward_regression_weight`。technical feature は `entry_date` より前の最新 trading session だけで計算する。

今回の rerun では既存 `annual-value-technical-feature-importance` runner に `range_position_252d` を追加し、以下3 featureを比較した。

| Feature | Definition |
| --- | --- |
| `rebound_from_252d_low_pct` | `(close / low_252 - 1) * 100` |
| `range_position_252d` | `(close - low_252) / (high_252 - low_252)` |
| `drawdown_from_252d_high_pct` | `(close / high_252 - 1) * 100` |

### Main Findings

#### 結論

| Feature | Avg delta mean | Avg delta p10 | Avg delta worst | Avg kept | Selected side | 読み |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `rebound_from_252d_low_pct` | `+9.82pp` | `-0.66pp` | `+6.80pp` | `18.9%` | high / high | 平均 return 改善は明確。左尾改善は弱い |
| `drawdown_from_252d_high_pct` | `+1.36pp` | `-2.03pp` | `+3.74pp` | `19.2%` | low / low | 高値から大きく下にいる側が少し良いが弱い |
| `range_position_252d` | `-1.88pp` | `-0.22pp` | `+3.74pp` | `19.3%` | low / low | range 内位置そのものは OOS で弱い |

#### Static Bucket

| Score | Feature | High-low mean | High-low p10 | Best bucket | Best-worst mean | Importance |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `equal_weight` | `rebound_from_252d_low_pct` | `+14.21pp` | `-6.40pp` | `5` | `+14.44pp` | `11.84` |
| `equal_weight` | `range_position_252d` | `+7.92pp` | `-0.13pp` | `5` | `+7.92pp` | `6.16` |
| `equal_weight` | `drawdown_from_252d_high_pct` | `+0.86pp` | `+0.40pp` | `2` | `+7.51pp` | `4.58` |
| `walkforward_regression_weight` | `rebound_from_252d_low_pct` | `+11.76pp` | `-6.92pp` | `5` | `+14.60pp` | `11.22` |
| `walkforward_regression_weight` | `range_position_252d` | `+4.76pp` | `-2.06pp` | `5` | `+5.35pp` | `5.19` |
| `walkforward_regression_weight` | `drawdown_from_252d_high_pct` | `-0.08pp` | `-1.83pp` | `2` | `+7.88pp` | `4.49` |

Static bucket では `range_position_252d` も high bucket が良く見えるが、walk-forward では過去年の best bucket 選択が low side に寄り、OOS mean は悪化した。つまり `range_position_252d` は同じ方向で安定していない。

#### Feature Correlation

| Score | `rebound` vs `range_position` | `rebound` vs `drawdown` | `range_position` vs `drawdown` |
| --- | ---: | ---: | ---: |
| `equal_weight` | `0.287` | `0.142` | `0.779` |
| `walkforward_regression_weight` | `0.296` | `0.152` | `0.775` |

`rebound_from_252d_low_pct` と `range_position_252d` の相関は約 `0.29` で低い。逆に `range_position_252d` は `drawdown_from_252d_high_pct` と約 `0.78` 相関している。つまり range 内位置は「安値からの反発率」よりも「高値からどれくらい離れているか」に近い特徴として振る舞っている。

### Interpretation

今回の結果から見ると、前回強かったのは「252日レンジの中で上の方にいること」ではなく、「252日安値を基準にした反発率が高いこと」。これは、極端に売られた value 銘柄が安値から明確に戻り始めたケースを拾っている可能性が高い。

一方で `range_position_252d` は、直近高値がどこにあったかに強く影響される。過去1年に大きな spike high がある銘柄では、現在 price が安値からかなり戻っていても range position は低く見える。この違いが、`rebound` と `range_position` の低相関につながっている。

`drawdown_from_252d_high_pct` は左尾を少し抑える可能性はあるが、平均 return 改善は弱い。反転銘柄選択の主 feature としては `rebound_from_252d_low_pct` の方が明確。

### Production Implication

次の additive technical score 研究では、`rebound_from_252d_low_pct` を採用候補に残し、`range_position_252d` は比較用に留める。Ranking 表示では、ユーザーに誤解が出ないように `rebound_from_252d_low_pct` を「252日安値からの反発率」、`range_position_252d` を「252日レンジ内位置」と明確に分けて表示する。

単独 hard filter にはしない。`rebound_from_252d_low_pct` は mean 改善が強い一方で p10 は改善していないため、left-tail pruning ではなく right-tail capture / rebound progress feature として使う。

### Caveats

`rebound_from_252d_low_pct` は low price 銘柄や極端な安値を含む銘柄で大きな値になりうる。今回の static bucket でも max は `1831%` まで出ているため、実装時は winsorize / rank transform を前提にする。年次 rebalance 研究なので、年中の signal 更新、約定コスト、position sizing、capacity は未反映。

### Source Artifacts

- Domain: `apps/bt/src/domains/analytics/annual_value_technical_feature_importance.py`
- Runner: `apps/bt/scripts/research/run_annual_value_technical_feature_importance.py`
- Bundle: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_rebound_range_position_decomposition/`
- Results DB: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_rebound_range_position_decomposition/results.duckdb`
- Summary: `/tmp/trading25-research/market-behavior/annual-value-technical-feature-importance/20260502_value_rebound_range_position_decomposition/summary.md`

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_value_technical_feature_importance.py \
  --input-bundle /tmp/trading25-research/market-behavior/annual-value-composite-selection/20260502_share_basis_positive \
  --output-root /tmp/trading25-research \
  --run-id 20260502_value_rebound_range_position_decomposition
```
