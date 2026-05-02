# Margin Balance Supply/Demand

## Published Readout

### Decision

Phase 1 は `market.duckdb.margin_data` に既に入っている J-Quants `/markets/margin-interest` 由来の信用買い残・信用売り残だけを検証する。機関投資家の空売り残高報告はこの研究には含めない。まず aggregate な信用残高が単独で return 改善や bad-tail pruning に使えるかを、PIT-safe な effective date にずらして測る。

### Main Findings

#### 結論

Bundle `20260502_221038_c9ee3dfd`（2016-05-18 から 2026-04-30、1,652,401 observations、3,754 codes）の validation では、信用買い残 overhang をそのまま除外軸にする結果は弱い。一方、`short_to_adv20` 高位 bucket の除外は 1 / 5 / 10 / 20 sessions の平均 return を小幅に改善したが、severe-loss rate は 20 sessions 以外で悪化または横ばいに近く、単独 production filter としてはまだ不十分。

| Horizon | Validation top candidate | Baseline mean return | Retained mean return | Delta | Severe-loss delta |
|---|---|---:|---:|---:|---:|
| 1d | `exclude_high_short_to_adv20` | -0.0226% | -0.0166% | +0.0060pt | +0.0173pt |
| 5d | `exclude_low_long_short_ratio` | 0.1619% | 0.1929% | +0.0310pt | +0.1483pt |
| 10d | `exclude_high_short_to_adv20` | 0.5437% | 0.6187% | +0.0750pt | +0.0140pt |
| 20d | `exclude_high_short_to_adv20` | 1.2029% | 1.3457% | +0.1428pt | -0.1004pt |

#### 信用買い残 overhang の確認

| Candidate | 1d delta | 5d delta | 10d delta | 20d delta | 解釈 |
|---|---:|---:|---:|---:|---|
| `exclude_high_long_to_adv20` | -0.0102pt | -0.0157pt | -0.0661pt | -0.0868pt | 高買い残を単純除外すると validation 平均 return は悪化 |
| `exclude_high_net_to_adv20` | -0.0099pt | -0.0167pt | -0.0676pt | -0.0901pt | ネット買い残も単純除外では弱い |
| `exclude_high_long_percentile_52w` | -0.0003pt | -0.0132pt | -0.0191pt | -0.0711pt | rolling percentile でも改善しない |
| `exclude_high_long_weekly_change_pct` | +0.0028pt | -0.0041pt | +0.0037pt | -0.0405pt | 短期増加は方向が不安定 |

### Interpretation

`short_margin_volume` は信用取引の売り残であり、JPX/J-Quants の機関投資家別空売り残高報告ではない。このため、Phase 1 の解釈は「個人を含む信用取引需給 proxy」に留める。信用買い残の多さ、信用売り残の多さ、買い残増加、ネット買い残、買い残 percentile を同じ cross-section 内で bucket 化し、翌 1 / 5 / 10 / 20 trading sessions の open-to-close return を比較する。

今回の validation 結果では、ユーザー仮説に近い「信用買い残が重い銘柄を除く」方向は、少なくとも aggregate weekly margin balance 単独では確認できなかった。信用売り残高位の除外は平均 return の改善だけを見ると候補に見えるが、これは「踏み上げ候補を捨てている」可能性もあり、severe-loss / portfolio lens / strategy overlay なしに採用しない。

### Production Implication

この研究だけで production rule は作らない。特に信用買い残 overhang は単独除外軸として採用しない。信用売り残高位 bucket の除外は平均 return 改善候補として残すが、severe-loss 改善が不安定なので、既存 production strategy への overlay は別研究で portfolio lens を確認してから判断する。

### Caveats

- 週次信用残は週末時点データが翌週に公表されるため、record date そのものでは売買判断できない。
- `effective_lag_sessions=3` は通常週の「金曜残高 -> 翌週第2営業日夕方公表 -> その翌営業日寄り付き」を保守的にモデル化する。
- `stocks` の market classification は現行 snapshot に基づく診断列であり、historical market migration の厳密な PIT split ではない。
- 株式分割後の過去信用残は再調整されないため、raw 株数の単純比較ではなく ADV 正規化を優先する。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_margin_balance_supply_demand.py`
- Domain: `apps/bt/src/domains/analytics/margin_balance_supply_demand.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/margin-balance-supply-demand/20260502_221038_c9ee3dfd/`
- Tables: `coverage_summary_df`, `bucket_return_summary_df`, `pruning_summary_df`, `market_summary_df`, `observation_df`

## Runbook

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_margin_balance_supply_demand.py
```

Useful narrower run:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_margin_balance_supply_demand.py \
  --start-date 2018-01-01 \
  --horizons 1,5,10,20
```
