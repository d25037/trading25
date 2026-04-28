# TOPIX Downside Return Standard Deviation Family Committee Walk-Forward

TOPIX long-only book に対して、downside return standard deviation overlay の同一 family 内で committee 化が rank instability をどこまで緩和するかを確認した研究です。

## Published Readout

### Decision

この研究は candidate 昇格ではなく、downside stddev family の diagnostic として残す。committee size `5` は validation Sharpe win rate `58%`、平均 max drawdown improvement `+5.21pp` を出したが、平均 CAGR excess は `+0.26pp` と薄く、fold 間の rank stability も弱い。単独 production rule ではなく、後続の confirmation layer を PIT-safe に作り直すための baseline として使う。

### Why This Research Was Run

先行研究 `topix-downside-return-standard-deviation-exposure-timing` では、downside stddev overlay が通常 stddev より目的に合う一方、単一 parameter をそのまま採用するには selection instability が残った。この研究では同一 downside stddev family の上位候補を equal-weight committee にし、single-point parameter selection より安定するかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。対象は TOPIX long-only overlay。downside stddev window は `5`、mean windows は `1/2`、high thresholds は `0.22/0.24/0.25`、low thresholds は `0.05/0.10/0.15/0.20/0.22/0.24/0.25`、reduced exposure は `0/0.10`。family candidate count は `72`、committee sizes は `1/3/5`、walk-forward fold count は `12`。この run は TOPIX 単体の overlay family であり、TOPIX100 breadth proxy は使っていない。

### Main Findings

#### committee size 5 は drawdown を削るが、Sharpe / CAGR の上積みは薄い。

| Readout | Value |
| --- | ---: |
| Baseline validation Sharpe | `1.15` |
| Baseline validation CAGR | `+24.48%` |
| Baseline validation max drawdown | `-23.97%` |
| Best committee size by win rate | `5` |
| Sharpe win rate | `58%` |
| Mean validation Sharpe excess | `+0.07` |
| Mean validation CAGR excess | `+0.26pp` |
| Mean validation max drawdown improvement | `+5.21pp` |

#### fold 間の rank stability は弱く、top の exact parameter は信用しにくい。

| Stability lens | Sharpe rank | CAGR rank | Top overlap |
| --- | ---: | ---: | ---: |
| Single split Pearson | `0.11` | `0.48` | n/a |
| Single split Spearman | `0.06` | `0.39` | n/a |
| Single split top 10 overlap | n/a | n/a | `0.20` |
| Single split top 20 overlap | n/a | n/a | `0.20` |
| Single split top 50 overlap | n/a | n/a | `0.70` |
| Walk-forward mean Spearman Sharpe | `-0.00` | n/a | n/a |
| Walk-forward top 10 overlap | n/a | n/a | `0.16` |
| Walk-forward top 20 overlap | n/a | n/a | `0.21` |
| Walk-forward top 50 overlap | n/a | n/a | `0.71` |

### Interpretation

committee 化は drawdown 改善の方向では意味があるが、候補 family の上位 rank が安定しているとは言いにくい。top 10 / top 20 の overlap が低いため、best single parameter を採用する設計は危ない。一方で top 50 まで広げると overlap が残るので、広めの downside risk family は方向性として完全には壊れていない。

### Production Implication

production rule にはしない。使い道は、downside stddev overlay の「単独では弱いが、confirmation を足す価値はある」ことを示す baseline。次に進めるなら、trend / breadth / shock confirmation を PIT-safe universe で再設計し、committee の stability を改めて測る。

### Caveats

cost、slippage、tax、cash return、capacity は未評価。fold ごとの candidate rank は弱く、multiple testing の影響がある。TOPIX 単体 overlay なので、個別株 strategy にそのまま重ねた時の portfolio-level drawdown 改善は別途検証が必要。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_family_committee_walkforward.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_family_committee_walkforward.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-family-committee-walkforward/20260413_topix_downside_family_committee_walkforward`
- Tables: `results.duckdb`

## Purpose

- downside stddev family 内の parameter selection instability を測る。
- top-ranked member committee が TOPIX hold baseline を継続的に上回るか確認する。
- 後続の confirmation overlay の比較 baseline を作る。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_family_committee_walkforward.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_family_committee_walkforward.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-family-committee-walkforward/20260413_topix_downside_family_committee_walkforward`

## Current Read

- committee size `5` は drawdown 改善の diagnostic として残す。
- Sharpe / CAGR lift は薄く、単独 production candidate ではない。
- confirmation layer を PIT-safe に再設計する前提の baseline として扱う。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_downside_return_standard_deviation_family_committee_walkforward.py \
  --run-id 20260413_topix_downside_family_committee_walkforward
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

