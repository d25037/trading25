# TOPIX Downside Return Standard Deviation Trend Breadth Overlay

TOPIX downside stddev shock に TOPIX trend と TOPIX100 breadth confirmation を重ね、risk-off transition を絞る overlay 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。headline では validation Sharpe `1.42`、CAGR `+24.57%`、max drawdown `-17.15%` と TOPIX hold を上回る候補が出たが、breadth universe が current `stocks.scale_category` に基づく `latest_scale_category_proxy` であり、過去日に将来確定の TOPIX100 membership を混ぜている。production / candidate 判断には使わず、PIT-safe breadth universe で再実行する。

### Why This Research Was Run

先行研究 `topix-downside-return-standard-deviation-exposure-timing` と `topix-downside-return-standard-deviation-family-committee-walkforward` では、downside stddev は drawdown control に効くが、単独では false de-risk と rank instability が残った。この研究では risk-off 条件を downside stddev だけでなく、弱い TOPIX trend と弱い TOPIX100 breadth で確認し、不要な exposure reduction を減らせるかを試した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。対象は TOPIX long-only overlay。downside stddev window は `5`、mean windows は `1/2`、high thresholds は `0.22/0.24/0.25`、low thresholds は `0.05/0.10/0.15/0.20/0.22/0.24/0.25`、reduced exposure は `0/0.10`。trend rules は `close_below_sma20`、`sma20_below_sma60`、`drawdown_63d_le_neg0p05`、`return_10d_le_neg0p03`。breadth rules は `topix100_above_sma20_le_0p40`、`topix100_positive_5d_le_0p40`、`topix100_at_20d_low_ge_0p20`。candidate count は `864`、walk-forward fold count は `11`。

PIT 上の blocker は breadth universe。historical constituents が market DB にないため、current scale-category snapshot を TOPIX100 proxy として過去全期間に適用している。これは universe-level future leak であり、walk-forward split では無効化できない。

### Main Findings

#### breadth proxy leak があるため、headline improvement は evidence として使えない。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Best candidate validation Sharpe | `1.42` vs baseline `1.08` | contaminated universe のため invalid |
| Best candidate validation CAGR | `+24.57%` vs baseline `+23.08%` | contaminated universe のため invalid |
| Best candidate max drawdown | `-17.15%` vs baseline `-23.97%` | contaminated universe のため invalid |
| Candidate status | trend + breadth confirmation candidate | candidate から除外し、PIT-safe rerun 待ち |

#### 旧 headline 候補は drawdown を削ったが、ranking stability は弱い。

| Lens | Value |
| --- | ---: |
| Candidate | `std5_mean1_hi0p25_lo0p22_reduced0_trend_return_10d_le_neg0p03_breadth_topix100_above_sma20_le_0p40` |
| Discovery Sharpe | `0.67` |
| Validation Sharpe | `1.42` |
| Validation CAGR | `+24.57%` |
| Validation max drawdown | `-17.15%` |
| Walk-forward mean Spearman Sharpe | `0.05` |
| Walk-forward top 10 overlap | `0.01` |
| Walk-forward top 20 overlap | `0.03` |
| Walk-forward top 50 overlap | `0.05` |

#### walk-forward top1 は平均では drawdown を改善するが、勝率は十分ではない。

| Metric | Value |
| --- | ---: |
| Fold count | `11` |
| Validation Sharpe win rate | `45%` |
| Mean validation Sharpe excess | `+0.08` |
| Mean validation CAGR excess | `+0.97pp` |
| Mean validation max drawdown improvement | `+5.13pp` |

### Interpretation

trend + breadth confirmation の設計意図は妥当だが、この run の TOPIX100 breadth は PIT-safe ではない。結果として、headline 候補の drawdown 改善が本当に market breadth の情報なのか、current TOPIX100 membership / survivorship bias なのかを分離できない。さらに、leak を無視しても top overlap が低く、exact rule selection は不安定。

### Production Implication

production / ranking / screening には使わない。次にやるべきことは parameter の微修正ではなく、date-effective TOPIX100 membership または PIT-safe 代替 universe で breadth feature を作り直すこと。再実行後にだけ、trend + breadth confirmation を downside stddev overlay の候補として再評価する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。current TOPIX100 membership proxy を過去に固定した future leak がある。cost、slippage、tax、cash return、capacity、multiple testing、fold overlap より先に、この universe contamination を解消する必要がある。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_trend_breadth_overlay.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-trend-breadth-overlay/20260413_topix_downside_trend_breadth_overlay_baseline`
- Tables: `results.duckdb`

## Purpose

- downside stddev shock を trend / breadth で確認し、false de-risk を減らせるかを見る。
- TOPIX100 breadth を risk-off confirmation として使えるかを評価する。
- 後続の vote / committee overlay の baseline を作る。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_trend_breadth_overlay.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_trend_breadth_overlay.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-trend-breadth-overlay/20260413_topix_downside_trend_breadth_overlay_baseline`

## Current Read

- future leak により invalidated。
- trend + breadth confirmation の発想だけを残し、performance 数値は使わない。
- PIT-safe breadth universe で完全に再実行する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_downside_return_standard_deviation_trend_breadth_overlay.py \
  --run-id 20260413_topix_downside_trend_breadth_overlay_baseline
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。

