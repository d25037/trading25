# TOPIX Downside Return Standard Deviation Shock Confirmation Committee Overlay

downside stddev shock confirmation を fixed family に絞り、single-point selection と equal-weight committee を比較した TOPIX exposure overlay 研究です。

## Published Readout

### Decision

この run は **future leak により invalidated**。headline では committee が validation Sharpe `1.50`、CAGR `+25.63%`、max drawdown `-13.86%` を出し、single-point より fold stability も良く見える。ただし breadth universe が current TOPIX100 membership proxy なので、performance 数値は candidate evidence として使わない。committee stabilization の設計案だけを残し、PIT-safe breadth universe で再実行する。

### Why This Research Was Run

先行研究 `topix-downside-return-standard-deviation-shock-confirmation-vote-overlay` では、family-level vote の発想は有用そうだが exact threshold selection が不安定だった。この研究では shock confirmation family を固定し、`mean x high` の single-point member selection と equal-weight committee を比較して、parameter instability を committee が緩和できるかを確認した。

### Data Scope / PIT Assumptions

入力は local market DB snapshot。TOPIX long-only overlay。standard deviation window は `5`、committee mean windows は `1/2`、committee high thresholds は `0.24/0.25`、low thresholds は `0.20/0.22`、reduced exposure は `0`。trend family は `close_below_sma20`、`sma20_below_sma60`、`drawdown_63d_le_neg0p05`、`return_10d_le_neg0p03`。breadth family は `topix100_above_sma20_le_0p40`、`topix100_positive_5d_le_0p40`、`topix100_at_20d_low_ge_0p20`。trend vote thresholds は `1/2/3`、breadth vote threshold は `3`、confirmation mode は `stress_and_trend_and_breadth`。single-point candidates は `24`、committee candidates は `6`、walk-forward fold count は `11`。

PIT 上の blocker は TOPIX100 breadth universe。historical TOPIX100 constituents ではなく current proxy を全過去日に適用しているため、universe-level future leak がある。

### Main Findings

#### headline は最も強いが、breadth proxy leak により candidate evidence ではない。

| Item | 旧 readout の見え方 | 現在の扱い |
| --- | --- | --- |
| Best committee validation Sharpe | `1.50` vs baseline `1.08` | contaminated universe のため invalid |
| Best committee validation CAGR | `+25.63%` vs baseline `+23.08%` | contaminated universe のため invalid |
| Best committee max drawdown | `-13.86%` vs baseline `-23.97%` | contaminated universe のため invalid |
| Committee stability | single-point より改善 | design clue のみ。performance evidence ではない |

#### contaminated headline では committee が single point より drawdown と stability を改善した。

| Space | Best candidate | Validation Sharpe | Validation CAGR | Max drawdown | Discovery Sharpe |
| --- | --- | ---: | ---: | ---: | ---: |
| Baseline | `100% TOPIX` | `1.08` | `+23.08%` | `-23.97%` | n/a |
| Single point | `std5_mean1_hi0p25_lo0p22_reduced0_trendvotes1_breadthvotes3_mode_stress_and_trend_and_breadth` | `1.47` | `+25.53%` | `-16.15%` | `0.65` |
| Committee | `committee_m1-2_h0p24-0p25_low0p22_reduced0_trendvotes1_breadthvotes3_mode_stress_and_trend_and_breadth` | `1.50` | `+25.63%` | `-13.86%` | `0.59` |

#### committee space は fold stability を上げたが、leak のため昇格できない。

| Metric | Single point | Committee |
| --- | ---: | ---: |
| Fold count | `11` | `11` |
| Mean fold Spearman Sharpe | `0.31` | `0.64` |
| Mean top 3 overlap | `0.30` | `0.73` |
| Mean top 5 overlap | `0.31` | `0.95` |
| Validation Sharpe win rate | `55%` | `64%` |
| Mean validation Sharpe excess | `+0.25` | `+0.23` |
| Mean validation CAGR excess | `+4.50pp` | `+3.96pp` |
| Mean validation max drawdown improvement | `+6.27pp` | `+6.54pp` |

### Interpretation

committee 化は exact parameter の不安定さを減らす設計としては有望に見える。特に top 5 overlap `0.95` は、single-point の `0.31` よりかなり安定している。ただし、breadth signal の母集団が future contaminated なので、この stability が本当に robust な overlay family を示しているのか、current TOPIX100 universe に依存した見かけなのかを分離できない。

### Production Implication

production / ranking / screening には使わない。次段は同じ committee idea を date-effective TOPIX100 membership または PIT-safe 代替 universe で再実行すること。再実行後に、committee が drawdown control と false de-risk reduction を保てる場合だけ、downside stddev overlay candidate として再検討する。

### Caveats

これは通常の caveat ではなく **P0 invalidation**。current TOPIX100 membership proxy を過去に固定した future leak がある。cost、slippage、tax、cash return、capacity、multiple testing、leverage read は未評価だが、まず universe contamination を解消する必要がある。

### Source Artifacts

- Runner: `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Domain logic: `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Bundle: `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-shock-confirmation-committee-overlay/20260413_topix_shock_confirmation_committee_overlay`
- Tables: `results.duckdb`

## Purpose

- fixed shock confirmation family で single-point と committee を比較する。
- committee が parameter instability を緩和するかを見る。
- TOPIX exposure overlay として drawdown を削れるかを評価する。

## Source Of Truth

- Runner:
  - `apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Domain logic:
  - `apps/bt/src/domains/analytics/topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py`
- Bundle:
  - `~/.local/share/trading25/research/market-behavior/topix-downside-return-standard-deviation-shock-confirmation-committee-overlay/20260413_topix_shock_confirmation_committee_overlay`

## Current Read

- future leak により invalidated。
- committee stabilization の設計案だけを残し、performance 数値は使わない。
- PIT-safe breadth universe で完全に再実行する。

## Reproduction

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run --project apps/bt python \
  apps/bt/scripts/research/run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay.py \
  --run-id 20260413_topix_shock_confirmation_committee_overlay
```

結果確認は runner が出力する bundle の `summary.md` と `results.duckdb` を参照します。
