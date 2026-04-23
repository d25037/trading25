# Annual Fundamental Confounder Analysis

先行研究
[`annual-first-open-last-close-fundamental-panel`](../annual-first-open-last-close-fundamental-panel/README.md)
の `event_ledger_df` を入力に、ファンダメンタル指標同士の交絡と独立効果を
統計的に確認する研究。

## Current Surface

- Domain:
  - `apps/bt/src/domains/analytics/annual_fundamental_confounder_analysis.py`
- Runner:
  - `apps/bt/scripts/research/run_annual_fundamental_confounder_analysis.py`
- Bundle:
  - `manifest.json`
  - `results.duckdb`
  - `summary.md`
  - `summary.json`

## Design

- Input: annual first-open/last-close fundamental panel bundle.
- Objective: `event_return_pct` を `1% / 99%` winsorize した
  `event_return_winsor_pct`。
- Factor score:
  - 各 factor を `year x current market` 内 percentile score に変換する。
  - score は常に「高いほど仮説上よい方向」。
  - 例: `low_pbr_score` は PBR が低いほど高く、`small_market_cap_score`
    は時価総額が小さいほど高い。
- Statistical views:
  - factor coverage
  - Spearman correlation
  - VIF
  - pairwise conditional Q5-Q1 spread
  - year / market / sector fixed-effect OLS
  - Fama-MacBeth style yearly cross-sectional regression
  - leave-one-year-out coefficient stability
  - incremental selection tests

## Outputs

- `prepared_panel_df`: realized event panel with winsorized return and factor scores.
- `factor_coverage_df`: factor coverage by market scope.
- `feature_correlation_df`: pairwise Spearman correlations.
- `vif_df`: factor-level variance inflation diagnostics.
- `conditional_spread_df`: target Q5-Q1 spread inside confounder Q1/Q5.
- `panel_regression_df`: fixed-effect OLS coefficients with HC1 robust SE.
- `fama_macbeth_df`: yearly cross-sectional coefficient stability.
- `leave_one_year_out_df`: coefficient stability after excluding each year.
- `incremental_selection_df`: rule-level annual event-return summary.

## Current Findings

Baseline result: [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)

- Low `PBR`, small market cap, and low `forward PER` remained independent after
  controlling for each other plus year / market / sector fixed effects.
- Low `ADV60` did not survive as a clean independent factor. In `prime`, its
  coefficient turned strongly negative after controlling for market cap, which
  means the earlier low-ADV edge mostly overlapped with small-cap exposure.
- Forecast dividend yield and dividend yield were almost the same signal
  statistically, so they should not both be used as independent factors in a
  compact model.
- `forward EPS / actual EPS` stayed weak. It had a small all-market OLS
  coefficient, but Fama-MacBeth stability was poor and `standard` was near zero.

## Run

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_fundamental_confounder_analysis.py \
  --output-root /tmp/trading25-research
```

出力先:

`/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/<run_id>/`

## Caveats

- This is statistical association, not causal proof.
- Market split still uses the current `stocks` snapshot from the upstream
  annual panel.
- Year count is only `2017-2025`; ordinary p-values are secondary to coefficient
  sign stability and leave-one-year-out behavior.
- Incremental selection rows are event-return summaries, not capacity-checked
  executable portfolio simulations.

