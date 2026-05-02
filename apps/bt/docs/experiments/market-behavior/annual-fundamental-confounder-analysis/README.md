# Annual Fundamental Confounder Analysis

先行研究
[`annual-first-open-last-close-fundamental-panel`](../annual-first-open-last-close-fundamental-panel/README.md)
の `event_ledger_df` を入力に、ファンダメンタル指標同士の交絡と独立効果を
統計的に確認する研究。

## Published Readout

### Decision

低 `PBR`、小型、低 `forward PER` は v3 PIT stock-master + statement-document semantics rerun でも独立性が残る。`ADV60` は alpha ではなく capacity / execution diagnostic として扱う。`forward EPS / actual EPS` は補助情報に留め、主スコアへ強く入れない。

### Why This Research Was Run

annual panel の単独 factor spread が、低 `PBR`・小型・低 `forward PER` の相互相関だけで説明できるのか、また positive ratio 条件で distressed denominator を除くと結論が変わるのかを確認した。

### Data Scope / PIT Assumptions

入力は v3 parent bundle `/tmp/trading25-research/market-behavior/annual-first-open-last-close-fundamental-panel/20260502_statement_doc_semantics/`。期間は `2017-2025`、input realized events は `32,264`。upstream は `stock_master_daily` の entry-date market membership と entry-date as-of FY fundamentals を使い、actual metrics は FY financial-statement documents から取る。default run は全 realized events、practical run は `PBR > 0` かつ `forward PER > 0` の `24,660` events。

### Main Findings

#### 結論

| Scope | Condition | low PBR | small cap | low forward PER | low ADV60 |
| --- | --- | ---: | ---: | ---: | ---: |
| `all` | default | `5.04pp / t=16.47` | `3.10pp / t=8.97` | `2.73pp / t=9.88` | `-1.54pp / t=-4.71` |
| `all` | positive ratios | `4.37pp / t=13.91` | `2.99pp / t=8.69` | `3.48pp / t=12.17` | `-1.49pp / t=-4.56` |
| `prime` | positive ratios | `2.98pp / t=7.70` | `3.83pp / t=6.52` | `3.72pp / t=10.53` | `-3.09pp / t=-5.68` |
| `standard` | positive ratios | `5.41pp / t=9.92` | `2.51pp / t=5.42` | `3.07pp / t=6.31` | `-0.68pp / t=-1.46` |
| `growth` | positive ratios | `7.19pp / t=5.20` | `6.04pp / t=3.77` | `2.91pp / t=2.34` | `-0.60pp / t=-0.39` |

#### 結論

| Incremental rule | Scope | Events | Mean return | Annual mean | Year t |
| --- | --- | ---: | ---: | ---: | ---: |
| `low_pbr_small_cap_low_forward_per` | `prime`, positive ratios | `706` | `23.78%` | `25.30%` | `3.04` |
| `low_pbr_small_cap_low_forward_per` | `standard`, positive ratios | `377` | `28.91%` | `32.17%` | `3.43` |
| `low_pbr_small_cap` | `standard`, positive ratios | `556` | `28.20%` | `29.43%` | `3.61` |

### Interpretation

Positive-ratio filtering does not overturn the main conclusion. The statement-document semantics fix increases the practical positive-ratio universe, but the market split remains clear: `prime` は小型 + 低 `forward PER` が厚く、`standard` は低 `PBR` が主役。`standard` では低 `PBR`、小型、低 `forward PER` が同時に残るので、composite selection の主戦場は引き続き `standard`。

### Production Implication

Ranking score は低 `PBR` + 小型 + 低 `forward PER` を中心にする。CFO yield や forecast dividend yield は補助・tie-breaker 候補で、`ADV60` は rank score ではなく capacity cap / execution warning に分離する。

### Caveats

これは association study であり、因果推定ではない。Market label は v3 `stock_master_daily` の entry-date membership だが、legacy JPX segment は current research label に collapse している。

### Source Artifacts

- Default bundle: `/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_default/`
- Positive-ratio bundle: `/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/20260502_statement_doc_semantics_positive/`
- Domain: `apps/bt/src/domains/analytics/annual_fundamental_confounder_analysis.py`
- Runner: `apps/bt/scripts/research/run_annual_fundamental_confounder_analysis.py`

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
  - 各 factor を `year x entry-date market` 内 percentile score に変換する。
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

Baseline results:

- [`baseline-2026-04-23.md`](./baseline-2026-04-23.md)
- [`baseline-2026-04-24.md`](./baseline-2026-04-24.md)
  (`PBR > 0` and `forward PER > 0` practical rerun)

- Low `PBR`, small market cap, and low `forward PER` remained independent after
  controlling for each other plus year / market / sector fixed effects.
- The positive-ratio-only rerun did not change the main result in `all`,
  `prime`, or `standard`. It mainly removed distressed negative-denominator
  names and made the `growth` low-`forward PER` effect disappear.
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

Practical rerun:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_annual_fundamental_confounder_analysis.py \
  --output-root /tmp/trading25-research \
  --require-positive-pbr-and-forward-per
```

出力先:

`/tmp/trading25-research/market-behavior/annual-fundamental-confounder-analysis/<run_id>/`

## Caveats

- This is statistical association, not causal proof.
- Market split uses the upstream annual panel's entry-date `stock_master_daily`
  membership in the v3 rerun.
- Year count is only `2017-2025`; ordinary p-values are secondary to coefficient
  sign stability and leave-one-year-out behavior.
- Incremental selection rows are event-return summaries, not capacity-checked
  executable portfolio simulations.
