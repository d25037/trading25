# Ranking Trend Acceleration Conditional Lift Research Design

## Goal

Determine whether rolling log-price OLS trend features add useful priority ordering inside pre-existing Prime-equivalent long-candidate groups that already have positive-return evidence, without using the OLS features to select the research population.

The research may recommend additive Ranking columns or a derived badge. It does not change production Ranking behavior, replace the existing fixed 20D/60D semantics, or expose a new API field by itself.

## Standing Market Constraint

This research and subsequent research in this Ranking trend-priority line use only the point-in-time Prime-equivalent market universe:

- current Prime: market code `0111`;
- pre-2022-reorganization Prime proxy: market code `0101` (TSE First Section).

The implementation must resolve the `prime` scope through `MARKET_CODES_BY_SCOPE` in `apps/bt/src/shared/utils/market_code_alias.py`. Membership must come from `stock_master_daily` on the signal date. Current membership must never be projected backward. Standard and Growth are out of scope and must not appear in default arguments, aggregate rows, decision gates, or the Published Readout evidence tables.

## Research Question

Within long-candidate groups selected only by existing Ranking information, does trend acceleration provide incremental priority information for forward TOPIX-excess return?

The primary binary feature is:

```text
trend_acceleration_triple =
    price_lr_slope_20_pct > 0
    AND price_lr_slope_60_pct > 0
    AND price_lr_slope_20_pct > price_lr_slope_60_pct
```

The primary continuous feature is:

```text
trend_acceleration_margin_pct =
    price_lr_slope_20_pct - price_lr_slope_60_pct
```

`price_lr_slope_N_pct` retains the existing research definition: fit OLS to trailing adjusted `log(close)` against session index, then convert the per-session coefficient to the fitted move over the window with `(exp(beta * (N - 1)) - 1) * 100`. Therefore the margin compares fitted cumulative moves over different windows; it is not a direct comparison of raw per-session OLS coefficients.

## Candidate Population

Candidate selection must be independent of every OLS slope, OLS R², moving-average slope, and future-return field. Candidate definitions are frozen before outcome aggregation.

The research registry contains these production-aligned groups:

| Candidate group | Existing-only definition | Role |
| --- | --- | --- |
| `core_long` | `neutral_rerating` + Deep Value + ATR20 acceleration excluding overheat + liquidity residual z in `[-1, 2]` | Primary |
| `momentum_value` | `neutral_rerating` + Deep Value + existing `momentum_20_60_top20` + liquidity residual z in `[-1, 2]` | Primary |
| `neutral_rerating_good` | `neutral_rerating` + Deep Value | Broad sensitivity |
| `earnings_priority` | `core_long` + forecast operating profit growth ratio `>= 1.2` | Nested sensitivity; never counted as independent evidence |
| `aggressive_rerating` | `crowded_rerating` + Deep Value + ATR20 acceleration excluding overheat + liquidity residual z in `[1, 2]` | 20D secondary; 60D is diagnostic only |

Because these groups overlap, the adoption gate uses mutually exclusive evidence slices in addition to named-group readouts:

- `core_long_only`: `core_long AND NOT momentum_value`;
- `momentum_value_only`: `momentum_value AND NOT core_long`;
- `core_momentum_overlap`: `core_long AND momentum_value`, reported as a high-conviction sensitivity but not counted as an independent family;
- `aggressive_rerating`: already disjoint from neutral-regime groups;
- `neutral_good_remainder`: `neutral_rerating_good` excluding `core_long OR momentum_value`, used as a broad-family check.

The research must report named groups and mutually exclusive slices separately. It must not increase the apparent replication count by treating a parent and its nested child as independent successes.

## Point-in-Time and Timing Contract

- Signal date `X` includes data observable through date `X` close.
- Candidate membership, market membership, valuation, liquidity, sector, ATR, endpoint returns, and OLS features are resolved as of `X`.
- Forward outcomes start after `X`; features must not use any row later than `X`.
- Market membership uses exact-date `stock_master_daily` with the Prime-equivalent aliases above.
- Four-digit and five-digit symbol representations follow the existing Daily Ranking normalization and same-date deduplication rules.
- The feature is explicitly an after-close Ranking feature. Pre-open use is unsupported by this research.
- Appending future rows to a fixture must not change any feature, candidate assignment, or rank at an earlier cutoff.

## Time Segments

The fixed feature definitions are evaluated without threshold fitting in these segments:

| Segment | Dates | Purpose |
| --- | --- | --- |
| `historical_pre_reorg` | `2017-01-01` through `2021-12-31` | Historical replication using PIT TSE First Section membership |
| `historical_post_reorg` | `2022-01-01` through `2023-12-31` | Reorganization transition and early Prime replication |
| `recent_hypothesis_origin` | `2024-01-01` through latest complete signal date | Reproduce the period that motivated the hypothesis; not a holdout |

Year-level rows are also emitted. A conclusion cannot rely only on `recent_hypothesis_origin`.

## Comparisons

All primary comparisons are performed within candidate group and signal date before being aggregated across dates.

### Binary Conditional Lift

Compare `trend_acceleration_triple` with eligible non-triple candidates on the same date. A paired date is valid only when both sides contain at least two symbols. Report each side and the triple-minus-control spread for forward 5D, 20D, and 60D TOPIX-excess return.

### Incremental Lift Inside Fixed Momentum

Define:

```text
fixed_dual_positive = recent_return_20d_pct > 0
                      AND recent_return_60d_pct > 0
```

Within `fixed_dual_positive`, compare triple with non-triple candidates. Also report the full fixed/triple 2x2 table. This determines whether OLS acceleration adds information rather than merely reproducing existing fixed momentum.

### Continuous Ranking Lift

Within each candidate group and date with at least 20 eligible symbols:

- rank `trend_acceleration_margin_pct` cross-sectionally;
- form bottom 20%, middle 60%, and top 20% buckets;
- compute top-minus-bottom forward-return spread;
- compute daily Spearman IC against forward TOPIX-excess return;
- report median IC, mean IC, IC-positive date rate, and observation/date coverage.

No percentile may be computed against the full market when evaluating candidate-internal ordering.

### Operational Top-K Lift

For candidate dates with at least `2K` eligible symbols, compare the existing unordered candidate basket with lexicographic priority `trend_acceleration_triple DESC, trend_acceleration_margin_pct DESC` for `K=5` and `K=10`. Report return distribution, win rate, severe-loss rate, candidates per date, symbol turnover, and rank stability.

## Outcomes and Statistical Treatment

Primary horizon: 20D close-to-close TOPIX-excess return.

Supporting horizons:

- 5D for very-short entry timing;
- 60D as a hold-risk diagnostic, not as the primary adoption target.

Stock-day pooled statistics are descriptive only. The primary evidence is the time series of same-day paired spreads. Confidence intervals use a fixed-seed moving-block bootstrap with 2,000 resamples and block length equal to the forward horizon. Incomplete forward windows are excluded.

For every comparison report:

- observation, symbol, date, and paired-date counts;
- mean and median TOPIX-excess return;
- win rate;
- P10 and P25;
- severe-loss rate using the existing research threshold;
- bootstrap point estimate and 95% confidence interval.

## Decision Gates

### Add Continuous Ranking Columns

Recommend `priceLrSlope20Pct`, `priceLrSlope60Pct`, and `trendAccelerationMarginPct` only when all conditions hold:

1. After-warmup feature coverage is at least 95% in every primary mutually exclusive family.
2. For the 20D horizon, median daily Spearman IC is at least `0.02`.
3. IC is positive on at least 52% of eligible dates.
4. Top-minus-bottom same-day 20D lift is at least `+0.25` percentage points and its 95% moving-block-bootstrap lower bound is above zero in combined historical replication (`2017-2023`).
5. The same lift has a positive sign in `historical_pre_reorg`, `historical_post_reorg`, and `recent_hypothesis_origin`.
6. At least two mutually exclusive primary/secondary families have a positive 20D lift; nested children do not count toward this minimum.
7. No primary family worsens severe-loss rate by more than `+1.0` percentage point.

### Add Binary Badge Only

Recommend a `trend_acceleration_triple` badge when continuous-column gates fail but all of these hold:

1. Triple-minus-control median same-day 20D lift is at least `+0.25` percentage points.
2. Triple wins on at least 52% of paired dates.
3. Combined historical bootstrap lower bound is above zero.
4. All three time segments have the same positive direction.
5. At least two mutually exclusive families have the same positive direction.
6. Severe-loss deterioration is no more than `+1.0` percentage point.
7. Median triple candidates per eligible date is at least five.

### Reject Introduction

Reject both columns and badge when neither gate passes. A positive result in only one nested scaffold is insufficient.

### Fixed 20D/60D Replacement

Replacement is not authorized by this experiment. Existing fixed-return fields, liquidity regimes, `Overheat`, `stale_rally_fade`, and `momentum_20_60_top20` remain unchanged regardless of the result.

## Architecture

Create a new independent experiment `market-behavior/ranking-trend-acceleration-conditional-lift` with:

- a focused analytics domain module;
- a runner-first CLI;
- unit tests including PIT append stability;
- a bundle containing `manifest.json`, `results.duckdb`, and `summary.md`;
- a canonical experiment README with a complete Japanese `## Published Readout`;
- catalog and experiment-index entries.

Extract the rolling log-price OLS calculation from the earlier trend-slope experiment into a small shared analytics helper so both experiments use exactly the same numerical definition. Candidate definitions remain explicit in the new research module and mirror existing Ranking preset semantics without importing frontend code.

The bundle contains:

- `coverage_diagnostics_df`;
- `candidate_registry_df`;
- `conditional_binary_lift_df`;
- `fixed_incremental_2x2_df`;
- `continuous_rank_lift_df`;
- `topk_priority_lift_df`;
- `segment_stability_df`;
- `bootstrap_effect_ci_df`;
- `decision_gate_df`;
- `observation_sample_df`.

## Testing

Tests must cover:

- exact triple boundaries for zero, equality, negative, and null slopes;
- known OLS slope and R² fixtures;
- candidate definitions that do not reference OLS or future-return columns;
- mutually exclusive slice assignment and union deduplication;
- Prime-equivalent PIT membership for `0101` and `0111`, with Standard/Growth excluded;
- same-day pairing requiring both sides;
- candidate/date-local percentile calculation;
- incomplete forward-window exclusion;
- stable split labels;
- fixed-seed bootstrap reproducibility;
- future-row append invariance;
- bundle creation with every required result table.

## Publication and Follow-On

The canonical README controls the conclusion. The readout must distinguish historical replication from the 2024+ hypothesis-origin period and must state that observation-level evidence is not portfolio performance.

If a decision gate passes, production API/materialization/UI work requires a separate approved implementation design. That follow-on must be additive first, must run OpenAPI contract synchronization, and must not alter fixed 20D/60D-derived production semantics without a separate replacement study.
