# Standard Negative EPS Speculative Winner Feature Combos Design

## Status

Approved for planning. Do not implement beyond this spec until the user reviews it.

## Goal

Design a new runner-first bt research study that compares two speculative cohorts inside
`standard / FY actual EPS < 0` and identifies which pre-entry feature combinations are most
associated with extreme winners.

The primary objective is return maximization through understanding speculative winners, not
median improvement or practical tradability. Low-liquidity and small-cap names remain in scope.

## Target Cohorts

The study compares these two cohorts from the existing standard negative-EPS decomposition:

1. `forecast_positive__cfo_positive`
2. `forecast_missing__cfo_non_positive`

These represent two different speculative paths:

- `forecast_positive__cfo_positive`: speculative turnaround with some fundamental narrative
- `forecast_missing__cfo_non_positive`: more demand/squeeze-driven or forgotten-name speculation

## Winner Definition

The label is defined within each cohort separately.

- Universe for labeling: realized events only
- Outcome metric: existing `next_fy` event return
- Winner label: top 10% of realized events by `event_return_pct` within each cohort

Events without a realized next-FY return, such as `no_next_fy` or price-path failures, must not
participate in winner-threshold calculation.

## Scope

### In Scope

- New analytics domain and research runner
- Pre-entry feature engineering using already-available PIT-safe event inputs
- Single-feature summaries
- Two-feature combination mining
- Conditional three-feature expansion for already-strong two-feature cells
- Cross-cohort comparison tables and markdown summary

### Out of Scope

- Predictive model training
- Execution-cost modeling
- Liquidity constraints meant to make the strategy practical
- New notebook logic
- Historical TOPIX500 reconstruction or other universe changes

## Research Shape

The study should remain runner-first and bundle-first.

- Domain logic: `apps/bt/src/domains/analytics`
- Runner: `apps/bt/scripts/research`
- Outputs: `manifest.json`, `results.duckdb`, `summary.md`, and published summary payload

It should reuse the standard negative-EPS event ledger as the source of truth for base events and
extend it with additional pre-entry feature buckets.

## Feature Set

Use price, liquidity, and financial features together.

### Primary Features

These receive interpretable fixed buckets:

- `entry_market_cap_bil_jpy`
  - `<5b`
  - `5b-20b`
  - `20b-50b`
  - `50b-200b`
  - `>=200b`
- `entry_adv`
  - `<5m`
  - `5m-20m`
  - `20m-100m`
  - `100m-500m`
  - `>=500m`
- `entry_open`
  - `<100`
  - `100-300`
  - `300-1000`
  - `>=1000`
- `prior_252d_return_pct`
  - `-80% to -50%`
  - `-50% to -20%`
  - `>-20%`
  - `missing`
- `prior_20d_return_pct`
  - `<=-30%`
  - `-30% to 0%`
  - `>0%`

### Secondary Features

These use coarse three-bucket or compact categorical treatment:

- `prior_63d_return_pct`
  - `<=-50%`
  - `-50% to -10%`
  - `>-10%`
- `volume_ratio_20d`
  - `<0.7`
  - `0.7-1.5`
  - `>1.5`
- `pre_entry_volatility_20d`
  - `low`
  - `mid`
  - `high`
- `equity_ratio_pct`
  - `<30%`
  - `30-50%`
  - `>=50%`
- `profit_margin_pct`
  - `<=0%`
  - `0-5%`
  - `>5%`
- `cfo_margin_pct`
  - `<=0%`
  - `0-10%`
  - `>10%`
- `sector_33_name`
  - Keep named sectors when sample size is large enough
  - Collapse sparse sectors into `other`

Missing values must be preserved as explicit buckets where applicable rather than dropped silently.

## PIT Rules

Every feature must be available at FY disclosure time or before the entry session.

Allowed inputs include:

- current event-ledger values already computed from the FY disclosure
- price and volume history strictly before entry
- statement snapshot fields known at disclosure time

Disallowed inputs include:

- anything derived from follow-up forecast resumption
- any price observation after entry for feature construction
- any future-year threshold or grouping used as a feature

## Analysis Flow

1. Build the base realized-event frame for each cohort.
2. Compute cohort-specific top-decile winner cutoffs.
3. Attach bucketed pre-entry features to every realized event.
4. Produce single-feature summaries for context and sanity checks.
5. Mine all two-feature combinations across the selected bucketed features.
6. Retain only cells with:
   - `event_count >= 15`
   - `winner_count >= 3` for expansion to three-feature analysis
7. Expand only strong two-feature cells into three-feature combinations.
8. Build a cross-cohort comparison table showing:
   - combinations strong in both cohorts
   - combinations strong only in `forecast_positive__cfo_positive`
   - combinations strong only in `forecast_missing__cfo_non_positive`

## Metrics

For single, pair, and triplet summaries, report:

- `event_count`
- `winner_count`
- `winner_hit_rate`
- `lift_vs_base_rate`
- `winner_capture_rate`
- `mean_return_pct`
- `median_return_pct`

Where useful, also include:

- cohort base rate
- bucket labels for each feature
- sorting rank for strongest cells

## Outputs

The results bundle should include at least these tables:

- `winner_threshold_df`
- `feature_bucket_def_df`
- `event_feature_df`
- `single_feature_summary_df`
- `pair_combo_summary_df`
- `triplet_combo_summary_df`
- `group_comparison_df`
- `top_examples_df`

### Table Intent

- `winner_threshold_df`: top-decile cutoff and realized-event count for each cohort
- `feature_bucket_def_df`: explicit bucket definitions used by the run
- `event_feature_df`: one row per realized event with cohort, winner label, and bucketed features
- `single_feature_summary_df`: bucket-level hit rates and lift for each feature
- `pair_combo_summary_df`: main output for two-feature mining
- `triplet_combo_summary_df`: only derived from strong pair cells
- `group_comparison_df`: side-by-side comparison of pair or triplet strength across the two cohorts
- `top_examples_df`: representative high-return winners captured by strong cells

## Summary Markdown

`summary.md` should lead with:

1. cohort sizes and top-decile cutoffs
2. strongest two-feature combinations by cohort
3. strongest three-feature extensions
4. shared speculative signatures across both cohorts
5. signatures unique to one cohort

The summary should answer whether the speculative winners are closer to:

- narrative-backed turnaround speculation
- pure demand/squeeze speculation

## Validation

Add focused tests for:

- bucket boundary correctness
- winner-label threshold assignment
- explicit exclusion of non-realized events from thresholding
- `event_count >= 15` filter behavior
- `winner_count >= 3` gating for three-feature expansion
- three-feature outputs only deriving from already-strong two-feature cells
- PIT-safe feature construction using only disclosure-time or pre-entry inputs
- runner argument contract and `--help`

Validation gates for the research change:

- `ruff`
- `pyright`
- focused `pytest`
- runner `--help`

## Implementation Constraints

- Follow existing runner-first bt research patterns in this repo
- Reuse existing event-ledger and enrichment helpers where practical
- Keep the study interpretable; no tree models or black-box ranking in this phase
- Prefer explicit bucket definitions over purely data-driven deciles for the main speculative features
- Keep the change scoped to this study; do not refactor unrelated analytics modules

## Expected Result

The study should make it possible to answer:

- which pre-entry feature combinations best identify top-decile speculative winners
- which combinations are common across both speculative cohorts
- which combinations are specific to turnaround-like versus demand-driven speculation

The result should be directly usable as the next decision surface for a later rule design or
portfolio experiment.
