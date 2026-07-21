# Ranking Fixed Return Priority Evidence — Research Design

## Goal

Determine whether fixed trailing 20-session and 60-session returns provide useful Ranking priority **inside** Prime-equivalent long candidates whose expected return is established independently of those fixed returns. This is a replacement-or-retention study for priority semantics, not a broad all-stock momentum study.

The research may recommend keeping both fixed columns, keeping only one, using an equal-weight composite, reducing them to a badge, or removing them from Ranking priority. Raw 20D/60D returns may remain informational even when they fail the priority gate. Production/API/UI changes require a separate follow-on.

## Standing Scope and PIT Contract

- Universe is exact-date Prime-equivalent membership only: pre-reorganization `0101`, post-reorganization `0111`.
- Standard and Growth are excluded from inputs, aggregates, gates, and readout tables.
- Signal date `X` uses information observable through the close of `X`; outcomes begin after `X`.
- Market membership comes from exact-date `stock_master_daily`; current membership is never projected backward.
- Market Data Plane must be schema v4 with `stock_price_adjustment_mode=local_projection_v2_event_time`.
- No latest/current fallback is allowed for missing PIT valuation, fundamentals, basis, membership, or liquidity data.
- Primary outcome is 20D close-to-close TOPIX-excess return. 5D and 60D are supporting diagnostics.
- The feature is after-close only. This research does not establish pre-open decidability.

## Prior Evidence Being Tested

Existing research indicates:

- broad `neutral_rerating` is not independently strong, but value/leadership/ATR long scaffolds within or near that regime have positive forward-return evidence;
- `distribution_stress` is weaker and has worse left-tail behavior, but is not standalone short alpha;
- shallow `20D < 0` is associated with weaker forward excess return, while deep pullbacks (`20D <= -10%`) can behave differently and carry high severe-loss risk;
- previous OLS/SMA/EMA replacement studies did not prove a replacement, but also did not directly establish that fixed 20D/60D returns add priority inside a fixed-free long scaffold.

This experiment closes that gap.

## Fixed-Free Primary Candidate Families

Candidate flags are materialized before fixed-return features, signs, percentiles, or outcomes. Primary predicates must not reference liquidity regime labels, 20D/60D momentum classifications, `ex_overheat`, current sector-strength fields, or future returns.

Two mutually exclusive primary families are frozen:

| Family | Definition | Role |
| --- | --- | --- |
| `strict_value_long_only` | Deep Value + Long Hybrid Leadership + raw ATR20 acceleration | Primary |
| `value_extension_long_only` | equal-weight value composite score `>= 0.8` + Long Hybrid Leadership + raw ATR20 acceleration, excluding Deep Value | Primary |

Long Hybrid Leadership uses only 120D/252D/504D leadership. Raw ATR20 acceleration must not include the 20D-return overheat cap. Current sector strength is excluded because its construction uses constituent 20D/60D returns.

Liquidity residual z is sensitivity-only in frozen bands `<-1`, `[-1,1)`, `[1,2)`, and `>=2`. The primary families are not conditioned on neutral/crowded/stress labels.

## Priority Variants

Within signal-date Prime membership, compute:

- `fixed20_priority`: cross-sectional percentile of trailing 20D return;
- `fixed60_priority`: cross-sectional percentile of trailing 60D return;
- `fixed_equal_priority`: simple mean of the two percentiles.

There is no weight or threshold optimization.

Strict sign quadrants are `++`, `+-`, `-+`, and `--`. Zero and missing values are explicit non-quadrant buckets. Primary incremental contrasts are:

1. `++ - +-`: incremental 60D-positive sign conditional on 20D positive;
2. `++ - -+`: incremental 20D-positive sign conditional on 60D positive;
3. `++ - non++`;
4. interaction: `(++ - +-) - (-+ - --)`.

Each daily comparison requires at least two symbols per side.

## Analyses

### Continuous ordering

For each primary family/date, compare top and bottom 20% for each priority variant and calculate daily Spearman IC. Report same-date mean/median lift, IC distribution, positive-date rate, severe-loss difference, and coverage. Each daily side requires at least two candidates.

### Sign badge

Report the full 2×2 and the four pre-registered contrasts. Zero and missing rows are visible in coverage but excluded from strict quadrant contrasts.

### Operational Top-K

Combine the two primary-family shortlists without duplicates, preserve family labels, then compare the unordered eligible basket with priority-selected Top 5 and Top 10. Dates with fewer than `2K` candidates are excluded. Report each family separately, the combined shortlist, and leave-one-family-out sensitivity.

### Statistical treatment

Primary evidence is the time series of same-date paired spreads, not pooled stock-day significance. Confidence intervals use a fixed-seed moving-block bootstrap with 2,000 resamples and block length equal to the forward horizon. Incomplete forward windows are excluded.

Segments are `2017-2021`, `2022-2023`, and `2024+`, with annual rows. The 2024+ period is not a holdout and cannot support a full out-of-sample claim.

## Decision Gates

For `fixed20_priority`, `fixed60_priority`, and `fixed_equal_priority`, both primary families must satisfy at 20D:

- top-minus-bottom mean lift at least `+0.25` percentage points;
- moving-block bootstrap 95% CI lower bound above zero;
- median daily Spearman IC at least `0.02`;
- IC-positive dates at least 52%;
- positive mean lift in all three segments;
- severe-loss deterioration no more than `+1.0` percentage point;
- at least 300 historical observations, 50 eligible paired dates, and median 5 focus candidates per date.

The `++` badge requires both `++ - +-` and `++ - -+` to meet, in both families:

- median daily lift at least `+0.25` percentage points;
- positive-date rate at least 52%;
- bootstrap lower bound above zero;
- positive direction in all three segments;
- median candidates at least five in both cells;
- severe-loss deterioration no more than `+1.0` percentage point.

Operational Top-K requires K=5 and K=10 point estimates both positive, at least one lower CI above zero, no direction reversal in leave-one-family-out checks, and no worsening of severe-loss rate or sector concentration.

The final recommendation is exactly one of:

1. keep both fixed 20D and 60D as priority columns;
2. keep 20D only;
3. keep 60D only;
4. retain raw columns as informational and use the equal-weight composite for priority;
5. use a `++` badge only;
6. remove fixed returns from Ranking priority while retaining raw values as informational where useful;
7. insufficient evidence.

No gate is relaxed after observing results. Insufficient sample produces `insufficient evidence`, not rejection.

## Sensitivities

Sensitivity analyses do not enter the primary gate and do not search thresholds:

- liquidity z bands `<-1`, `[-1,1)`, `[1,2)`, `>=2`;
- `20D < 0` split into `<= -10%` and `(-10%, 0%)`;
- sector-equal-weight date baskets;
- bank exclusion;
- date-fixed-effect regression controlling value score, liquidity z, raw ATR, and long leadership;
- N225-excess return (TOPIX remains primary);
- `>0/<0` primary boundaries versus `>=0/<0` sensitivity.

## Architecture and Bundle

Create `market-behavior/ranking-fixed-return-priority-evidence` with a focused analytics module, runner-first CLI, unit tests, durable bundle, catalog/index entries, and a Japanese decision-first Published Readout.

The bundle contains:

- `coverage_attrition_df`;
- `scaffold_registry_df`;
- `continuous_priority_lift_df`;
- `fixed_2x2_daily_df`;
- `fixed_incremental_contrast_df`;
- `topk_priority_lift_df`;
- `segment_stability_df`;
- `bootstrap_effect_ci_df`;
- `regression_sensitivity_df`;
- `decision_gate_df`;
- `observation_sample_df`.

Unsupported Market v4 provenance, missing exact-date membership, or broken PIT source coverage fails closed. Tests cover forbidden candidate predicates, mutual exclusivity, 0101/0111 exact-date membership, Standard/Growth exclusion, zero/missing sign boundaries, date-local percentiles, same-date minimum cells, incomplete outcomes, deterministic bootstrap, future-row append invariance, exact bundle tables, and the two-family gate.

## Publication

The canonical README begins with the decision and plain-language answer to whether fixed 20D/60D deserves Ranking priority within expected-return long candidates. It then shows the two independent families, effect sizes and uncertainty, segment stability, downside, Top-K practicality, limitations, and the production follow-on (if any). Observation-level evidence is explicitly not portfolio performance.
