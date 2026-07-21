# Ranking Technical Fit Score Shape Evidence — Research Design

## Goal

Determine whether a third, high-is-good `Technical Fit Score` improves discretionary prioritization among stocks that already have high Value Score and Long Hybrid Score, and whether the underlying technical family should be fixed 20D/60D endpoint returns or rolling OLS fitted moves.

The research must not assume that higher raw momentum is always better or preselect a `60–80%` sweet spot. It first measures the raw response shape, then learns a nonlinear desirability mapping using prior data only, and evaluates that mapping out of time. The result may recommend a fixed-based Fit Score, an OLS-based Fit Score, an operational fixed preference under statistical equivalence, neither, or insufficient evidence.

This experiment does not change production Ranking, API, materialization, or UI. Any production introduction requires a separate approved follow-on.

## Standing Universe and PIT Contract

- Universe is exact signal-date Prime-equivalent membership only: market code `0101` before reorganization and `0111` after reorganization.
- Standard and Growth are excluded from inputs, aggregates, gates, and readout tables.
- Market Data Plane must be schema v4 with `stock_price_adjustment_mode=local_projection_v2_event_time`.
- Signal date `X` uses information observable through the close of `X`; forward outcomes begin after `X`.
- Market membership comes from exact-date `stock_master_daily`; current membership is never projected backward.
- No latest/current fallback is allowed for missing membership, PIT valuation, price basis, fundamentals, or liquidity data.
- The feature is after-close only. Pre-open decidability is not established.
- Primary outcome is 20D close-to-close TOPIX-excess return. 5D is entry-timing support and 60D is a hold diagnostic.
- Incomplete forward windows are excluded.

## Candidate Population

Candidate membership is materialized before fixed-return, OLS, ATR, liquidity regime, sector-strength, or forward-outcome features are attached. It uses only `value_composite_equal_score` and `long_hybrid_leadership_score`.

Three mutually exclusive rings are frozen:

| Ring | Predicate | Role |
| --- | --- | --- |
| `core_high_high` | Value Score `>=0.8` and Long Hybrid Score `>=0.8` | Primary |
| `near_high_high_1` | both scores `>=0.7`, excluding `core_high_high` | Independent replication 1 |
| `near_high_high_2` | both scores `>=0.6`, excluding the higher two rings | Independent replication 2 |

The rings preserve the discretionary condition that both scores are elevated. They do not include stocks with only one elevated score. ATR acceleration, `ex_overheat`, neutral/crowded/stress labels, fixed momentum signs, OLS features, and current sector strength must not enter ring predicates.

Ring results are never pooled to hide direction differences. The three-ring union may be used to learn one common technical mapping and for an operational combined shortlist, but adoption evidence remains visible by ring.

## Raw Technical Families

Every raw technical input is computed on signal date `X` and ranked cross-sectionally against all exact-date Prime members, before candidate-ring filtering.

### Fixed family

```text
fixed20_level = Prime-wide percentile of trailing fixed 20D return
fixed60_level = Prime-wide percentile of trailing fixed 60D return
fixed_equal_level = mean(fixed20_level, fixed60_level)
```

### OLS family

For window `N`, fit OLS to trailing adjusted `log(close)` against session index and convert the coefficient to the fitted cumulative move:

```text
ols_move_N_pct = (exp(beta_N * (N - 1)) - 1) * 100
```

Then define:

```text
ols20_level = Prime-wide percentile of ols_move_20_pct
ols60_level = Prime-wide percentile of ols_move_60_pct
ols_equal_level = mean(ols20_level, ols60_level)
```

The primary fixed-versus-OLS comparison is `fixed_equal_level` versus `ols_equal_level`. The 20D and 60D components are attribution outputs and cannot replace a failed equal-weight primary after results are seen. Weights are not optimized.

OLS `R²`, `slope20-slope60`, and fixed/OLS sign conflicts are diagnostic only. They cannot tune the OLS primary. Fixed `20D<0`, `60D<0`, fixed `20D>=30%` overheat, and fixed/OLS sign conflicts remain observable diagnostics and do not select the candidate population.

## Shape Discovery Without Sweet-Spot Preselection

Raw levels use five fixed bins:

```text
[0.0,0.2), [0.2,0.4), [0.4,0.6), [0.6,0.8), [0.8,1.0]
```

No bin is designated the expected winner. Report date-equal mean and median forward return, win rate, P10, P25, and severe-loss rate for every family, component, ring, horizon, segment, and year.

Shape classifications are:

- `interior_sweet_spot_confirmed`;
- `monotonic`;
- `flat`;
- `unstable_shape`;
- `insufficient_evidence`.

An interior sweet spot is confirmed only when the walk-forward-selected best raw bin is neither the bottom nor top bin, beats its adjacent bin(s) and the top bin out of time, reproduces in `core_high_high` and at least one near ring, has the same direction in 2022–2023 and 2024+, and does not worsen severe-loss rate.

## Walk-Forward Technical Fit Mapping

The mapping is learned on the union of all three rings so the third score has one meaning across candidate quality tiers. Ring-specific curves are diagnostics only and cannot generate separate favorable transformations.

The first training window is 2017–2021. For evaluation year `Y`:

1. use only rows and completed outcomes strictly before January 1 of `Y`;
2. estimate date-equal 20D TOPIX-excess expectancy for each of the five raw bins;
3. require at least 200 observations and 50 distinct signal dates in every raw bin;
4. normalize the five prior-only expectancy values to `[0,1]`;
5. linearly interpolate between fixed bin centers to assign the evaluation-year `Technical Fit Score`;
6. expand training through year `Y-1` and repeat for the next year.

If all bin expectancies are equal, assign a neutral `0.5` Fit Score and classify the mapping as `flat`. If any bin lacks training coverage, emit `insufficient_training_data` and do not generate a mapping for that family/year. A previous mapping is never carried forward as a fallback.

Evaluation begins in 2022. The 2024+ period is walk-forward but is also the hypothesis-origin period; it is not described as a clean holdout or full out-of-sample proof.

Five-bin plus piecewise-linear interpolation is the only production-candidate mapping. A continuous spline is sensitivity-only.

## Primary Evaluation

For each family, ring, signal date, and horizon:

- calculate Spearman IC between out-of-time Fit Score and forward TOPIX-excess return;
- compare the top 30% with the bottom 30%;
- report mean/median lift, win rate, P10, P25, severe-loss difference, `20D<0` share, fixed `20D>=30%` overheat share, and sector HHI;
- require at least 10 candidates on the date and at least three candidates on each side.

For the combined three-ring shortlist, compare Top 5 and Top 10 selected by Fit Score with the eligible basket. Dates with fewer than `2K` eligible candidates are excluded. Report ring composition, sector concentration, turnover, and downside.

Fixed and OLS are compared on the same eligible dates using:

```text
fixed Fit lift - OLS Fit lift
```

Stock-day pooled results are descriptive. Primary inference uses the time series of same-date paired effects. Confidence intervals use a fixed-seed moving-block bootstrap with 2,000 resamples and block length equal to the forward horizon.

## Fit Score Adoption Gate

At the 20D primary horizon, a family passes only when:

1. `core_high_high` and at least one near ring have top-minus-bottom mean lift of at least `+0.25` percentage points;
2. their moving-block-bootstrap 95% CI lower bounds exceed zero;
3. median daily Spearman IC is at least `0.02`;
4. IC-positive dates are at least 52%;
5. lift is positive in both 2022–2023 and 2024+;
6. severe-loss deterioration is no more than `+1.0` percentage point;
7. combined-shortlist Top 5 and Top 10 lifts are both positive;
8. the remaining near ring is nonnegative or explicitly lacks pre-registered sample coverage.

Sensitivity outputs cannot rescue a failed primary gate.

## Fixed-Versus-OLS Decision

If only one equal-weight family passes, that family wins. If both pass, use the paired daily difference:

- 95% CI lower bound `>0`: `fixed_wins`;
- 95% CI upper bound `<0`: `ols_wins`;
- CI includes zero: `equivalent_fixed_preferred_operationally`.

The operational preference under a statistical tie reflects fixed's simpler explanation, lower computation cost, and existing Ranking semantics. The research result must still say `equivalent`; it must not claim statistical superiority.

Other final states are:

- `neither` when both are sufficiently sampled and fail;
- `insufficient_evidence` when either required comparison lacks sample coverage.

Insufficiency takes precedence over partial recommendations. A component-only success cannot replace the frozen equal-weight comparison.

## Sensitivities

Sensitivity analyses are fixed in advance and do not tune the primary mapping:

- OLS spline shape;
- OLS `R²` bands and `slope20>slope60`;
- fixed/OLS 20D and 60D sign conflicts;
- fixed `20D<0`, shallow negative `(-10%,0%)`, and deep pullback `<=-10%`;
- fixed `20D>=30%` overheat;
- liquidity residual z bands `<-1`, `[-1,1)`, `[1,2)`, `>=2`;
- sector-equal-weight date baskets;
- bank exclusion;
- N225-excess benchmark;
- date-fixed-effect regression controlling Value Score, Long Hybrid Score, liquidity z, and raw ATR.

## Architecture and Bundle

Create `market-behavior/ranking-technical-fit-score-shape-evidence` with a focused analytics module, runner-first CLI, unit tests, durable bundle, experiment index/catalog entries, and a Japanese decision-first Published Readout.

Reuse the Daily Ranking PIT panel, value composite definition, 120D/252D/504D Long Hybrid calculation, shared rolling log-price OLS helper, read-only Market v4 guard, and research-bundle writer. Ring flags must be materialized as keys plus predicates before joining raw technical and outcome columns.

The bundle contains exactly:

- `ring_registry`;
- `raw_score_registry`;
- `coverage_attrition`;
- `raw_shape_daily`;
- `raw_shape_summary`;
- `walkforward_mapping`;
- `oos_fit_score_lift`;
- `fixed_vs_ols_paired`;
- `topk_operational_lift`;
- `overheat_negative_diagnostics`;
- `segment_stability`;
- `annual_stability`;
- `bootstrap_effect_ci`;
- `decision_gate`;
- `observation_sample`.

## Testing

Tests must cover:

- exact `0.6/0.7/0.8` ring boundaries, mutual exclusivity, and union deduplication;
- static rejection of fixed, OLS, ATR, liquidity-regime, sector-strength, and future-outcome tokens in ring predicates;
- exact-date `0101/0111` membership with Standard/Growth exclusion;
- Market v4 and adjustment-mode rejection;
- candidate flag materialization before technical/outcome joins;
- Prime-date-wide fixed and OLS percentiles;
- existing OLS fitted-move numerical equivalence;
- five-bin boundaries, zero, one, and missing values;
- mapping training rows strictly before the evaluation year;
- per-bin 200-observation/50-date minimum;
- flat, monotonic, interior-sweet-spot, unstable, and insufficient classifications;
- no mapping fallback when training coverage is insufficient;
- future-row append invariance for rings, raw scores, mappings, and historical Fit Scores;
- same-date minimum of 10 candidates and three per comparison side;
- incomplete forward-window exclusion;
- deterministic moving-block bootstrap;
- fixed win, OLS win, tie, neither, and insufficient decisions;
- insufficiency precedence over partial success;
- exact 15-table bundle contract;
- Published Readout and `decision_gate` consistency.

## Publication

The canonical README begins with:

1. whether fixed or OLS won;
2. whether an interior sweet spot was confirmed and where it appeared;
3. whether a Technical Fit Score should enter Ranking;
4. how `20D<0` and overheat should be treated;
5. the observation-level, after-close, non-portfolio limitation.

The readout separates 2017–2021 training evidence, 2022–2023 walk-forward validation, and 2024+ hypothesis-origin walk-forward evidence. It reports every ring and does not merge conflicting directions into a favorable aggregate.
