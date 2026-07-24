# Task 3 Report: statistics, evidence, and adoption gates

## Status

Implemented the Task 3 domain statistics, evidence tables, and frozen adoption
decision gates in the requested domain module and focused unit test.

## RED / GREEN evidence

1. Statistics RED:
   - Added deterministic paired moving-block bootstrap and original-order Holm
     tests.
   - Observed collection failure because `moving_block_bootstrap_delta_ci` and
     `holm_adjust` were undefined.
2. Statistics GREEN:
   - Implemented circular paired blocks, fixed-seed reproducibility, union-date
     alignment with inactive strategies earning zero cash return, empirical
     two-sided p-values, and Holm step-down adjustment.
3. Decision RED:
   - Added independent failures for CI touching zero, adjusted p-value `0.05`,
     199 trades, 99 dates, IR lift `0.149`, tail improvement `9.99%`, turnover
     ratio `1.501`, 20bps reversal, minority positive years, holdout reversal,
     and near-ring/20-session sign reversal.
   - Observed collection failure because `build_decision_gate_df` was undefined.
4. Decision GREEN:
   - Implemented strict `adjusted_p_value < 0.05`, inclusive operational
     thresholds, core/60/OOS primary evaluation, holdout direction, and
     near1/near2 with 20-session robustness sign checks.
   - Entry and exit family outcomes are independent. Combined remains
     `not_evaluated` until both families pass pre-holdout gates.
5. Evidence RED:
   - Added a real VectorBT execution aggregation test.
   - Observed collection failure because `build_evidence_tables` was undefined.
6. Evidence GREEN:
   - Implemented the approved result table contracts, correct per-family
     baseline matching, gross/net trade statistics, annualized IR, drawdown,
     5% expected shortfall, event-based turnover, paired deltas, annual
     stability, bootstrap rows, cost sensitivity, and family-separated Holm
     adjustment.
7. Turnover regression RED/GREEN:
   - The first implementation reported return-row count (`4.0`) as turnover.
   - Added the expected `2 transitions / 4 trading dates = 0.5` assertion, then
     corrected the source to use period entry/exit events.
8. Lineage cleanup RED/GREEN:
   - Added assertions for the exact `HardFilterPitLineage` fields.
   - Removed the duplicate earlier lineage type and reused the experiment-local
     immutable lineage in the research result.

## Verification

- `uv run --directory apps/bt pytest tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py -q`
  - `55 passed, 1 warning in 43.13s`
- `uv run ruff check src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py tests/unit/domains/analytics/test_ranking_sma5_score_ring_hard_filter_evidence.py`
  - `All checks passed!`
- `uv run pyright src/domains/analytics/ranking_sma5_score_ring_hard_filter_evidence.py`
  - `0 errors, 0 warnings, 0 informations`
- `git diff --check`
  - clean

## Self-review

- No frozen rule, threshold, period, gate, or family was changed in response to
  observed outcomes.
- Inactive dates are aligned on the union comparison calendar and filled with
  zero cash return before paired deltas and bootstrap resampling.
- Holm correction is grouped separately by entry/exit family (and matching
  ring/cap/period), preserving original row order.
- Primary and robustness evidence are not pooled: core/60/OOS determines the
  primary result; holdout and near/20 rows only supply their specified sign
  checks.
- The builder requires explicit gross/10bps/20bps executions for each variant,
  so missing cost evidence fails loudly instead of silently changing the gate.
- Existing Task 2 fee assertions contained a pre-existing mismatch with
  VectorBT semantics. The readable trade return is net PnL divided by gross
  entry value (no fee cross-term), and first-day portfolio fee return is
  `-fee / (1 + fee)`. The focused test now documents those definitions while
  retaining the entry- and exit-day fee-booking assertions.

## Concerns

- The focused suite retains one existing warning from the test environment; no
  new ruff or pyright diagnostics remain.
- Task 4 must execute and pass all required 0/10/20bps variant inputs into
  `build_evidence_tables`; the domain intentionally does not synthesize missing
  evidence.

## Independent-review follow-up

Five Important review findings were fixed with additional RED/GREEN coverage:

1. Annual stability scope and concentration:
   - RED demonstrated that discovery, holdout, near-ring, cap-20, and duplicate
     rows could influence the family/variant-only annual gate.
   - GREEN restricts both generation and gate consumption to
     `core_high_high` / 60 sessions / OOS (2022–2024), groups duplicate rows by
     distinct year, and exposes `distinct_annual_year_count`,
     `positive_annual_year_count`, `passes_positive_year_majority`, and
     `passes_not_single_year_dependent`.
   - The concentration condition is explicit: after removing the single best
     annual delta, the sum of the remaining distinct-year deltas must remain
     strictly positive.
2. Exact combined prerequisites:
   - RED showed that any passing entry plus any passing exit promoted a
     mismatched combined ID.
   - GREEN parses the exact `E*__X*` components and requires those exact entry
     and exit variants to pass pre-holdout. Ineligible combined variant and
     family outputs are consistently `not_evaluated`.
3. TOPIX-excess IR:
   - RED showed that annualized IR used raw portfolio returns.
   - GREEN requires one finite `topix_close` per comparison date, carries the
     derived benchmark return in `VariantExecution`, publishes benchmark and
     TOPIX-excess daily columns, and calculates IR from
     `portfolio_return - benchmark_return`. Inactive strategy dates remain zero
     cash return before benchmark subtraction.
4. Period-safe trade evidence:
   - RED used a trade entered in December 2024 and closed in January 2025; it
     incorrectly appeared in OOS trade metrics.
   - GREEN documents and applies the contract that a period trade statistic
     includes only a closed trade whose entry and exit are both inside the
     period. Date-level portfolio returns remain sliced independently.
5. E4 eligibility:
   - RED showed E4 could be promoted without E2 and E3 evidence.
   - GREEN exposes `passes_confirmatory_prerequisite` and leaves E4
     `not_evaluated` unless both E2 and E3 independently pass pre-holdout entry
     criteria.

Follow-up verification:

- Focused pytest: `63 passed, 1 warning in 43.32s`.
- Ruff: `All checks passed!`.
- Pyright: `0 errors, 0 warnings, 0 informations`.
