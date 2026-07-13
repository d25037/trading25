# PIT Future-Leak Research Removal Design

**Date:** 2026-07-13  
**Status:** Approved  
**Scope:** `apps/bt` research implementation, publication surfaces, and CI governance

## Objective

Remove research whose selection, universe, or feature construction depends on future information. The removal is end-to-end: contaminated research must not remain executable, test-routed as an archived exception, published in the active experiment catalog, or available as downstream evidence.

Compatibility aliases, archived executable paths, and broad CI exemptions are not retained.

## Decision

Use a full active-surface purge:

1. Delete contaminated domain implementations, runners, tests, published readouts, and catalog entries.
2. Delete downstream studies that consume contaminated bundles or combine them with other invalidated studies.
3. Delete retrospective analyses that preserve future-derived parameters, even when they do not claim tradeable evidence; preserve only generic primitives with independent active consumers.
4. Replace broad archive exceptions with enforceable PIT and referential-integrity checks.
5. Keep a concise audit record of why the surfaces were removed; do not preserve obsolete performance headlines as active research.

## Confirmed Contamination Classes

### Future-derived parameter selection

- `topix-streak-extreme-mode`: `X=3` was selected using forward-return results from the same history.
- `topix-extreme-mode-mean-reversion-comparison`: inherited the contaminated streak selection.
- `topix-streak-multi-timeframe-mode`: `short=3 / long=53` was selected from future-return ordering and validation results.

Any tradeable study that fixes `3/53` from this discovery chain is invalid even if its later feature construction and walk-forward mechanics are point-in-time correct.

### Future-conditioned feature rows

The former daily feature panel accepted segment rows keyed by `segment_end_date`. A row for date `t` could disappear or change when a later streak extension was added. Legacy coercion from these event rows into daily signal rows must be removed.

### Historical universe membership leak

Several studies applied current `stocks.scale_category` or a current TOPIX100 proxy to all historical dates. Walk-forward splitting does not repair this survivorship and future-membership leak.

This affects the published fixed-3/53 TOPIX100 studies, the downside-risk breadth confirmation overlays, and the invalidated TOPIX100 SMA readouts recorded in the PIT invalidation register.

## Removal Boundary

### Fixed-3/53 tradeable family

Remove all executable and published surfaces for:

- signal-score LightGBM
- next-session intraday LightGBM and walk-forward
- open-to-close 5-day LightGBM and walk-forward
- open-to-close 10-day LightGBM and walk-forward
- 5-day excess-vs-TOPIX walk-forward
- open-to-open 5-day LightGBM and walk-forward
- Top1 fixed-committee overlay
- Top1 duplicate-policy analysis

The deletion includes corresponding domain modules, runners, unit tests, runner tests, experiment directories, experiment index entries, catalog metadata, and UI test fixtures that represent already-removed invalid research.

Family-local LightGBM feature and validation helpers are deleted when their reverse dependency set becomes empty. No `topix100_streak_353_*` compatibility import is retained for deleted studies.

### Invalid upstream streak experiments and transfer study

Remove the executable runner/publication surfaces for the three future-derived parameter-selection experiments. Also remove `topix100_streak_353_transfer`: its fixed `3/53` parameters preserve the contaminated discovery result, so a retrospective label is not an acceptable compatibility boundary. Delete `topix_streak_state.py` because its only remaining consumer is that transfer study.

### Invalid breadth-confirmation overlays

Remove the trend/breadth overlay, shock-confirmation vote overlay, and shock-confirmation committee overlay end-to-end. Preserve unaffected downside-risk baselines and family-committee research only when they do not consume a current-membership breadth proxy.

### Invalidated TOPIX100 SMA readouts

Remove the three invalidated published readouts and their dedicated executable orchestration:

- `topix100-sma-ratio-lightgbm`
- `topix100-price-vs-sma-q10-bounce-regime-conditioning`
- `topix100-sma50-raw-vs-atr-q10-bounce`

PIT-safe generic calculations may remain only when they have an independent, active consumer and do not perform current-membership historical resolution. Otherwise they are deleted with the study.

## Full Deletion Boundary

No fixed-`3/53` transfer, retrospective event study, compatibility import, runner, test, publication, or web fixture remains. A future-extension-stable implementation is still invalid when its fixed parameters originated from future-return selection. Neutral-looking helpers created solely for that study are deleted with it.

Shared infrastructure such as `research_bundle.py`, `scripts/research/common.py`, `pit_guard.py`, and generic walk-forward infrastructure remains.

## Publication and Audit Cleanup

- Remove deleted experiments from `apps/bt/docs/experiments/README.md`.
- Remove their complete TOML sections and dangling `relatedExperiments` references from `research-catalog-metadata.toml`.
- Remove their active and rerun-queue rows from the PIT invalidation register.
- Keep one concise deletion record identifying the contamination class and removal date, without obsolete performance headlines or a rerun promise.
- Update the 2026-04-10 streak PIT audit so deleted module links and the superseded temporary retention decision cannot be read as current guidance.
- Do not rewrite dated maintainability snapshots or completed issue history. Regenerate only the latest snapshot when its generator is part of normal verification.

## CI and Guardrail Changes

### Remove archive exemptions

Delete `ARCHIVED_RESEARCH_PREFIXES`, `_is_archived_research_module()`, and the branch that skips tests for `topix100_streak_353_*`. No removed transfer module or test mapping remains.

### Test routing must fail closed

A changed research domain or runner without a matching test must not silently produce an empty pytest target. The mapper will report an error or conservatively select the relevant test directory. Tests cover both domain and runner cases.

### Cross-surface integrity

The research guard scans the complete research surface in CI and verifies that:

- active experiment README IDs and catalog IDs agree;
- related experiment IDs resolve;
- active catalog IDs and related experiment IDs resolve to readouts;
- deleted paths do not require an archived exception;
- duplicate IDs and status drift fail CI.

Domain `*_RESEARCH_EXPERIMENT_ID` constants remain bundle namespaces and are
not treated as proof that a readout is published.

### PIT regression contract

Tradeable historical-universe research must prove, where applicable:

- the result at `t` is identical with input truncated at `t` and with future rows appended;
- membership is resolved for the signal date from `stock_master_daily` and, when needed, `index_membership_daily`;
- missing historical membership does not fall back to a latest/current snapshot;
- future disclosures, OHLCV, or membership do not alter past universe, features, scores, buckets, or selections;
- fold training and feature cutoffs precede prediction dates;
- forward-return and label columns do not enter the feature matrix.

The existing `tests/unit/utils/pit_assertions.py` helpers should be reused and extended instead of introducing family-specific compatibility fixtures.

## Implementation Sequence

1. Add or tighten guard and routing tests so stale references and missing test mappings fail.
2. Remove the fixed-3/53 executable family and downstream bundle consumers.
3. Remove future-derived upstream experiment orchestration, the fixed-`3/53` transfer study, and its otherwise-unreferenced neutral helper.
4. Remove breadth-overlay and invalidated SMA experiment surfaces.
5. Clean experiment index, catalog, register, audit, and TS fixtures.
6. Run targeted PIT/guard tests, full research guardrails, Ruff, Pyright, dependency-direction checks, and the full bt test suite.

Independent slices are implemented by subagents, with the primary agent owning integration, cross-surface cleanup, and final verification.

## Acceptance Criteria

- No contaminated research runner or domain entry point remains importable.
- No active experiment catalog or web fixture exposes a deleted experiment.
- No legacy compatibility path accepts future-conditioned event rows as daily signals.
- No fixed-`3/53` transfer or neutral-helper compatibility surface remains.
- Research CI has no family-prefix archive exemption.
- New untested research files and dangling publication references fail CI.
- Repository-wide verification passes without weakening unrelated tests or guards.
