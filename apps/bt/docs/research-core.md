# bt Internal Research Core

## Purpose

`src.domains.analytics.research_core` is a thin internal layer for runner-first
research primitives that are repeated across Trading25 analytics studies. It is
not an external library, a product API, or a generic research DSL. The core keeps
Trading25-specific Data Plane assumptions close to the research modules while
removing low-value duplication from individual studies.

## Current Scope

- `research_core.universe`
  - Market-behavior universe labels and order: `topix500`, `prime_ex_topix500`,
    `standard`, `growth`.
  - Current market-code aliases through `expand_market_codes`.
  - Static SQL fragments for the `stock_master_daily` as-of universe join.
- `research_core.parameters`
  - Positive integer sequence normalization for window and horizon parameters.
  - Rolling-window warmup start-date estimation with explicit session/calendar
    multiplier and available-start clamp.
- `research_core.tables`
  - Stable output-table ordering with universe order and optional local ordering
    maps.

The initial migration covers these representative market-behavior studies:

- `classical_momentum_research`
- `new_high_momentum_research`
- `turtle_like_momentum_research`

These were chosen because they share the same market universe, output ordering,
and warmup/parameter patterns while still exercising different research shapes:
event selection, condition bucket summaries, and trade-ledger portfolio output.

## Boundaries

- `research_bundle.py` remains the bundle/output SoT for
  `manifest.json + results.duckdb + summary.md`.
- `readonly_duckdb_support.py` remains the local DuckDB read/snapshot support.
- `shared/utils/pit_guard.py` remains the preferred helper for PIT-safe
  dataframe joins.
- `scripts/research/common.py` remains runner CLI glue for output arguments and
  bundle payload printing.
- Individual research modules still own study-specific SQL, features, buckets,
  portfolio construction, interpretation, and Published Readout content.

## Candidate Backlog

Keep future additions small and proven by at least two real research modules
before moving them into the core.

- `event_panel`: canonical stock price + `stock_master_daily` as-of panel CTE,
  including 4-digit/5-digit dedupe and current market universe scoping.
- `asof_features`: PIT-safe latest fundamentals or valuation feature joins when
  dataframe helpers are not enough and the SQL pattern is repeated.
- `bucket_analysis`: repeated quantile/condition bucket summaries, baseline
  lift, and same-universe-day lift calculations.
- `research_outputs`: output table shape checks and bundle table naming helpers.
- `validation`: horizon coverage checks, event/date leakage assertions, and
  minimum sample-size diagnostics.

## Guardrails

- Do not move one-off strategy logic into the core.
- Do not make the core a new execution engine or replace vectorbt/Nautilus roles.
- Do not change runner-first, bundle-first, or Published Readout SoT behavior.
- Preserve PIT filtering order: as-of filtering must happen before latest-row
  selection.
- Keep migrations incremental; each core primitive should have focused tests and
  at least one representative runner migration.
