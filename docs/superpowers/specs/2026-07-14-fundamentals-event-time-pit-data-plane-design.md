# Fundamentals Event-Time PIT Data Plane Design

## Goal

Make historical Fundamentals requests strictly event-time point-in-time (PIT)
across disclosures, prices, corporate actions, adjusted per-share metrics,
valuation, stock metadata, and liquidity analytics.

The implementation must preserve the repository rule that
`statement_metrics_adjusted` and `daily_valuation` are the consumer-facing
fundamentals SoTs. A request must never repair or recompute those SoTs locally,
write to the database, substitute a current basis, or fall back to
`stocks_latest`.

## Current Problems

### `to` filters period ends, not information availability

`FundamentalsCalculator._filter_statements` compares `to_date` with the
statement period end. A statement about an old fiscal period can therefore be
disclosed after the requested historical cutoff and still enter the response.
The unbounded statement collection is also reused for forecast enhancement,
quarterly revision selection, share adjustment, and latest-metric selection.

### Each source independently resolves current or latest state

`FundamentalsService` separately loads statements, `stocks_latest`, adjusted
`stock_data`, adjustment events, `statement_metrics_adjusted`, and
`daily_valuation`. There is no single read snapshot or basis identity tying the
results together. Filtering only the final response cannot prove that nested
provenance or intermediate latest-row selections were PIT-safe.

### Existing adjusted rows use a future corporate-action basis

The adjusted-metrics materializer selects one global latest price date, builds
one `adjusted-v1:*` basis, and deletes older bases. `stock_data` is likewise the
current fully adjusted projection of `stock_data_raw`.

Consequently, a `daily_valuation.date <= cutoff` row or an adjusted statement
with `disclosed_date <= cutoff` can still incorporate a split or reverse split
that occurs after the cutoff. Date predicates alone do not remove this leak.

### Historical stock classification is replaced by current metadata

The Fundamentals summary and Prime liquidity model read `stocks_latest`.
Historical requests can therefore use a future company name, listing state,
market classification, or Prime membership. The liquidity cross-section also
mixes this current universe with independently recomputed share adjustments.

## Chosen Architecture

Implement two dependent slices under one design:

1. create a Market Data Plane v4 that retains complete per-code corporate-
   action regime bases;
2. make both Fundamentals APIs consume one validated PIT bundle from that Data
   Plane.

Slice 2 cannot start until Slice 1 has passed its own verification and review.
No compatibility bridge between current-basis and event-time results is
allowed.

## Event-Time Basis Model

### One basis per corporate-action regime

Do not create a basis for every trading date. For each normalized four-digit
code, create:

- one origin basis beginning at the first `stock_data_raw` date;
- one new basis at every valid `stock_data_raw.adjustment_factor != 1` event.

A basis covers a half-open interval:

```text
valid_from <= effective_market_date < valid_to_exclusive
```

The final basis has no `valid_to_exclusive`. Invalid, non-finite, zero, or
negative adjustment factors make the affected basis lineage invalid; they are
not silently treated as `1`.

### Basis catalog

Add `stock_adjustment_bases` to `market.duckdb` with these logical fields:

- `code`
- `basis_id`
- `valid_from`
- `valid_to_exclusive`
- `adjustment_through_date`
- `source_fingerprint`
- `materialized_through_date`
- `status` (`building`, `ready`, or `invalid`)
- `created_at`
- `updated_at`

`basis_id` is stable and readable:

```text
event-pit-v1:{normalized_code}:{valid_from}
```

The source fingerprint detects corrections and deletions without putting a
mutable hash in the identifier. The primary key is `(code, basis_id)`, and
`(code, valid_from)` is unique.

### Basis-adjusted price segments

Add `stock_adjustment_basis_segments` with:

- `code`
- `basis_id`
- `source_date_from`
- `source_date_to_exclusive`
- `cumulative_factor`

Its primary key is `(code, basis_id, source_date_from)` and every segment must
end at or before the next segment begins.

The factor for a raw source date is the product of valid adjustment events in
`(source_date, adjustment_through_date]`. Basis-aware readers project raw OHLC
by multiplying this factor and volume by dividing it. The current `stock_data`
table remains the current convenience projection, but cutoff-aware
Fundamentals and liquidity code must never read it.

### Adjusted metrics and valuation retention

`statement_metrics_adjusted` and `daily_valuation` continue to own
consumer-facing adjusted values. Their rows are keyed by the selected
`basis_version`, whose value is exactly the catalog `basis_id`; all regime
bases are retained.

For each basis:

- adjusted statement metrics include every statement disclosed before the
  basis interval ends, with all prior statements normalized to that basis;
- daily valuation includes the full required price history normalized to that
  basis, but only through the final market date covered by the regime;
- a closed basis is completed through the trading day before the next basis;
- the active basis is appended through the current local market frontier.

This deliberately duplicates valuation history only when a code has a split
or reverse split. Codes without corporate actions retain one basis.

The current prune operation, its SQL, and tests asserting deletion of older
bases are removed. Bases are deleted only when a source event/code is removed,
or during a full Data Plane reset.

## Materialization and Reconciliation

Historical basis construction is a write-side responsibility. It runs after
raw prices and statements are published by initial/incremental sync, stock
refresh, or an explicit adjusted-metrics materialization job. Analytics GET and
POST requests are read-only and never trigger it.

Reconciliation rules are:

- ordinary new price rows extend only the active basis;
- a new adjustment event closes the preceding basis and builds the new basis
  from full required history;
- an event correction or deletion rebuilds lineage from the first changed
  event forward;
- a statement correction rebuilds affected statement metrics and valuation in
  every basis whose interval can observe that disclosure;
- a raw price correction rebuilds the affected valuation rows in every basis
  covering that source date;
- four-digit and five-digit aliases are normalized before lineage is built.

Rows and segments are written to staging relations in one DuckDB transaction.
The catalog changes to `ready` only after coverage and integrity checks pass.
Readers ignore `building` and `invalid` bases.

## Schema Compatibility and Dataset Bundles

Raise the Market Data Plane schema to v4 and set the adjustment mode to
`local_projection_v2_event_time`.

Older market databases are incompatible. Do not add `ALTER` backfills,
automatic conversion, dual reads, old-basis aliases, or migration shims. A v3
or older database must be rebuilt with initial sync and
`resetBeforeSync=true`.

Dataset snapshot creation must copy the basis catalog, segments, and all basis
rows needed by the snapshot date range. Dataset readers become basis-aware and
use the same resolver. Existing snapshots without the v4 basis structures are
unsupported and must be recreated; root-level or legacy snapshot fallbacks
remain forbidden.

## Canonical PIT Bundle

Expose one Data Plane read contract rather than a set of independent latest
getters:

```python
get_fundamentals_pit_snapshot(
    symbol: str,
    cutoff_date: date | None,
) -> FundamentalsPitSnapshot
```

The bundle is resolved inside one protected DuckDB read snapshot and contains:

- requested cutoff, resolved knowledge cutoff, and effective market date;
- selected basis ID, adjustment-through date, and coverage frontier;
- exact-date `stock_master_daily` row;
- disclosure-bounded raw statements;
- same-basis adjusted statement metrics;
- same-basis daily valuation;
- same-basis adjusted OHLCV;
- same-date, same-basis Prime liquidity panel inputs.

Resolution order is fixed:

1. Validate `from` and `to` as real ISO `YYYY-MM-DD` dates and require
   `from <= to` when both are supplied.
2. If `to` is present, use it as `knowledge_cutoff_date`. If it is absent, use
   the current complete local market frontier as `knowledge_cutoff_date`.
   Resolve the latest local market session on or before that date as
   `effective_market_date`; do not derive the date from the requested symbol's
   last price, because a suspended symbol must not move the market snapshot
   backward.
3. Require the `ready` basis interval containing `effective_market_date` and
   require `materialized_through_date >= effective_market_date`.
4. Require an exact `stock_master_daily` snapshot at
   `effective_market_date`; do not use an earlier snapshot or `stocks_latest`.
5. Read every source with the selected basis and cutoff predicates.
6. Validate the completed bundle before any latest-row selection.

Weekend and holiday cutoffs therefore use the preceding local trading date for
prices, valuation, master, and liquidity, while disclosures remain bounded by
`knowledge_cutoff_date`.

## `from` and `to` Semantics

`to` is the knowledge/event cutoff, not a fiscal-period upper bound.

- statements require `disclosed_date <= knowledge_cutoff_date`;
- a forecast disclosed before `to` remains eligible even when its fiscal
  period ends after `to`;
- OHLCV and valuation require `date <= effective_market_date`;
- nested statement and forecast provenance must not be later than the row date
  or `knowledge_cutoff_date`.

`from` is only the response display lower bound. Internal calculations may read
older rows for price-at-disclosure lookup, FY comparisons, revisions, rolling
trading value, ADV, and returns. After calculations, `data` is cropped by
fiscal period end and `dailyValuation` by market date. Changing only `from`
must not change the selected basis, latest metrics, or liquidity result.

## Liquidity PIT Rules

The Prime liquidity path must use:

- the exact `stock_master_daily` snapshot at `effective_market_date`;
- basis-aware OHLCV;
- `daily_valuation.free_float_market_cap` from the same basis;
- a Prime cross-section resolved from the same exact master date.

It must not read `stocks_latest`, raw statements, current `stock_data`, or
service-local share adjustment events. Unsupported non-Prime or insufficient-
sample outcomes remain normal responses. Missing basis/master/integrity is an
error and cannot be downgraded to an unsupported liquidity profile.

## Bundle Integrity and Fail-Closed Errors

Bundle validation requires:

- every price and valuation date is on or before `effective_market_date`;
- every statement disclosure is on or before `knowledge_cutoff_date`;
- every adjusted row has the selected code and basis;
- basis coverage reaches `effective_market_date`;
- `statement_disclosed_date`, `forward_eps_disclosed_date`, and
  `forward_sales_disclosed_date` are not later than their valuation row;
- stock-master and liquidity dates equal `effective_market_date`;
- `latestMetricsSource` is derived from rows actually present in the bundle.

An inconsistent row invalidates the whole bundle. It is not dropped silently.
Current/latest fallback is forbidden.

Use the unified error response with these outcomes for both APIs:

- 404 `stock_not_listed_as_of`: exact master exists but the symbol does not;
- 409 `historical_adjustment_basis_required`: no complete containing basis;
- 409 `stock_master_snapshot_required`: exact master snapshot is unavailable;
- 409 `pit_snapshot_inconsistent`: basis, coverage, or provenance validation
  fails;
- 422 for malformed dates or `from > to`.

The 409 recovery tells the operator to rerun Market DB sync/materialization for
the explicit `adjusted_metrics_pit` stage. It must not recommend `repair`, which
does not refresh price adjustment events.

## Fundamentals Service and API Behavior

Both endpoints use the same request validation, PIT bundle reader, calculation
path, and error mapper:

- `POST /api/fundamentals/compute`
- `GET /api/analytics/fundamentals/{symbol}`

The service may continue to calculate non-basis-dependent ratios from bounded
raw statement values. Adjusted per-share values, adjusted prices, valuation,
market cap, free-float market cap, and their provenance come only from the PIT
bundle.

Remove the cutoff execution path's service-local adjustment-event loading,
share reprojection, current stock-info lookup, and latest-price fallback.

If the stock is listed at the effective date but has no disclosure by the
cutoff, both APIs return 200 with empty `data`. If it is not listed, both return
404. Missing or inconsistent basis/master data returns the same 409 from both
routes. The existing GET-only empty-data 404 behavior is removed.

Add `asOfDate` to `FundamentalsComputeResponse`. It reports the effective local
market date used for price, valuation, stock master, and liquidity. Existing
`provenance.reference_date` reports `knowledge_cutoff_date`.
`priceBasisDate` continues to report the selected adjustment basis frontier and
is not reused as the market as-of date.

This additive response change and clarified request descriptions require
OpenAPI regeneration and TypeScript contract sync. No frontend financial
calculation is introduced.

## Approaches Rejected

### Filter current materialized rows by date

Rejected because a pre-cutoff row can contain a post-cutoff adjustment basis or
nested future disclosure.

### Recompute historical valuation inside FundamentalsService

Rejected because it creates a second valuation implementation outside the Data
Plane SoT and makes read requests mutate or repair state implicitly.

### Fail every historical request when the current basis is newer

Rejected because it removes future leak by making ordinary historical analysis
unavailable instead of maintaining the required historical state.

### Keep only the newest basis with a compatibility fallback

Rejected because arbitrary historical cutoffs require prior regimes and the
repository explicitly prefers removal of legacy/current fallbacks.

### Materialize one basis per trading date

Rejected because adjustment factors change only at corporate-action events.
Per-date versions would multiply storage without changing results.

## Testing Strategy

Implementation follows red-green-refactor and is reviewed after each dependent
slice.

### Data Plane tests

- origin and split-event bases form complete non-overlapping intervals;
- an arbitrary cutoff selects the containing regime, not merely a latest row;
- closed bases cover every market date before the next event;
- active bases append without rewriting unrelated closed bases;
- price projection uses only events through the selected frontier;
- new, corrected, and deleted events rebuild only affected lineage forward;
- invalid adjustment factors mark lineage invalid and fail closed;
- older ready bases survive rebuilds and no prune path remains;
- interrupted materialization never exposes partial rows as `ready`;
- v3 market DBs are rejected until reset and rebuilt;
- dataset snapshots preserve basis resolution and reject pre-v4 bundles.

### Fundamentals PIT tests

- an old fiscal period disclosed after the cutoff is excluded everywhere;
- a post-cutoff quarterly revision cannot populate revised forecasts;
- a post-cutoff split cannot change adjusted EPS, BPS, dividends, price, or
  valuation in a pre-cutoff response;
- future OHLCV and valuation sentinels are excluded;
- mixed basis IDs or future nested provenance produce 409;
- a weekend cutoff resolves the previous trading date and exact master;
- future market reclassification does not change past company/Prime behavior;
- missing historical basis never falls back to the current basis;
- changing `from` changes displayed history only;
- a pre-cutoff forecast for a post-cutoff fiscal period remains eligible;
- no-cutoff requests use the current complete event-time basis;
- GET and POST return equivalent payloads and 404/409/422 behavior;
- `asOfDate`, `priceBasisDate`, and provenance identify their distinct dates.

### Verification gates

- focused materializer, market reader, dataset, Fundamentals service, and route
  tests;
- the full backend test suite;
- Ruff and Pyright for all changed backend paths;
- OpenAPI sync/check and generated TypeScript type tests;
- TypeScript workspace tests and typecheck for affected consumers;
- architecture, skill-reference, and repository hygiene checks.

## Non-Goals

- Ingestion-vintage or bitemporal reconstruction. Later corrections update the
  historical event-time lineage according to corrected source facts.
- Dataset fallback for the live Fundamentals APIs.
- Frontend-local valuation, PIT, or validation logic.
- Retaining v3 market databases, old adjusted basis rows, current-stock
  fallbacks, or response compatibility shims.
- Migrating the Fundamentals Python/TypeScript contract ownership in this
  slice; that remains a later maintenance slice after PIT correctness.

## Acceptance Criteria

1. Every historical Fundamentals result is produced from one validated,
   complete, per-code event-time basis.
2. No cutoff-aware path reads current `stock_data`, `stocks_latest`, a newer
   adjustment basis, or unbounded disclosures.
3. Corporate-action regime bases are retained indefinitely and selected by
   interval plus coverage, with no old-basis prune or latest fallback.
4. Materialization occurs only in write-side sync/refresh/materialization
   workflows and publishes atomically.
5. Market v3 and pre-v4 dataset bundles are rejected rather than migrated or
   read through compatibility code.
6. Stock master and liquidity inputs use the exact effective market date and
   the selected basis.
7. `to` is a disclosure/event cutoff; `from` is a display lower bound; forecasts
   known by the cutoff remain eligible regardless of future period end.
8. GET and POST share validation, data, empty-result, and error semantics.
9. `asOfDate`, `priceBasisDate`, and provenance accurately expose market date,
   adjustment frontier, and knowledge cutoff.
10. Future statements, revisions, prices, corporate actions, metadata, and
    nested provenance are excluded by regression tests.
