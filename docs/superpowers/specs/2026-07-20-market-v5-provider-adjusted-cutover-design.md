# Market v5 Provider-Adjusted Cutover Design

## Status

GitHub Issue #490 is the approved product specification. This document records the implementation design selected for that issue.

## Goal

Replace Market v4's locally projected, multi-basis data plane with Market v5, whose consumer-facing daily prices exactly match J-Quants `AdjO/AdjH/AdjL/AdjC/AdjVo`, while preserving raw provenance, point-in-time disclosure safety, immutable bounded datasets, and explicit full-rebuild cutover and rollback.

## Alternatives considered

1. **Provider-adjusted physical consumer table plus current-basis fundamentals (selected).** Keep `stock_data`'s established OHLCV columns but populate them from provider `Adj*`; preserve raw and adjusted provider fields together in `stock_data_raw`; replace basis catalogs with a compact event ledger and current-basis adjusted statements; expose valuation as a read-only ASOF relation. This minimizes price-consumer churn and removes the expensive all-basis write path.
2. **Store provider payload once and make every price consumer a view.** This removes duplication, but makes the most frequently used backtest and analytics path depend on a wider view and complicates Parquet publication and existing direct readers.
3. **Retain v4 basis tables as a compatibility layer.** This keeps old readers working but preserves the operational complexity and dual-read ambiguity that Issue #490 explicitly removes.

## Physical contract

Market v5 uses physical schema version `5` and adjustment mode `provider_adjusted_v1`.

- `stock_data_raw` stores `O/H/L/C/Vo/Va`, `AdjFactor`, and `AdjO/AdjH/AdjL/AdjC/AdjVo` without local price projection.
- `stock_data` keeps its existing `open/high/low/close/volume` interface and stores the provider `Adj*` values exactly.
- `stock_adjustment_events` stores only non-unit factors within the effective provider window, keyed by code/date, with factor and a deterministic source fingerprint.
- Provider vintage metadata records plan, provider-as-of, effective coverage start/end, and source fingerprint. Coverage comes from the configured plan and observed provider response; business logic never embeds a fixed ten-year duration.
- `stock_adjustment_bases` and `stock_adjustment_basis_segments` are absent from the v5 required schema.
- `statement_metrics_adjusted` has one current-basis row per disclosure identity, including diluted EPS, dividend, and share-count audit fields. It has no retained basis dimension.
- `daily_valuation` is a view over provider-adjusted prices and the latest disclosure known on or before each market date. It retains disclosure provenance columns and never joins a future disclosure.

## Ingest and atomicity

Normal date sync validates and appends only new provider rows. A non-unit factor, correction of an existing factor, or drift in a previously stored provider-adjusted value marks that code for a full available-window refresh.

The refresh fetches all pages before mutation, normalizes and validates code/date uniqueness, positive finite factors, complete raw/adjusted values, coverage, and raw/adjusted consistency. It then replaces that code's active-window rows, prunes rows before coverage, rebuilds its event ledger and current-basis statements, and updates watermark/hash in one DuckDB transaction. Fetch, pagination, validation, or transaction failure leaves the prior snapshot and metadata unchanged. Parquet export occurs only after the committed DuckDB mutation and is regenerated from the committed source of truth.

## Fundamentals and valuation

`statements` remains raw provider provenance. Market v5 adds provider disclosure identity/date/time and period identity so same-day documents and corrections do not coalesce accidentally. Per-share values are multiplied only by events strictly after the disclosure and through the current provider basis date; share counts are divided by the same factor. Totals and dimensionless ratios remain raw. Recalculation is limited to changed disclosures and codes affected by event changes.

Valuation reads `AdjC` and current-basis adjusted statements through a PIT ASOF join. The join condition is disclosure timestamp/date less than or equal to the price date/knowledge cutoff. PER, PBR, forward PER, market capitalization, and related ratios therefore share one official price/fundamentals basis without materializing code x basis x date history.

## Dataset contract

Dataset payload schema version `4` is an immutable Market v5 snapshot. It copies only rows within the pinned source's effective provider coverage and records `providerAsOf`, `providerPlan`, effective coverage, source fingerprint, `provider_adjusted_v1`, and fundamentals adjustment basis date in `manifest.v2.json`. Dataset v3/Market v4 bundles are unsupported and must be recreated; no old price rows are inherited or archived.

## API and consumers

Backtest, chart, screening, and ordinary analytics continue reading `stock_data`, so they switch to provider-adjusted prices without a second price path. Fundamentals, ranking, screening-statement, and valuation-heavy analytics use one current-basis/ASOF query contract. The standalone `adjusted_metrics_pit` job and recovery vocabulary are removed. Stats and validation expose provider coverage, exact adjusted/raw checks, ledger validity, and current-basis freshness. OpenAPI and TypeScript contracts are regenerated after the backend schema changes.

## Cutover and rollback

Market v4 and its mode are incompatible. Only isolated explicit full rebuild, validation, immutable v4 backup, semantic smoke, and atomic activation are supported. Retained-v4 promotion is not a v5 migration path. Rollback restores the exact immutable v4 backup; failed reports and staging data remain available for diagnosis. Existing datasets are deleted or recreated separately.

## Verification

Tests cover provider field equality, normal append, factor/drift full refresh, pagination and validation failure atomicity, provider-window pruning, disclosure identity, no double adjustment, PIT valuation, bounded dataset manifests, schema rejection, OpenAPI/TS consumers, and cutover rollback. A benchmark harness records wall time, CPU, peak RSS, request/page counts, affected codes, row mutations, and storage growth for no-op, one-day, fundamentals-only, and split/drift scenarios.

## Self-review

The design contains no compatibility or dual-read path, no hard-coded retention duration, and no unresolved placeholder. The physical, sync, fundamentals, dataset, API, and cutover contracts all map directly to Issue #490 acceptance criteria.
