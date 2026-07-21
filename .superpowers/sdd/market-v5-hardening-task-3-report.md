# Market v5 Hardening Task 3 Report

## Result

Task 3 is implemented as one indivisible Market v5 schema and lineage change on
base `485e22e88e14349d6a8963cabc61b34772e53528`.

- `ProviderStockStage` is immutable and validates plan, canonical ISO request
  frontier, and normalized non-empty code scope.
- `stock_provider_windows.provider_plan` is non-null in the physical schema,
  Python table contract, JSON contract, writers, readers, and explicit fixtures.
- Stock publish, staged flush, sync commit, and refresh require explicit provider
  lineage. Refresh requests use `to=provider_as_of`; returned row maxima are not
  lineage authority.
- A same-plan in-scope symbol with no frontier quote advances its `provider_as_of`
  without inventing coverage. A plan change never relabels an untouched window.
- Dataset selection/copy, diagnostics, PIT fundamentals, and fundamental ranking
  derive plan and as-of from exact per-window lineage and fail closed on blank,
  malformed, mixed, missing, or incoherent windows.
- Global `sync_metadata.provider_plan` remains observational metadata only. It is
  not used as Dataset, diagnostics, PIT, or ranking lineage authority.

## Scope

The approved Task 3 production and contract files were changed, together with all
test fixtures found by repository-wide search that explicitly create or insert
`stock_provider_windows`.

The two production propagation sites omitted from the plan list but explicitly
approved in the task brief were also changed:

- `apps/bt/src/application/services/sync_stock_data_fetch.py`
- `apps/bt/src/entrypoints/http/routes/db.py`

They only propagate authoritative sync/request plan and TOPIX/request frontier
into the required interfaces. No Task 4+ corporate-action, projection, or journal
work was absorbed.

The plan also omitted one necessary per-window diagnostics implementation file:

- `apps/bt/src/application/services/db_stats_service.py`

It only exposes `providerPlan` from the exact DuckDB per-window inspection
snapshot. It does not make global metadata authoritative.

## RED/GREEN evidence

### Stage and store contract

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_provider_stock_window.py \
  tests/unit/server/db/test_time_series_store.py \
  -k 'provider_stage or suspended_symbol or plan_change' -q
```

- RED: 111 collected, 6 selected, 6 failed because `ProviderStockStage` did not
  exist.
- GREEN: 6 passed, 105 deselected.

### Sync and refresh propagation

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_stock_refresh_service.py \
  tests/unit/server/services/test_sync_strategies.py \
  -k 'explicit_provider_lineage_frontier' -q
```

- RED: 185 collected, 2 selected, 2 failed because refresh and session commit did
  not accept explicit plan/frontier stage authority.
- GREEN: 2 passed, 183 deselected.

### Diagnostics

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_market_adjusted_metrics.py \
  -k 'per_window_plan' -q
```

- RED: 17 collected, 3 selected, 3 failed with missing `providerPlan`.
- GREEN: 3 passed, 14 deselected.

### Dataset selection

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_dataset_snapshot_selection.py \
  -k 'suspended_symbol or mixed_provider_plans' -q
```

- RED: 3 collected, 2 selected; suspended-symbol coverage passed, while mixed
  plans were incorrectly accepted (1 failure).
- GREEN: 2 passed, 1 deselected.

### Dataset writer preflight

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_dataset_event_time_basis_snapshot.py \
  -k 'window_plan_when_global_metadata_is_stale or mixed_plans_before_destination_mutation' -q
```

- RED: 36 collected, 2 selected, 2 failed. A stale global plan overrode window
  lineage, and mixed plans reached later copy work.
- GREEN: 2 passed, 34 deselected. The mixed-plan test proves destination
  `stock_data_raw`, `stock_master_daily`, `statement_metrics_adjusted`, and
  `daily_valuation` remain at zero rows before failure.

### PIT and ranking consumers

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_fundamentals_pit_reader.py \
  tests/unit/server/services/test_ranking_service.py \
  -k 'provider_plan or mixed_provider_plans' -q
```

- RED: 135 collected, 4 selected, 4 failed because blank and mixed plans did not
  fail closed in either consumer.
- GREEN: 4 passed, 131 deselected.

### Pending-lineage tuple audit

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_time_series_store.py \
  -k 'new_adjustment_factor_marks_fundamentals_pending' -q
```

- RED: 92 collected, 1 selected, 1 failed: pending `source_fingerprint` contained
  the as-of date after the ledger tuple gained a plan field.
- GREEN: 1 passed, 91 deselected; pending and window fingerprints are identical.

### Independent review remediation

Two Important findings were reproduced with direct-store regressions on Task 3
head `58ed2487042a272a0c9492553ca6b61b6f5f3ac0` before production changes:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_time_series_store.py::test_provider_stage_canonicalizes_alias_across_stock_publication \
  tests/unit/server/db/test_time_series_store.py::test_same_plan_older_backfill_preserves_monotonic_provider_frontier -q
```

- RED: 2 collected, 2 failed. Provider-native alias `72030` remained as a
  second raw key beside `7203`; same-plan missing-date backfill regressed
  `provider_as_of` from `2026-02-06` to `2026-02-05` while coverage still ended
  on `2026-02-06`.
- GREEN: 2 passed. Publication canonicalizes row codes before deduplication and
  semantic application, so only canonical keys reach raw, consumer, event, and
  window writes. Same-plan windows retain `max(existing, stage)` as-of, and any
  resulting frontier before coverage is rejected before transaction mutation.

The first full focused run exposed an existing mutation-stat contract:
`409 passed, 1 failed` because canonical deduplication reduced `stats.input`
from two source rows to one. The assertion was not changed. Raw publication was
adjusted to pass all canonical input rows into the existing last-wins semantic
kernel, preserving input accounting while storing one canonical row. The two
new regressions plus the existing last-wins regression then passed `3 passed`.

The existing alias, suspended/no-row, plan-change, mixed-plan, and
pending/fingerprint subset was then run across the store, Dataset, PIT, and
ranking consumers: `19 passed, 268 deselected`.

### Independent re-review remediation

A remaining Important finding was reproduced on clean remediation head
`a574ef5fc3931c438e3edf448a1fdc682715d04a` before production changes:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_time_series_store.py::test_same_plan_older_backfill_preserves_no_row_peer_window -q
```

- RED: 1 collected, 1 failed with `Provider stock stage provider as-of precedes
  existing coverage`. A valid 7203 missing-date backfill at older frontier
  `2026-02-05` was globally aborted because the same-plan 6758 peer had no staged
  row and existing coverage/as-of through `2026-02-06`.
- GREEN: 1 passed. The same-plan no-row branch now validates and stores
  `max(existing_as_of, stage_as_of)`. The 7203 row is inserted, while the 6758
  coverage, plan, as-of, and fingerprint remain unchanged and its frontier stays
  on or after coverage end.

The relation subset covering alias canonicalization, last-wins input accounting,
older touched and no-row-peer backfills, newer suspended advancement, untouched
plan-change windows, mixed plans, and pending fingerprints passed `15 passed,
273 deselected`.

## Final verification

Exact required focused suite:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_provider_stock_window.py \
  tests/unit/server/db/test_time_series_store.py \
  tests/unit/server/services/test_stock_refresh_service.py \
  tests/unit/server/services/test_sync_strategies.py \
  tests/unit/server/services/test_dataset_snapshot_selection.py \
  tests/unit/server/db/test_dataset_event_time_basis_snapshot.py \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py \
  tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Result after independent re-review remediation: `411 passed`, one pre-existing
warning.

Plan-listed PIT/ranking consumers were additionally run in full:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_fundamentals_pit_reader.py \
  tests/unit/server/services/test_ranking_service.py -q
```

Result: `135 passed`, one pre-existing warning.

Quality and contract checks:

```bash
uv run --directory apps/bt ruff check src tests
# All checks passed!

uv run --directory apps/bt pyright src
# 0 errors, 0 warnings, 0 informations

./scripts/check-contract-sync.sh
# PASS

git diff --check
# clean
```

The repository-wide test suite was deliberately not run, per the task brief.

## Generated artifacts

`check-contract-sync.sh` regenerated OpenAPI/TypeScript types in its temporary
check flow and found no committed generated-type drift. The only committed
contract artifact changed by this task is `contracts/market-db-schema-v4.json`.

## Migrations deliberately not provided

No `ALTER TABLE`, automatic migration, compatibility alias, dual read, global
metadata fallback, or latest/current fallback is provided. A database missing the
new non-null per-window plan is incompatible Market v5 state and must be rebuilt
through the already-approved isolated full-rebuild/cutover path.

## Residual risks

- This is a physical schema boundary; operators must not reuse a pre-change
  Market v5 candidate database lacking `provider_plan`.
- The intentionally focused verification does not claim repository-wide test
  coverage.
- Global provider metadata is still written for observability and older external
  tooling may display it, but Task 3 consumers no longer treat it as authority.

## Commit identity

Commit subject: `fix(bt): persist exact provider lineage per stock window`.
The exact content-addressed SHA is recorded in the delivery message after the
commit is created; embedding a commit's own SHA in this tracked report would be
self-referential and would change that SHA.
