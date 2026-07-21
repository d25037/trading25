# Market Sync Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issue #507 and reduce `market.duckdb` operations to explicit RESET initial sync and daily incremental sync.

**Architecture:** Daily raw volume remains integer while provider-adjusted daily volume is a float propagated unchanged through Market v5, Dataset v4, analytics, and APIs. The sync API exposes only `initial` and `incremental`; initial requires an explicit reset request and resets the fixed Market root before opening the new schema, while incremental owns all normal updates and warning backfill.

**Tech Stack:** Python 3.12, FastAPI, DuckDB, SQLAlchemy Core, pytest, React 19, TypeScript, Bun, OpenAPI.

## Global Constraints

- `SyncRequest.mode` accepts exactly `initial` and `incremental`, with default `incremental`.
- `initial` requires `resetBeforeSync: true`; `incremental` requires `resetBeforeSync: false`.
- RESET deletes only the fixed Market `market.duckdb`, WAL, and market Parquet; it never deletes `portfolio.db` or `datasets/`.
- Keep writer lease, in-process locking, path confinement, symlink/special-file rejection, normal compaction, and maintenance finalization.
- Remove `auto`, `repair`, reset eligibility, and all cutover/rehearsal/backup/journal/promotion/retained-runtime product surfaces.
- Keep physical Market schema version 5 and Dataset manifest payload schema version 4; add no migration or compatibility read.
- `stock_data_raw.volume` and minute volume remain integer; only `stock_data_raw.adjusted_volume` and `stock_data.volume` become `DOUBLE`.
- Unknown sync strategies must fail explicitly and must never fall back to destructive initial.
- Follow RED-GREEN-REFACTOR for every behavior change.

---

### Task 1: Preserve fractional provider-adjusted volume in Market v5

**Files:**
- Modify: `apps/bt/src/application/services/stock_data_row_builder.py`
- Modify: `apps/bt/src/shared/provider_stock_window.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_schema.py`
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/src/infrastructure/db/market/tables.py`
- Modify: `contracts/market-db-schema-v4.json`
- Test: `apps/bt/tests/unit/server/services/test_stock_data_row_builder.py`
- Test: `apps/bt/tests/unit/server/services/test_provider_stock_window.py`
- Test: `apps/bt/tests/unit/server/db/test_time_series_store.py`
- Test: `apps/bt/tests/unit/server/db/test_market_db.py`

**Interfaces:**
- Consumes: J-Quants daily `Vo` and `AdjVo`.
- Produces: raw rows with `volume: int` and `adjusted_volume: float`, plus consumer rows with `volume: float`.

- [ ] **Step 1: Write failing row-builder and provider-window tests**

Add tests using `Vo=8730892` and `AdjVo=87308.9` that assert the adjusted value is preserved. Add parameterized invalid adjusted values `-0.1`, `float("nan")`, and `float("inf")`, plus a fractional raw `Vo`, and assert rejection.

- [ ] **Step 2: Run the focused tests and verify expected failures**

Run: `uv run --directory apps/bt pytest tests/unit/server/services/test_stock_data_row_builder.py tests/unit/server/services/test_provider_stock_window.py -q`

Expected: fractional `AdjVo` tests fail because `_coerce_int` and integral provider-window validation reject/truncate it.

- [ ] **Step 3: Implement float parsing and precision-aware validation**

Use `_coerce_float` only for `AdjVo`, reject negative values, retain `_coerce_int` for raw `Vo`, store `adjusted_volume` as `float`, and compare it with `raw_volume / cumulative_factor` using a focused absolute tolerance matching published 0.1 precision without integer casts.

- [ ] **Step 4: Write failing DuckDB type/persistence/drift tests**

Assert `PRAGMA table_info` reports `DOUBLE` for the two adjusted daily columns, `BIGINT` for raw/minute volume, and that `87308.9` survives publish, consumer projection, Parquet export/reopen, and factor-one drift detection.

- [ ] **Step 5: Run the DuckDB tests and verify expected failures**

Run: `uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py tests/unit/server/db/test_market_db.py -q`

Expected: old `BIGINT` columns round the value or schema assertions fail.

- [ ] **Step 6: Change the Market physical schema and mirrors**

Change only `stock_data_raw.adjusted_volume` and `stock_data.volume` from integer types to `DOUBLE`/SQLAlchemy `Float`; update drift comparison to use float tolerance and update `contracts/market-db-schema-v4.json` accordingly.

- [ ] **Step 7: Verify Task 1**

Run the four focused test files, then `uv run --directory apps/bt ruff check src/application/services/stock_data_row_builder.py src/shared/provider_stock_window.py src/infrastructure/db/market`.

Expected: all pass with no lint errors.

### Task 2: Preserve fractional volume through Dataset, analytics, and APIs

**Files:**
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/snapshot_contract.py`
- Modify: `contracts/dataset-db-schema-v4.json`
- Modify: `apps/bt/src/domains/analytics/daily_ranking_event_time_prices.py`
- Modify: `apps/bt/src/application/services/market_data_service.py`
- Modify: `apps/bt/src/application/services/watchlist_prices_service.py`
- Modify: `apps/bt/src/application/contracts/market_data.py`
- Modify: `apps/bt/src/application/contracts/dataset_data.py`
- Modify: `apps/bt/src/application/contracts/watchlist_prices.py`
- Test: `apps/bt/tests/unit/server/db/test_dataset_event_time_basis_snapshot.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py`
- Test: `apps/bt/tests/unit/server/routes/test_market_data.py`

**Interfaces:**
- Consumes: Task 1 `DOUBLE` adjusted daily volume.
- Produces: Dataset v4 and daily API numeric volume without integer coercion; minute API remains integer.

- [ ] **Step 1: Add failing Dataset, ranking, and API regression tests**

Create Dataset source data with `adjusted_volume=87308.9`, assert Dataset DuckDB/Parquet preserves it, assert event-time ranking returns the fractional value, and assert daily market responses return it while minute responses remain integer.

- [ ] **Step 2: Run focused tests and verify integer casts fail them**

Run: `uv run --directory apps/bt pytest tests/unit/server/db/test_dataset_event_time_basis_snapshot.py tests/unit/domains/analytics/test_daily_ranking_event_time_prices.py tests/unit/server/routes/test_market_data.py -q`

- [ ] **Step 3: Remove downstream integer coercion**

Use `DOUBLE` in Dataset stock projections/contracts, remove `CAST(... AS BIGINT)` for adjusted daily volume, make event-time null and value branches `DOUBLE`, split daily float volume conversion from minute integer conversion, and change daily market/dataset/watchlist response models to `float`.

- [ ] **Step 4: Verify Task 2**

Run the focused tests plus `uv run --directory apps/bt pytest tests/unit/server/test_dataset_writer.py tests/unit/server/test_dataset_snapshot_reader.py -q` and ruff on modified Python files.

Expected: fractional values survive all paths; integer minute/raw tests remain green.

### Task 3: Reduce backend sync to RESET initial and incremental

**Files:**
- Modify: `apps/bt/src/application/contracts/market_data_plane.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_writer_resources.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/test_routes_db_sync.py`

**Interfaces:**
- Produces: `start_sync(initial|incremental)` where initial resets before opening new handles and incremental validates/uses current handles.

- [ ] **Step 1: Add failing request/strategy/reset tests**

Assert only two modes validate; initial without reset and incremental with reset return 422; missing/malformed/incompatible roots can start reset initial without opening old handles; unknown strategy raises; incremental retains missing-date and fundamentals recovery.

- [ ] **Step 2: Run focused sync tests and verify failures**

Run: `uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py tests/unit/server/services/test_sync_strategies.py tests/unit/server/test_routes_db_sync.py -q`

- [ ] **Step 3: Simplify the sync contract and orchestration**

Remove AUTO/REPAIR resolution and repair strategy. Make `get_strategy` raise for unknown input. For initial, acquire/reset/open through the existing guarded writer resource factory before `_prepare_market_db_for_sync`; do not require or validate old Market handles. Preserve the original reset failure if no writer session/finalizer exists. Rename stale `reset_and_open_v4` to `reset_and_open`.

- [ ] **Step 4: Remove reset eligibility from stats/validation**

Delete `resetBeforeSyncEligible` and its protocol/model/UI contract source. Change recovery recommendations from cutover/repair to RESET initial or incremental as appropriate.

- [ ] **Step 5: Verify Task 3**

Run the focused sync tests, db stats/validation tests, ruff, and pyright for modified backend modules.

Expected: two-mode contract and malformed-root reset tests pass; no AUTO/REPAIR fallback remains.

### Task 4: Delete cutover/rehearsal runtime and active guidance

**Files:**
- Delete: `apps/bt/src/application/services/market_v4_cutover/`
- Delete: `apps/bt/src/entrypoints/cli/market_cutover.py`
- Delete: `apps/bt/config/strategies/production/cutover_smoke.yaml`
- Delete: cutover-only tests under `apps/bt/tests/unit/server/services/` and `apps/bt/tests/unit/cli_bt/`
- Modify: `apps/bt/src/entrypoints/cli/__init__.py`
- Modify: `apps/bt/src/entrypoints/http/app.py`
- Modify: `apps/bt/src/infrastructure/db/market/duckdb_connection.py`
- Modify: repository skill audit/reference scripts and tests

**Interfaces:**
- Preserves: normal managed-root preparation, writer lease, compaction atomic file exchange, clients, shutdown cleanup.
- Removes: retained-runtime environment/FD adoption and market-cutover CLI.

- [ ] **Step 1: Add/adjust failing absence and normal-startup tests**

Assert CLI help has no `market-cutover`, normal app startup no longer branches on retained runtime, and skill audit/reference generation does not require cutover files or runbook.

- [ ] **Step 2: Delete cutover-owned code and detach imports**

Delete the dedicated package, CLI, smoke strategy, and dedicated tests. Remove retained runtime branches from app startup and cutover temp-directory handling while preserving default DuckDB temp handling and normal resource cleanup.

- [ ] **Step 3: Update repository skill governance**

Remove cutover/repair/auto requirements from `.codex/skills/bt-market-sync-strategies`, `bt-database-management`, `bt-financial-analysis`, `ts-financial-analysis`, and CLI reference generation/audit expectations.

- [ ] **Step 4: Verify Task 4**

Run CLI/app/skill audit tests, `rg` active source and skills for cutover product references, and ruff/pyright on affected modules.

Expected: no active runtime import or CLI surface references deleted modules.

### Task 5: Simplify Web UI, regenerate contracts, and align active docs

**Files:**
- Modify: `apps/ts/packages/web/src/components/Settings/SyncModeSelect.tsx`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SettingsMarketDbDiagnostics.ts`
- Modify: `apps/ts/packages/web/src/pages/SettingsMarketDbPanels.tsx`
- Modify: `apps/ts/packages/web/src/hooks/useDbSync.ts`
- Modify: `apps/ts/packages/web/src/components/Settings/SyncStatusCard.tsx`
- Modify: Web and contract tests
- Regenerate: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Regenerate: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `AGENTS.md`, `README.md`, and active architecture/runbook guidance
- Delete: `docs/runbooks/market-v5-cutover.md`

**Interfaces:**
- Consumes: Task 3 OpenAPI two-mode contract.
- Produces: Web UI with incremental default and always-confirmed RESET initial.

- [ ] **Step 1: Write failing Web and contract tests**

Assert only Initial RESET and Incremental options render, default start sends incremental, selecting initial always opens RESET confirmation and sends `resetBeforeSync: true`, and Auto/Repair/Warning Recovery/cutover guidance is absent. Add type tests rejecting `auto` and `repair`.

- [ ] **Step 2: Run focused frontend tests and verify failures**

Run: `(cd apps/ts && bun test packages/web/src/pages/SettingsPage.test.tsx packages/web/src/hooks/useDbSync.test.tsx packages/web/src/components/Settings/SyncStatusCard.test.tsx)`

- [ ] **Step 3: Implement the two-action UI**

Default to incremental, remove optional reset switch/eligibility and repair card, always confirm Initial RESET, normalize request construction, and update diagnostics to recommend incremental for recoverable gaps.

- [ ] **Step 4: Regenerate OpenAPI contracts**

Run: `bun run --filter @trading25/contracts bt:sync`

Expected: OpenAPI exposes only two sync modes and daily adjusted volume as number; raw/minute volume stays integer.

- [ ] **Step 5: Align active documentation**

Update `AGENTS.md`, `README.md`, architecture SoT guidance, and security runbook to describe RESET initial/incremental only. Delete the cutover runbook; retain historical superpowers documents without active links. Repository skill guidance is owned by Task 4.

- [ ] **Step 6: Verify Task 5**

Run focused Web tests, contract checks, `(cd apps/ts && bun run quality:typecheck)`, and `rg` checks for active Auto/Repair/Cutover guidance.

### Task 6: Integration verification and review

**Files:**
- Modify only if verification identifies a regression.

- [ ] **Step 1: Run backend integration suites**

Run the complete market DB/sync/dataset focused suites, followed by `uv run --directory apps/bt ruff check src` and `uv run --directory apps/bt pyright src`.

- [ ] **Step 2: Run frontend/contract verification**

Run `(cd apps/ts && bun run workspace:test)`, `(cd apps/ts && bun run quality:typecheck)`, and `(cd apps/ts && bun run quality:lint)`.

- [ ] **Step 3: Run repository consistency checks**

Run `git diff --check`, repository skill audits, and search active code/docs for removed product surfaces. Confirm deleted modules have no imports.

- [ ] **Step 4: Review the full diff against the design**

Verify every acceptance criterion in `docs/superpowers/specs/2026-07-22-market-sync-simplification-design.md`, fix Critical/Important findings, and rerun covering tests.
