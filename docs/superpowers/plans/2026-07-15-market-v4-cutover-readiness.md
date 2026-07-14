# Market v4 Cutover Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make repair and event-time materialization operationally correct, restore TS/API recovery gates, and safely rebuild and verify the active XDG Market Data Plane as schema v4.

**Architecture:** Repair and validation converge on source-derived event-time invariants. Materialization becomes a per-code atomic worker owned by a cooperative, joined lifecycle. CI and web preserve the typed recovery contract, while a new gated cutover workflow performs immutable backup, isolated rehearsal, active rebuild, smoke verification, and explicit restore.

**Tech Stack:** Python 3.12, asyncio/threading, DuckDB, FastAPI/Pydantic, pytest, React 19, TanStack Query/Router, TypeScript, Bun, GitHub Actions.

## Global Constraints

- Market Data Plane supports only schema v4 and `stock_price_adjustment_mode=local_projection_v2_event_time`.
- Do not add in-place v3 migration, dual reads, compatibility aliases, current/latest fallbacks, or read-side materialization.
- `stock_data_raw` remains raw price/corporate-action SoT; incomplete OHLCV rows remain basis events but are not physical valuation observations.
- Per-code basis catalog, segments, adjusted statements, and valuations publish atomically; old bases are retained.
- Dataset physical manifest remains `manifest.v2.json` with payload `schemaVersion: 3`; Dataset contract major v3 is current and must not be removed.
- Cancellation/timeout must await worker termination before database close, reset, restore, or terminal job status.
- Workbench may guide operators to Market DB recovery but must not invoke materialization or calculate financial fallbacks.
- The pre-cutover active data plane must be checksummed and retained; no automatic backup deletion.
- Existing user-owned `.codex/config.toml` remains untouched.

---

### Task 1: Repair Must Materialize Published Fundamentals

**Files:**
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_strategies.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`

**Interfaces:**
- Consumes: `SyncContext.materialize_adjusted_metrics: Callable[[], Awaitable[Any]] | None` and `fundamentals_sync["updated"]`.
- Produces: successful repair implies every published statement batch has completed `adjusted_metrics_pit`.

- [ ] **Step 1: Write repair RED tests**

Add tests with these assertions:

```python
assert result.fundamentalsUpdated == 1
assert materialize_calls == 1
assert events.index("fundamentals_published") < events.index("adjusted_metrics_pit")
assert events.index("adjusted_metrics_pit") < events.index("complete")
```

Add separate cases proving zero published rows skips the callback, a missing callback raises `RuntimeError`, and a callback exception prevents successful repair.

- [ ] **Step 2: Run RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_strategies.py -k 'repair and (materializ or fundamentals)' -q
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py -k 'repair or materializ' -q
```

Expected: the published-row and callback-injection cases fail because repair currently skips the stage.

- [ ] **Step 3: Implement the conditional stage**

In `RepairSyncStrategy.execute`, after statement publication and before completion, implement the equivalent of:

```python
if fundamentals_updated > 0:
    if ctx.materialize_adjusted_metrics is None:
        raise RuntimeError(
            "adjusted_metrics_pit materializer is required after fundamentals repair"
        )
    ctx.on_progress(
        "adjusted_metrics_pit",
        100,
        200,
        "Materializing repaired fundamentals",
    )
    await ctx.materialize_adjusted_metrics()
else:
    ctx.on_progress(
        "adjusted_metrics_pit",
        200,
        200,
        "No statement changes; materialization skipped.",
    )
```

Inject the callback for `initial`, `incremental`, and `repair` in `start_sync`.

- [ ] **Step 4: Run GREEN and quality checks**

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_strategies.py -k 'repair or adjusted_metrics_pit' -q
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py -k 'repair or materializ' -q
uv run --directory apps/bt ruff check src/application/services/sync_service.py src/application/services/sync_strategies.py
uv run --directory apps/bt pyright src/application/services/sync_service.py src/application/services/sync_strategies.py
```

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/sync_service.py apps/bt/src/application/services/sync_strategies.py apps/bt/tests/unit/server/services/test_sync_service.py apps/bt/tests/unit/server/services/test_sync_strategies.py
git commit -m "fix(bt): materialize repaired fundamentals"
```

### Task 2: Source-Derived Adjusted-Metric Freshness Diagnostics

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/valuation_queries.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Regenerate: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Regenerate: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Test: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`
- Test: `apps/bt/tests/unit/server/services/test_db_stats_service.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`

**Interfaces:**
- Produces: `MarketDB.get_adjusted_metrics_source_diagnostics() -> dict[str, int]`.
- Public counters: `sourceStatementKeyCount`, `expectedAdjustedStatementRows`, `missingAdjustedStatementRows`, `extraAdjustedStatementRows`, `staleAdjustedStatementRows`, `wrongBasisAdjustedStatementRows`, `missingDailyValuationRows`, `extraDailyValuationRows`, `wrongBasisDailyValuationRows`.

- [ ] **Step 1: Write real-DuckDB RED tests**

Create exact cases where aggregate counts appear plausible but the source relation is wrong:

```python
diagnostics = market_db.get_adjusted_metrics_source_diagnostics()
assert diagnostics["staleAdjustedStatementRows"] == 1
assert diagnostics["missingAdjustedStatementRows"] == 0
```

Cover source raw EPS changed after materialization, one missing ready-basis row, one source-less extra row, a non-ready/wrong-interval basis, and one missing/wrong-basis valuation observation.

- [ ] **Step 2: Run RED**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_market_adjusted_metrics.py -q
```

Expected: diagnostics method and public fields do not exist.

- [ ] **Step 3: Implement canonical source and exact joins**

Add the exact repository interface
`get_adjusted_metrics_source_diagnostics(table_exists: Callable[[str], bool], fetchone: Callable[[str, Sequence[Any] | None], Any]) -> dict[str, int]`.

The SQL must normalize trailing-zero aliases, prefer the four-digit statement row for `(code, disclosed_date)`, mirror materializer forecast selection, cross source statements with every containing ready basis, and `FULL OUTER JOIN` on `(code, disclosed_date, period_end, period_type, basis_id)`. Use `IS DISTINCT FROM` for raw provenance fields. Derive valuation expectations only from complete raw OHLCV observations joined to ready basis segments; never use `stock_data`.

- [ ] **Step 4: Map diagnostics to stats and validation**

Expose zero-default Pydantic counters. Map wrong-basis to `invalid_lineage`, missing coverage to `incomplete_coverage`, stale/extra rows to `stale`, and attach recovery `adjusted_metrics_pit`. Do not recompute or repair from stats/validate.

- [ ] **Step 5: Run backend GREEN**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_market_adjusted_metrics.py tests/unit/server/services/test_db_stats_service.py tests/unit/server/services/test_db_validation_service.py -q
uv run --directory apps/bt ruff check src/infrastructure/db/market/valuation_queries.py src/infrastructure/db/market/market_db.py src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/schemas/db.py
uv run --directory apps/bt pyright src/infrastructure/db/market/valuation_queries.py src/infrastructure/db/market/market_db.py src/application/services/db_stats_service.py src/application/services/db_validation_service.py src/entrypoints/http/schemas/db.py
```

- [ ] **Step 6: Sync and verify contracts**

```bash
bun run --cwd apps/ts --filter @trading25/contracts bt:sync
bun run --cwd apps/ts --filter @trading25/contracts bt:check
bun run --cwd apps/ts --filter @trading25/contracts test
```

Update the handwritten `adjustedMetrics` interface to exactly match the generated schema; remove obsolete status literals rather than preserving a compatibility union.

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/infrastructure/db/market/valuation_queries.py apps/bt/src/infrastructure/db/market/market_db.py apps/bt/src/application/services/db_stats_service.py apps/bt/src/application/services/db_validation_service.py apps/bt/src/entrypoints/http/schemas/db.py apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py apps/bt/tests/unit/server/services/test_db_stats_service.py apps/bt/tests/unit/server/services/test_db_validation_service.py apps/ts/packages/contracts
git commit -m "feat(bt): validate adjusted metrics from source provenance"
```

### Task 3: Bounded Per-Code Materialization

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/adjustment_basis_queries.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Modify: `apps/bt/src/application/services/adjusted_metrics_materializer.py`
- Test: `apps/bt/tests/unit/server/db/test_adjustment_basis_repository.py`
- Test: `apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py`
- Test: `apps/bt/tests/unit/server/db/test_adjusted_metrics_atomic_publish.py`

**Interfaces:**
- Produces: `MarketDB.list_adjustment_materialization_codes() -> list[str]`.
- Produces: `AdjustedMetricsMaterializer.reconcile_code(code, market_sessions, *, cancel_requested=None) -> AdjustedMetricsBuildResult`.
- Full `reconcile()` becomes an aggregate loop and never holds all-market price/statement/output rows.

- [ ] **Step 1: Write bounded-execution RED tests**

Use spies to assert every statement/raw-price/lineage load receives exactly one normalized code. Add two-code tests proving the first code publishes a complete graph before the second begins and rerunning is idempotent.

```python
assert observed_load_codes == [["1301"], ["7203"]]
assert result.completed_codes == 2
assert result.total_codes == 2
```

- [ ] **Step 2: Run RED**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_adjustment_basis_repository.py tests/unit/server/services/test_adjusted_metrics_materializer.py tests/unit/server/db/test_adjusted_metrics_atomic_publish.py -q
```

- [ ] **Step 3: Add code enumeration**

Implement one SQL query returning sorted distinct normalized codes from `stock_data_raw` union existing basis catalog codes. Do not load raw points to enumerate codes.

- [ ] **Step 4: Refactor to a per-code primitive**

The per-code method loads only that code, builds all retained/open bases for it, and calls the existing atomic `publish_adjusted_basis_materialization` once for that code. Accumulate only counters in the full loop. Rebuild technical metrics once after the full loop.

- [ ] **Step 5: Run GREEN, memory-shape assertions, and quality checks**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_adjustment_basis_repository.py tests/unit/server/services/test_adjusted_metrics_materializer.py tests/unit/server/db/test_adjusted_metrics_atomic_publish.py -q
uv run --directory apps/bt ruff check src/infrastructure/db/market/adjustment_basis_queries.py src/infrastructure/db/market/market_db.py src/application/services/adjusted_metrics_materializer.py
uv run --directory apps/bt pyright src/infrastructure/db/market/adjustment_basis_queries.py src/infrastructure/db/market/market_db.py src/application/services/adjusted_metrics_materializer.py
```

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/infrastructure/db/market/adjustment_basis_queries.py apps/bt/src/infrastructure/db/market/market_db.py apps/bt/src/application/services/adjusted_metrics_materializer.py apps/bt/tests/unit/server/db/test_adjustment_basis_repository.py apps/bt/tests/unit/server/services/test_adjusted_metrics_materializer.py apps/bt/tests/unit/server/db/test_adjusted_metrics_atomic_publish.py
git commit -m "perf(bt): materialize event-time metrics per code"
```

### Task 4: Joined Cancellation, Timeout, and Truthful Progress

**Files:**
- Create: `apps/bt/src/application/services/adjusted_metrics_materialization_run.py`
- Modify: `apps/bt/src/application/services/adjusted_metrics_materializer.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/application/services/generic_job_manager.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/src/entrypoints/http/schemas/db.py`
- Test: `apps/bt/tests/unit/server/services/test_sync_service.py`
- Test: `apps/bt/tests/unit/server/test_generic_job_manager.py`
- Test: `apps/bt/tests/unit/server/test_routes_db_sync.py`

**Interfaces:**
- Produces: synchronous `MaterializationCancellationToken` backed by `threading.Event`.
- Produces: immutable progress `{stage, completed_codes, total_codes, current_code, published_basis_count}`.
- Produces: async `run_shielded_materialization(materializer: AdjustedMetricsMaterializer, *, timeout_seconds: float, on_progress: Callable[[MaterializationProgress], None]) -> AdjustedMetricsBuildResult` that joins the worker after cancel/timeout.

- [ ] **Step 1: Write real blocking-worker RED tests**

Use `threading.Event` barriers, not a mocked `asyncio.wait_for`. Assert `close`, `on_finish`, reset, and terminal `CANCELLED` remain false while the worker is blocked; after releasing a code boundary, assert the worker exits before teardown/status transition.

- [ ] **Step 2: Run RED**

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py tests/unit/server/test_generic_job_manager.py tests/unit/server/test_routes_db_sync.py -k 'cancel or timeout or materializ or progress' -q
```

- [ ] **Step 3: Implement the shared run lifecycle**

Create a worker task with `asyncio.to_thread`, await it through `asyncio.shield`, and on `CancelledError` or timeout call `token.request_cancel()` then await the shielded worker before re-raising. Materializer checks the token only between code transactions.

- [ ] **Step 4: Fix job terminal semantics and progress**

For `GenericJobManager.cancel_job(wait=True)`, set cancellation intent, cancel/await the task, then assign terminal cancelled status. Keep `wait=False` immediate behavior for Dataset cancel. Add optional code-level fields to sync/materialization progress and expose a standalone materialization cancel route.

- [ ] **Step 5: Sync OpenAPI and run GREEN**

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py tests/unit/server/test_generic_job_manager.py tests/unit/server/test_routes_db_sync.py -q
uv run --directory apps/bt ruff check src/application/services/adjusted_metrics_materialization_run.py src/application/services/adjusted_metrics_materializer.py src/application/services/sync_service.py src/application/services/generic_job_manager.py src/entrypoints/http/routes/db.py src/entrypoints/http/schemas/db.py
uv run --directory apps/bt pyright src/application/services/adjusted_metrics_materialization_run.py src/application/services/adjusted_metrics_materializer.py src/application/services/sync_service.py src/application/services/generic_job_manager.py src/entrypoints/http/routes/db.py src/entrypoints/http/schemas/db.py
bun run --cwd apps/ts --filter @trading25/contracts bt:sync
bun run --cwd apps/ts --filter @trading25/contracts bt:check
```

- [ ] **Step 6: Commit**

```bash
git add apps/bt/src/application/services/adjusted_metrics_materialization_run.py apps/bt/src/application/services/adjusted_metrics_materializer.py apps/bt/src/application/services/sync_service.py apps/bt/src/application/services/generic_job_manager.py apps/bt/src/entrypoints/http/routes/db.py apps/bt/src/entrypoints/http/schemas/db.py apps/bt/tests/unit/server/services/test_sync_service.py apps/bt/tests/unit/server/test_generic_job_manager.py apps/bt/tests/unit/server/test_routes_db_sync.py apps/ts/packages/contracts
git commit -m "fix(bt): join materialization workers before teardown"
```

### Task 5: Restore TypeScript Tests as a Required CI Gate

**Files:**
- Modify: `.github/workflows/ci.yml`
- Test: `apps/bt/tests/unit/scripts/test_ci_workflow.py`

**Interfaces:**
- Produces: required `ts-tests` job running `bun run workspace:test`.
- Produces: unconditional final `ci-gate` for product CI whose `needs` includes `ts-tests` and all required backend/quality jobs.

- [ ] **Step 1: Write workflow RED tests**

Parse the workflow and assert:

```python
assert jobs["ts-tests"]["steps"][-1]["run"] == "bun run workspace:test"
assert "ts-tests" in jobs["ci-gate"]["needs"]
assert jobs["ci-gate"]["if"] == "always()"
```

Also assert `ts-tests` does not set `SKIP_TS_TESTS`.

- [ ] **Step 2: Run RED**

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_ci_workflow.py -q
```

- [ ] **Step 3: Add the CI jobs**

Use Bun `1.3.8`, `bun install --frozen-lockfile`, and `bun run workspace:test`. Make `ci-gate` fail when any required dependency is failure/cancelled and succeed when path filtering intentionally skips product CI.

- [ ] **Step 4: Run local CI-equivalent checks**

```bash
uv run --directory apps/bt pytest tests/unit/scripts/test_ci_workflow.py -q
bun run --cwd apps/ts workspace:test
```

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml apps/bt/tests/unit/scripts/test_ci_workflow.py
git commit -m "ci: require TypeScript workspace tests"
```

### Task 6: Preserve Typed PIT Errors and Guide Workbench Recovery

**Files:**
- Modify: `apps/ts/packages/api-clients/src/base/http-client.ts`
- Modify: `apps/ts/packages/api-clients/src/base/http-client.test.ts`
- Modify: `apps/ts/packages/web/src/hooks/useFundamentals.ts`
- Modify: `apps/ts/packages/web/src/hooks/useFundamentals.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`

**Interfaces:**
- Produces: typed `HttpRequestError.details`, `correlationId`, `reason`, and `recovery` parsed from unified FastAPI errors.
- Produces: `shouldRetryFundamentals(failureCount, error) -> boolean`.
- Produces: `/market-db#adjusted-metrics` recovery link only for 409 `adjusted_metrics_pit`.

- [ ] **Step 1: Write shared-client RED tests**

Use a real 409 JSON body and assert:

```typescript
expect(error).toMatchObject({
  status: 409,
  correlationId: 'corr-1',
  reason: 'pit_snapshot_inconsistent',
  recovery: 'adjusted_metrics_pit',
});
```

- [ ] **Step 2: Implement strict unified-error parsing**

Accept only the backend object shape with string `message`, optional array `details`, and optional string `correlationId`. Preserve the raw body for diagnostics but do not infer recovery from message text.

- [ ] **Step 3: Write and implement retry RED/GREEN**

Assert 404/409/422 return false; network, timeout, and 5xx return true only while `failureCount < 2`.

- [ ] **Step 4: Write and implement Workbench recovery UI**

Destructure `error` from the existing page-level hook. Display backend message and correlation ID. For the exact recovery code, render a router link to `/market-db#adjusted-metrics`; add `id="adjusted-metrics"` to the Market DB card. Do not call a mutation.

- [ ] **Step 5: Run TS checks**

```bash
bun run --cwd apps/ts --filter @trading25/api-clients test
bun run --cwd apps/ts --filter @trading25/web test -- src/hooks/useFundamentals.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
bun run --cwd apps/ts quality:typecheck
bun run --cwd apps/ts quality:lint
```

- [ ] **Step 6: Commit**

```bash
git add apps/ts/packages/api-clients/src/base/http-client.ts apps/ts/packages/api-clients/src/base/http-client.test.ts apps/ts/packages/web/src/hooks/useFundamentals.ts apps/ts/packages/web/src/hooks/useFundamentals.test.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx apps/ts/packages/web/src/pages/SettingsPage.tsx
git commit -m "feat(web): surface Fundamentals PIT recovery"
```

### Task 7: Expose Dataset Event-Time Lineage

**Files:**
- Modify: `apps/bt/src/entrypoints/http/schemas/dataset.py`
- Modify: `apps/bt/src/application/services/dataset_service.py`
- Modify: `apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/web/src/components/Backtest/DatasetInfoDialog.tsx`
- Test: `apps/bt/tests/unit/server/test_dataset_service.py`
- Test: `apps/ts/packages/web/src/hooks/useDataset.test.tsx`
- Test: `apps/ts/packages/web/src/components/Backtest/DatasetInfoDialog.test.tsx`

**Interfaces:**
- Adds required snapshot fields: `schemaVersion`, `sourceMarketSchemaVersion`, `stockPriceAdjustmentMode`.

- [ ] **Step 1: Write backend RED**

Assert a validated manifest returns exactly `3`, `4`, and `local_projection_v2_event_time`; malformed/missing lineage remains unsupported rather than returning null compatibility fields.

- [ ] **Step 2: Implement backend response mapping and run GREEN**

```bash
uv run --directory apps/bt pytest tests/unit/server/test_dataset_service.py -q
uv run --directory apps/bt ruff check src/entrypoints/http/schemas/dataset.py src/application/services/dataset_service.py src/infrastructure/db/market/dataset_snapshot_reader.py
uv run --directory apps/bt pyright src/entrypoints/http/schemas/dataset.py src/application/services/dataset_service.py src/infrastructure/db/market/dataset_snapshot_reader.py
```

- [ ] **Step 3: Sync contracts and write UI RED**

```bash
bun run --cwd apps/ts --filter @trading25/contracts bt:sync
```

Assert the dialog renders `Payload schema 3`, `Market schema 4`, and `local_projection_v2_event_time`.

- [ ] **Step 4: Implement UI and run GREEN**

```bash
bun run --cwd apps/ts --filter @trading25/contracts bt:check
bun run --cwd apps/ts --filter @trading25/web test -- src/hooks/useDataset.test.tsx src/components/Backtest/DatasetInfoDialog.test.tsx
bun run --cwd apps/ts quality:typecheck
```

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/entrypoints/http/schemas/dataset.py apps/bt/src/application/services/dataset_service.py apps/bt/src/infrastructure/db/market/dataset_snapshot_reader.py apps/bt/tests/unit/server/test_dataset_service.py apps/ts/packages/contracts apps/ts/packages/web/src/hooks/useDataset.test.tsx apps/ts/packages/web/src/components/Backtest/DatasetInfoDialog.tsx apps/ts/packages/web/src/components/Backtest/DatasetInfoDialog.test.tsx
git commit -m "feat(dataset): expose event-time lineage metadata"
```

### Task 8: Retire Executable Market v3 Guards and Test Shims

**Files:**
- Modify: `apps/bt/src/domains/analytics/readonly_duckdb_support.py`
- Modify: `apps/bt/src/domains/analytics/stop_limit_daily_classification.py`
- Modify: `apps/bt/src/domains/analytics/topix_gap_intraday_distribution.py`
- Modify: `apps/bt/src/domains/analytics/topix_close_stock_overnight_distribution.py`
- Modify/Delete: `apps/bt/tests/unit/domains/analytics/conftest.py`
- Test: corresponding four analytics test modules and dependent fixture users.

**Interfaces:**
- Produces: `require_market_v4_compatibility(conn: duckdb.DuckDBPyConnection, *, required_tables: Collection[str]) -> None`.

- [ ] **Step 1: Write v3/wrong-mode RED tests**

Create physical schema v3 and schema v4/wrong-mode fixtures; assert both fail with reset guidance. A valid v4/event-time fixture passes only when consumer-required tables exist.

- [ ] **Step 2: Implement shared guard and migrate three modules**

Delete `_assert_schema_v3` functions and duplicated version SQL. The shared guard requires exact schema `4`, exact adjustment mode, and caller-supplied tables.

- [ ] **Step 3: Remove implicit PIT fixture synthesis**

Delete the analytics conftest shim that derives PIT master/membership from current `stocks`. Add explicit `stock_master_daily` and membership rows to each affected fixture so tests cannot hide future-leak behavior.

- [ ] **Step 4: Run analytics GREEN and quality checks**

```bash
uv run --directory apps/bt pytest tests/unit/domains/analytics/test_stop_limit_daily_classification.py tests/unit/domains/analytics/test_stop_limit_buy_only_next_close_followthrough.py tests/unit/domains/analytics/test_topix_gap_intraday_distribution.py tests/unit/domains/analytics/test_topix_close_stock_overnight_distribution.py -q
uv run --directory apps/bt ruff check src/domains/analytics/readonly_duckdb_support.py src/domains/analytics/stop_limit_daily_classification.py src/domains/analytics/topix_gap_intraday_distribution.py src/domains/analytics/topix_close_stock_overnight_distribution.py
uv run --directory apps/bt pyright src/domains/analytics/readonly_duckdb_support.py src/domains/analytics/stop_limit_daily_classification.py src/domains/analytics/topix_gap_intraday_distribution.py src/domains/analytics/topix_close_stock_overnight_distribution.py
```

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/domains/analytics apps/bt/tests/unit/domains/analytics
git commit -m "refactor(bt): require Market v4 in standalone analytics"
```

### Task 9: Build the Gated Market v4 Cutover Workflow

**Files:**
- Create: `apps/bt/src/application/services/market_v4_cutover.py`
- Create: `apps/bt/src/cli_bt/commands/market_cutover.py`
- Modify: `apps/bt/src/cli_bt/app.py`
- Create: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`
- Create: `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`
- Modify: `scripts/dev-bt-server.sh` only if a credential-safe wrapper is required.

**Interfaces:**
- CLI phases: `preflight`, `backup`, `rehearse`, `cutover`, `restore`, `smoke`.
- Backup manifest: recursive relative path, byte size, SHA-256, source schema/mode, timestamp; immutable destination.
- Cutover report: phase statuses, durations, commands/API checks, schema/coverage results, redacted paths, backup manifest reference.

- [ ] **Step 1: Write filesystem/preflight RED tests**

Use temporary roots and assert insufficient space, active jobs, a nonempty WAL, missing checkpoint/quiescence, existing backup destination, and checksum mismatch fail closed. Assert restore requires an explicit backup ID and never deletes the backup.

- [ ] **Step 2: Implement pure backup/restore primitives**

FastAPI must be stopped for backup. Obtain a writable DuckDB connection, run `CHECKPOINT`, close it, reject a remaining nonempty WAL, copy DB+Parquet, calculate SHA-256, fsync report/manifest, and verify the copy before returning success.

- [ ] **Step 3: Write API smoke RED tests**

With fake/TestClient adapters, require schema v4/mode, adjusted-metric validation, GET/POST Fundamentals semantic parity, screening result, fundamental ranking, and Dataset create/info/open with payload schema 3/source Market v4/event-time.

- [ ] **Step 4: Implement reusable smoke and isolated rehearsal**

Use an explicit isolated XDG root and separate process. Run initial sync with `resetBeforeSync=true` and `enforceBulkForStockData=true`, poll sync/materialization jobs, validate, then run the smoke suite. Never symlink the active dataset directory. Never serialize credentials.

- [ ] **Step 5: Implement active cutover gate and explicit restore**

Require a passing rehearsal report ID and verified backup before reset. On any active post-reset failure, close the app/client and restore the recorded backup. Keep both rehearsal and active reports.

- [ ] **Step 6: Run unit/CLI/security checks**

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover.py tests/unit/cli_bt/test_market_cutover_cli.py -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py src/cli_bt/commands/market_cutover.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py src/cli_bt/commands/market_cutover.py
python3 scripts/check-privacy-leaks.py
```

- [ ] **Step 7: Commit**

```bash
git add apps/bt/src/application/services/market_v4_cutover.py apps/bt/src/cli_bt/commands/market_cutover.py apps/bt/src/cli_bt/app.py apps/bt/tests/unit/server/services/test_market_v4_cutover.py apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py scripts/dev-bt-server.sh
git commit -m "feat(bt): add gated Market v4 cutover workflow"
```

### Task 10: Rehearse, Cut Over, Retire v3 Documentation, and Verify Everything

**Files:**
- Modify: `docs/research-pit-invalidation-register.md`
- Move: `docs/market-duckdb-sot-v3-plan.md` to `docs/archive/market-duckdb-sot-v3-plan.md`
- Modify: `docs/README.md`
- Modify: `docs/phase6-production-smoke-report.md`
- Modify: `apps/bt/docs/experiments/market-behavior/**/README.md` current rerun instructions.
- Modify: `.codex/skills/bt-market-sync-strategies/SKILL.md`
- Modify: `.codex/skills/bt-database-management/SKILL.md`
- Modify: `.codex/skills/bt-financial-analysis/SKILL.md`
- Modify: `.codex/skills/ts-api-endpoints/SKILL.md`
- Modify: `.codex/skills/ts-financial-analysis/SKILL.md`
- Create: redacted rehearsal/cutover report under `docs/operations/` only after privacy validation.

**Interfaces:**
- Consumes: all prior task gates and the `bt market-cutover` workflow.
- Produces: active XDG schema v4/event-time data plane, retained checksummed backup, current runbook/report, and no executable/current-doc schema v3 support claims.

- [ ] **Step 1: Run the complete pre-cutover software gate**

```bash
uv run --directory apps/bt pytest tests/
uv run --directory apps/bt ruff check src/
uv run --directory apps/bt pyright src/
bun run --cwd apps/ts workspace:test
bun run --cwd apps/ts quality:typecheck
bun run --cwd apps/ts quality:deps:audit
bun run --cwd apps/ts quality:lint
./scripts/check-contract-sync.sh
python3 scripts/skills/refresh_skill_references.py --check
python3 scripts/skills/audit_skills.py --strict-legacy
```

- [ ] **Step 2: Run isolated rehearsal**

```bash
uv run --directory apps/bt bt market-cutover preflight --target rehearsal
uv run --directory apps/bt bt market-cutover rehearse
uv run --directory apps/bt bt market-cutover smoke --target rehearsal
```

Expected: report is passing and records schema 4, event-time mode, validation, API parity, screening/ranking, and Dataset v3 lineage.

- [ ] **Step 3: Back up and cut over the active XDG root**

Stop FastAPI/writers, then run:

```bash
uv run --directory apps/bt bt market-cutover preflight --target active
uv run --directory apps/bt bt market-cutover backup
uv run --directory apps/bt bt market-cutover cutover --require-rehearsal-pass
uv run --directory apps/bt bt market-cutover smoke --target active
```

Expected: active DB is schema 4/event-time and the immutable backup manifest verifies. If cutover fails, run the explicit restore command recorded by the tool and repeat preflight; do not add compatibility code.

- [ ] **Step 4: Retire current v3 wording without deleting current contract majors**

Update current docs, experiment rerun instructions, and skills. Archive the old physical-v3 plan. Preserve `contracts/market-db-schema-v3.json`, `contracts/dataset-db-schema-v3.json`, payload `schemaVersion: 3`, and negative tests that reject old physical databases.

- [ ] **Step 5: Run privacy and stale-reference scans**

```bash
python3 scripts/check-privacy-leaks.py
rg -n "Market schema v3|schema_version.?==.?3|_assert_schema_v3|local_projection_v1" apps/bt/src apps/bt/docs docs .codex/skills --glob '!docs/archive/**'
python3 scripts/skills/refresh_skill_references.py --check
python3 scripts/skills/audit_skills.py --strict-legacy
git diff --check
```

Expected: production/current-doc matches are absent except explicit incompatibility diagnostics and negative tests.

- [ ] **Step 6: Commit operational evidence and retirement**

```bash
git add docs apps/bt/docs .codex/skills
git commit -m "docs: complete Market v4 production cutover"
```

- [ ] **Step 7: Final independent review and completion audit**

Generate one review package from `f7aa5d0c` through `HEAD`. The reviewer must verify all twelve design acceptance criteria, no compatibility path, worker lifecycle safety, source-derived validation, CI gating, typed recovery, backup integrity, isolated rehearsal, active schema/mode, production smoke evidence, and current-doc retirement. Fix and re-review every Critical or Important finding before marking the goal complete.
