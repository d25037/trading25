# Market v5 Review Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every Critical and Important pre-merge review finding in PR #493 while preserving Market v5/provider-adjusted semantics and the isolated atomic cutover as the only pre-v5 upgrade path.

**Architecture:** Enforce the migration boundary before any destructive callback, move provider vintage authority into each symbol window, make publication retryable and self-repairing, and journal exact activation identities before filesystem exchange. Each task begins with a failing regression test and ends with focused verification and an independently reviewable commit.

**Tech Stack:** Python 3.12, FastAPI, DuckDB, Parquet, Pydantic, pytest, React 19, TypeScript, Bun, OpenAPI generation.

## Global Constraints

- Market v4 and earlier remain incompatible; do not add in-place migration, dual read, compatibility aliases, or current/latest fallback.
- `bt market-cutover cutover` is the only supported pre-v5-to-v5 upgrade path.
- `stock_data_raw` preserves provider raw provenance and `stock_data` is the provider-adjusted consumer projection.
- Provider plan, request frontier, observed coverage, and fingerprint are exact per-symbol window lineage.
- Dataset schema 4 lineage must be derived from selected per-window records, never mutable global metadata.
- DuckDB, Parquet, activation journal, immutable backup, quarantine, and report ownership must fail closed on ambiguity.
- Every behavior change follows red-green testing; do not weaken or delete a failing assertion to make the suite pass.

---

### Task 1: Reject incompatible live reset before mutation

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Modify: `apps/bt/src/application/services/sync_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/db.py`
- Modify: `apps/bt/tests/unit/server/services/test_sync_service.py`
- Modify: `apps/bt/tests/unit/server/test_routes_db_sync.py`

**Interfaces:**
- Produces: `MarketDb.is_reset_before_sync_eligible() -> bool`
- Produces: `IncompatibleMarketResetError`
- Preserves: compatible Market v5 `initial + resetBeforeSync` maintenance behavior.

- [ ] **Step 1: Write the failing service tests**

Add parameterized cases for schema versions `4`, `3`, missing schema, Market v5 with missing/wrong adjustment mode, malformed schema validation, and legacy snapshot state. Assert `start_sync()` raises before `create_job()` and before `reset_market_snapshot()`.

```python
@pytest.mark.parametrize(
    ("eligible", "schema_version", "adjustment_mode"),
    [
        (False, 4, "local_projection_v2_event_time"),
        (False, 3, None),
        (False, None, None),
        (False, 5, "local_projection_v2_event_time"),
    ],
)
async def test_start_sync_rejects_incompatible_reset_before_job_creation(
    eligible: bool,
    schema_version: int | None,
    adjustment_mode: str | None,
) -> None:
    market_db = DummyMarketDb(
        reset_before_sync_eligible=eligible,
        schema_version=schema_version,
        adjustment_mode=adjustment_mode,
    )
    reset_calls = 0

    async def reset_market_snapshot() -> None:
        nonlocal reset_calls
        reset_calls += 1

    with pytest.raises(IncompatibleMarketResetError, match="market-cutover cutover"):
        await SyncService().start_sync(
            "initial",
            market_db=market_db,
            reset_before_sync=True,
            reset_market_snapshot=reset_market_snapshot,
        )

    assert reset_calls == 0
    assert market_db.create_job_calls == 0
```

- [ ] **Step 2: Run the service test and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py -k incompatible_reset -q
```

Expected: FAIL because reset eligibility is not checked before job creation.

- [ ] **Step 3: Add the read-only eligibility predicate and service gate**

Implement the predicate without calling schema creation or metadata writers.

```python
def is_reset_before_sync_eligible(self) -> bool:
    validation = self.validate_schema()
    return (
        self.get_market_schema_version() == MARKET_SCHEMA_VERSION
        and self.get_stock_price_adjustment_mode()
        == PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE
        and validation.get("valid") is True
        and not self.is_legacy_stock_price_snapshot()
    )
```

Add `is_reset_before_sync_eligible()` to `SyncServiceMarketDbLike`. In `start_sync()`, perform the check after validating mode/callback but before creating the job. Normalize predicate exceptions to this exact recovery guidance:

```text
resetBeforeSync is maintenance for an already-compatible Market v5 root only. Run bt market-cutover cutover for Market v4, older, malformed, or adjustment-mode-incompatible roots.
```

Map `IncompatibleMarketResetError` to HTTP 409 in the route.

- [ ] **Step 4: Add the byte-invariance route test**

Create an incompatible DuckDB file and Parquet marker under the test Market root, record `{relative_path: sha256(bytes)}`, call `POST /api/db/sync` with reset enabled, and assert HTTP 409 plus an identical mapping afterward.

```python
before = regular_file_sha256s(market_root)
response = client.post(
    "/api/db/sync",
    json={"mode": "initial", "resetBeforeSync": True},
)
assert response.status_code == 409
assert "market-cutover cutover" in response.json()["message"]
assert regular_file_sha256s(market_root) == before
```

- [ ] **Step 5: Prove compatible v5 maintenance reset remains green**

Update the existing successful reset fixture to schema 5 plus `provider_adjusted_v1`; assert reset callback executes once and the job runs.

- [ ] **Step 6: Run focused verification and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_sync_service.py \
  tests/unit/server/test_routes_db_sync.py -q
uv run --directory apps/bt ruff check \
  src/infrastructure/db/market/market_db.py \
  src/application/services/sync_service.py \
  src/entrypoints/http/routes/db.py
uv run --directory apps/bt pyright \
  src/infrastructure/db/market/market_db.py \
  src/application/services/sync_service.py \
  src/entrypoints/http/routes/db.py
git add apps/bt/src apps/bt/tests/unit/server
git commit -m "fix(bt): reject incompatible reset before sync"
```

### Task 2: Expose migration eligibility and correct recovery guidance

**Files:**
- Modify: `apps/bt/src/application/contracts/market_data_plane.py`
- Modify: `apps/bt/src/application/services/db_stats_service.py`
- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_db_stats_service.py`
- Modify: `apps/bt/tests/unit/server/services/test_db_validation_service.py`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.test.tsx`
- Regenerate: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Regenerate: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`

**Interfaces:**
- Produces: `MarketSchemaStats.resetBeforeSyncEligible: bool`
- Consumes: Task 1 eligibility predicate.

- [ ] **Step 1: Write failing backend and UI tests**

Assert validation/stats return `resetBeforeSyncEligible=false` for pre-v5/wrong-mode roots and never recommend Initial reset as migration. In the Settings test, pass an incompatible response and assert the reset switch is disabled while the cutover command is shown.

```tsx
expect(screen.getByRole("checkbox", { name: /reset before sync/i })).toBeDisabled();
expect(screen.getByText(/bt market-cutover cutover/)).toBeInTheDocument();
expect(screen.queryByText(/rebuild the legacy database with initial sync/i)).not.toBeInTheDocument();
```

- [ ] **Step 2: Run tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
cd apps/ts && bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
```

Expected: backend schema response lacks the field and the UI still advertises live reset.

- [ ] **Step 3: Implement the contract and UI distinction**

Add this exact field:

```python
class MarketSchemaStats(BaseModel):
    version: int | None = None
    requiredVersion: int = 5
    current: bool = False
    resetBeforeSyncEligible: bool = False
```

Populate it from the inspected Market DB. In Settings, enable typed confirmation only when true; otherwise render the cutover command and disable live reset.

- [ ] **Step 4: Regenerate contracts, verify, and commit**

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:sync
bun run --filter @trading25/contracts bt:check
bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
cd ../..
git add apps/bt/src/application apps/bt/tests/unit/server/services \
  apps/ts/packages/contracts apps/ts/packages/web/src/pages
git commit -m "fix(web): distinguish v5 reset from market cutover"
```

### Task 3: Persist exact provider lineage per stock window

**Files:**
- Modify: `apps/bt/src/shared/provider_stock_window.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_schema.py`
- Modify: `apps/bt/src/infrastructure/db/market/tables.py`
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/src/infrastructure/db/market/valuation_queries.py`
- Modify: `apps/bt/src/application/services/sync_strategies.py`
- Modify: `apps/bt/src/application/services/sync_publish_helpers.py`
- Modify: `apps/bt/src/application/services/sync_bulk_ingest_helpers.py`
- Modify: `apps/bt/src/application/services/stock_refresh_service.py`
- Modify: `apps/bt/src/application/services/dataset_snapshot_selection.py`
- Modify: `apps/bt/src/infrastructure/db/dataset_io/dataset_writer.py`
- Modify: `apps/bt/src/infrastructure/data_access/fundamentals_pit_reader.py`
- Modify: `apps/bt/src/application/services/ranking_fundamental_queries.py`
- Modify: `contracts/market-db-schema-v4.json`
- Modify: all test fixtures that explicitly create/insert `stock_provider_windows`.

**Interfaces:**
- Produces: `ProviderStockStage(provider_plan, provider_as_of, provider_codes)`
- Changes: `publish_stock_data(rows: list[dict[str, Any]], *, stage: ProviderStockStage) -> SemanticDeltaResult`
- Changes: `flush_staged_stock_data(*, stage: ProviderStockStage, exclude_codes=frozenset())`
- Changes: `refresh_stocks(codes: list[str], market_db: StockRefreshMarketDbLike, time_series_store: StockRefreshTimeSeriesStoreLike, jquants_client: StockRefreshClientLike, *, provider_plan: str, provider_as_of: str, progress_callback: Callable[[int, int, str], None] | None = None, cancel_check: Callable[[], bool] | None = None) -> RefreshResponse`

- [ ] **Step 1: Write failing stage and store tests**

Add validation tests for normalized non-empty code scope, ISO frontier, and non-empty plan. Add a two-symbol publication test where one symbol lacks a frontier quote.

```python
stage = ProviderStockStage(
    provider_plan="premium",
    provider_as_of="2026-02-12",
    provider_codes=frozenset({"7203", "6758"}),
)
store.publish_stock_data(
    [provider_row(code="7203", date="2026-02-12")],
    stage=stage,
)
windows = provider_windows_by_code(store)
assert windows["7203"].coverage_end == "2026-02-12"
assert windows["6758"].coverage_end == "2026-02-10"
assert {row.provider_as_of for row in windows.values()} == {"2026-02-12"}
assert {row.provider_plan for row in windows.values()} == {"premium"}
```

Add a plan-change test proving partial refresh does not relabel untouched windows.

- [ ] **Step 2: Run the store tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_provider_stock_window.py \
  tests/unit/server/db/test_time_series_store.py \
  -k 'provider_stage or suspended_symbol or plan_change' -q
```

- [ ] **Step 3: Add the stage object and physical schema column**

```python
@dataclass(frozen=True, slots=True)
class ProviderStockStage:
    provider_plan: str
    provider_as_of: str
    provider_codes: frozenset[str]

    def __post_init__(self) -> None:
        normalized = frozenset(normalize_stock_code(code) for code in self.provider_codes)
        if not self.provider_plan.strip():
            raise ValueError("provider_plan must be non-empty")
        date.fromisoformat(self.provider_as_of)
        if not normalized:
            raise ValueError("provider_codes must be non-empty")
        object.__setattr__(self, "provider_codes", normalized)
```

Add `provider_plan TEXT NOT NULL` to `stock_provider_windows` everywhere, including the JSON contract. Use explicit insert column lists in fixtures.

- [ ] **Step 4: Propagate stage authority through sync and refresh**

Use the TOPIX/request frontier, not `max(stock row dates)`. Advance same-plan in-scope windows even when no row was returned. Fetch full refresh with `to=provider_as_of` and persist the supplied plan/frontier.

```python
async def refresh_stocks(
    codes: list[str],
    market_db: StockRefreshMarketDbLike,
    time_series_store: StockRefreshTimeSeriesStoreLike,
    jquants_client: StockRefreshClientLike,
    *,
    provider_plan: str,
    provider_as_of: str,
    progress_callback: Callable[[int, int, str], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> RefreshResponse:
```

- [ ] **Step 5: Write Dataset and diagnostics tests**

Add tests that reject mixed per-window plans, accept a suspended symbol with common request frontier, and ignore stale/missing global plan metadata.

```python
manifest = build_dataset_with_windows(
    window_plans={"7203": "premium", "6758": "premium"},
    global_plan="free",
)
assert manifest["source"]["providerPlan"] == "premium"
```

For mixed plans, assert Dataset selection/copy fails before destructive overwrite. Diagnostics must return `providerPlan=None`, incoherent status, and a positive invalid-window count.

- [ ] **Step 6: Make Dataset and PIT readers consume per-window plan**

Remove global `sync_metadata.provider_plan` as lineage authority. Require exactly one distinct selected plan and one distinct selected as-of. Include `providerPlan` in canonical fingerprint rows and validate non-blank plan in PIT/ranking readers.

- [ ] **Step 7: Run both focused suites and commit the indivisible schema change**

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
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
./scripts/check-contract-sync.sh
git add apps/bt contracts/market-db-schema-v4.json
git commit -m "fix(bt): persist exact provider lineage per stock window"
```

### Task 4: Reject corporate-action evidence hidden in no-trade rows

**Files:**
- Modify: `apps/bt/src/application/services/stock_data_row_builder.py`
- Modify: `apps/bt/tests/unit/server/services/test_stock_data_row_builder.py`
- Modify: `apps/bt/tests/unit/server/services/test_stock_refresh_service.py`

- [ ] **Step 1: Write failing conversion and refresh tests**

Use an all-null OHLC/AdjOHLC row with `AdjFactor=0.5`. Assert bulk conversion and full-window refresh reject it rather than skip it. Preserve positive tests for absent/unit factors.

- [ ] **Step 2: Run tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_stock_data_row_builder.py \
  tests/unit/server/services/test_stock_refresh_service.py \
  -k 'no_trade and factor' -q
```

- [ ] **Step 3: Restrict ordinary no-trade classification**

Make `is_provider_no_trade_row()` return true only when prices are all null and factor is absent or exactly `1.0`. Invalid, non-finite, zero, negative, or non-unit factors return false so existing retry/reject paths execute.

- [ ] **Step 4: Verify and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_stock_data_row_builder.py \
  tests/unit/server/services/test_stock_refresh_service.py -q
uv run --directory apps/bt ruff check src/application/services/stock_data_row_builder.py
git add apps/bt/src/application/services/stock_data_row_builder.py \
  apps/bt/tests/unit/server/services/test_stock_data_row_builder.py \
  apps/bt/tests/unit/server/services/test_stock_refresh_service.py
git commit -m "fix(bt): reject adjustment events on no-trade rows"
```

### Task 5: Repair provider projection and reject projection drift

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/src/infrastructure/db/market/valuation_queries.py`
- Modify: `apps/bt/src/infrastructure/db/market/market_compaction.py`
- Modify: `apps/bt/tests/unit/server/db/test_time_series_store.py`
- Modify: `apps/bt/tests/unit/server/db/test_market_adjusted_metrics.py`
- Modify: `apps/bt/tests/unit/server/db/test_market_compaction.py`

- [ ] **Step 1: Write the missing/corrupt projection replay test**

Parameterize `DELETE` and corrupt `UPDATE` against `stock_data`, replay identical raw input, and assert raw bytes/`created_at` remain unchanged while the projection is restored and only `stock_data` becomes dirty.

- [ ] **Step 2: Run the replay test and verify RED**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py \
  -k repairs_missing_or_corrupt_consumer_projection -q
```

- [ ] **Step 3: Classify raw and projection deltas independently**

Read existing `stock_data` rows before the no-op gate. Compare semantic columns excluding `code`, `date`, and `created_at`. Include projection delta in the no-op decision and mutate/dirty only the physical layer whose delta is non-empty, inside the existing transaction.

- [ ] **Step 4: Write diagnostics and compaction tests**

Corrupt only `stock_data.adjustment_factor`; assert `providerAdjustedMismatchCount > 0`. Run compaction and assert it fails before staging while source inode and bytes remain unchanged.

- [ ] **Step 5: Include factor mismatch and compaction invalidation**

Add `adjustment_factor` to the provider projection FULL OUTER JOIN comparison. Add `providerAdjustedMismatchCount` to the invalid diagnostic tuple in compaction validation.

- [ ] **Step 6: Verify and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_time_series_store.py \
  tests/unit/server/db/test_market_adjusted_metrics.py \
  tests/unit/server/db/test_market_compaction.py -q
uv run --directory apps/bt ruff check \
  src/infrastructure/db/market/time_series_store.py \
  src/infrastructure/db/market/valuation_queries.py \
  src/infrastructure/db/market/market_compaction.py
uv run --directory apps/bt pyright \
  src/infrastructure/db/market/time_series_store.py \
  src/infrastructure/db/market/valuation_queries.py \
  src/infrastructure/db/market/market_compaction.py
git add apps/bt/src/infrastructure/db/market apps/bt/tests/unit/server/db
git commit -m "fix(bt): repair and validate provider stock projection"
```

### Task 6: Fail closed on Parquet partition deletion

**Files:**
- Modify: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Modify: `apps/bt/tests/unit/server/db/test_time_series_store.py`

- [ ] **Step 1: Write daily and minute deletion failure tests**

Monkeypatch `shutil.rmtree` to raise `OSError`. Assert the exception surfaces, the partition remains, dirty state remains, and `has_pending_index()` is true. Restore deletion and retry; assert both partition and dirty state clear.

- [ ] **Step 2: Run tests and verify RED**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py \
  -k 'partition_deletion_failure_keeps_dirty_state' -q
```

- [ ] **Step 3: Add one fail-closed helper**

```python
def _remove_partition_directory(path: Path) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return
```

Use it in daily and minute export paths. Do not catch other `OSError`; existing end-of-method clearing must run only after deletion success.

- [ ] **Step 4: Verify and commit**

```bash
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -q
git add apps/bt/src/infrastructure/db/market/time_series_store.py \
  apps/bt/tests/unit/server/db/test_time_series_store.py
git commit -m "fix(bt): fail closed on parquet partition deletion"
```

### Task 7: Add durable activation journal primitives

**Files:**
- Create: `apps/bt/src/application/services/market_v4_cutover/activation_journal.py`
- Create: `apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_journal.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/contracts.py`
- Modify: `apps/bt/tests/unit/server/services/test_market_v4_cutover_structure.py`

**Interfaces:**
- Produces: `ActivationState`, `MarketTreeIdentity`, `ActivationAttempt`, `ActivationJournalRecord`, `ActivationJournalRepository`.

- [ ] **Step 1: Write failing repository tests**

Test exact state order `prepared → exchange_started → activated → reported`; reject duplicate, skipped, regressed, torn, unknown, and mismatched-ID records. Inject file-fsync and directory-fsync failures and assert no later state is accepted.

- [ ] **Step 2: Run tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_journal.py -q
```

- [ ] **Step 3: Implement immutable attempt/state models**

Use `StrEnum` for the four states and frozen dataclasses for exact identities and attempt arguments. Store canonical JSON records under:

```text
operations/market-v5-cutover/activation-journals/<REPORT_ID>/
  00000001-prepared.json
  00000002-exchange_started.json
  00000003-activated.json
  00000004-reported.json
```

Create each record with `O_CREAT | O_EXCL | O_NOFOLLOW`, fsync the file, then fsync its directory. Validate sequence, state transition, report/rehearsal/backup IDs, paths, code version, config, and identities during load.

- [ ] **Step 4: Update structure ownership and commit**

Add the new module responsibilities to `EXPECTED_RESPONSIBILITIES` while retaining the guard that forbids old retained-promotion journal modules.

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_journal.py \
  tests/unit/server/services/test_market_v4_cutover_structure.py -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover \
  tests/unit/server/services/test_market_v4_cutover_activation_journal.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover
git add apps/bt/src/application/services/market_v4_cutover \
  apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_journal.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover_structure.py
git commit -m "feat(bt): add durable v5 activation journal"
```

### Task 8: Bind activation to journaled identities

**Files:**
- Modify: `apps/bt/src/application/services/market_v4_cutover/activation.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/workspace.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/backup.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/reports.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/activation_contract.py`
- Modify: activation/atomic-exchange test files under `apps/bt/tests/unit/server/services/`.

- [ ] **Step 1: Strengthen atomic-exchange tests with deterministic quarantine**

Assert activation uses exactly `operations/market-v5-cutover/quarantine/pre-cutover-<REPORT_ID>` and never generates a random ownership path.

- [ ] **Step 2: Run and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_atomic_exchange.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py -q
```

- [ ] **Step 3: Record durable states around exchange**

Change the workspace interface to:

```python
def _activate_staged_market(
    self,
    staged_market: Path,
    *,
    quarantine: Path,
) -> None:
```

After staged smoke and backup recheck, capture exact source/staged/backup/report context and append `prepared`. Append and fsync `exchange_started` immediately before exchange. Verify active/staged identities and deterministic quarantine immediately after exchange, then append `activated`. Publish and read-back-validate the exact report, then append `reported`.

- [ ] **Step 4: Make report adoption exact and idempotent**

If the success report already exists, accept it only when report/rehearsal/backup IDs, code version, config, source identity, activated identity, quarantine identity, and evidence match the journaled attempt. Never overwrite a mismatched report.

- [ ] **Step 5: Run caught-failure regression suites and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_failure_recovery.py \
  tests/unit/server/services/test_market_v4_cutover_atomic_exchange.py -q
git add apps/bt/src/application/services/market_v4_cutover \
  apps/bt/tests/unit/server/services/test_market_v4_cutover_*.py
git commit -m "refactor(bt): bind activation to journaled identities"
```

### Task 9: Recover interrupted activation by exact same ID

**Files:**
- Create: `apps/bt/src/application/services/market_v4_cutover/activation_recovery.py`
- Create: `apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/activation.py`
- Modify: `apps/bt/src/application/services/market_v4_cutover/service.py`

**Interfaces:**
- Produces: `ActivationRecoveryService.recover_if_present(report_id: str, *, rehearsal_report_id: str, backup_id: str, config: SmokeConfig, inherited_environment: dict[str, str], code_version: str) -> OperationResult | None`.

- [ ] **Step 1: Write fresh-process crash tests**

In a child process, call `os._exit(75)` at four boundaries: before exchange, after exchange, after activated state before report, and after report publication before `reported`. In the parent, construct a fresh service and rerun the exact same command IDs/arguments.

Assert latest journal state is `reported`, active identity is expected v5, immutable backup remains, source v4 is exact quarantine, and no tree has duplicate ownership.

- [ ] **Step 2: Run tests and verify RED**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py -q
```

- [ ] **Step 3: Implement the state recovery table**

At same-ID entry, before new preparation:

- `reported`: validate and return the exact report;
- `activated`: validate active/quarantine identities, run active smoke, publish/adopt report, append `reported`;
- `exchange_started`: inspect the three legal layouts (not exchanged, exchanged with source at staged path, exchanged with source at quarantine), then resume deterministically;
- `prepared`: proceed only when source/staged identities remain exact;
- any different rehearsal ID, backup ID, config, code version, target fingerprint, or ambiguous identity: restore the immutable backup when ownership is provable, otherwise fail closed without mutation.

- [ ] **Step 4: Run the full cutover suite and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_*.py \
  tests/unit/cli_bt/test_market_cutover_cli.py -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover
git add apps/bt/src/application/services/market_v4_cutover \
  apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py
git commit -m "fix(bt): recover interrupted cutover by exact same id"
```

### Task 10: Align active documentation and run final branch verification

**Files:**
- Modify: `AGENTS.md`
- Modify: `docs/architecture-sot-matrix.md`
- Modify: `docs/runbooks/market-v5-cutover.md`
- Modify: `.codex/skills/bt-financial-analysis/SKILL.md`
- Modify: `.codex/skills/ts-financial-analysis/SKILL.md`
- Modify: current-rerun clauses in `apps/bt/docs/experiments/**/README.md`
- Modify: relevant guardrail tests under `apps/bt/tests/unit/scripts/` and cutover contract tests.

- [ ] **Step 1: Write failing active-guidance guard tests**

Assert current guidance names Market v5/provider-adjusted publication, pre-v5 recovery names only the cutover command, and experiment current-rerun instructions do not require Market v4/local projection. Preserve historical benchmark evidence and superseded-contract text.

- [ ] **Step 2: Update normative guidance**

Document same-ID recovery states and require operators not to edit journal, lock, staging, backup, or quarantine paths. Replace active local-projection wording while retaining historical descriptions.

- [ ] **Step 3: Run documentation/skill guards and commit**

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_contracts.py \
  tests/unit/scripts/test_check_research_guardrails.py \
  tests/unit/scripts/test_audit_skills.py -q
python3 scripts/check-research-guardrails.py
python3 scripts/skills/refresh_skill_references.py --check
python3 scripts/skills/audit_skills.py --strict-legacy
git add AGENTS.md docs .codex apps/bt/docs apps/bt/tests/unit
git commit -m "docs: align Market v5 safety and recovery guidance"
```

- [ ] **Step 4: Integrate final main and regenerate contracts**

Fetch final `origin/main`, merge it once into the PR branch, resolve the ranking fixture conflict by retaining v5 provider-window/current-basis setup plus main's `scale_category` and `daily_technical_metrics` preservation. Then run:

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:sync
bun run --filter @trading25/contracts bt:check
cd ../..
python3 scripts/skills/refresh_skill_references.py
git diff --check
```

- [ ] **Step 5: Run full verification**

```bash
uv run --directory apps/bt pytest -q -o faulthandler_timeout=60
uv run --directory apps/bt ruff check src tests scripts/benchmark_market_v5_sync.py
uv run --directory apps/bt pyright src scripts/benchmark_market_v5_sync.py
cd apps/ts
bun run workspace:test
bun run quality:typecheck
bun run quality:lint
bun run quality:deps:audit
cd ../..
python3 scripts/skills/audit_skills.py --strict-legacy
python3 scripts/check-research-guardrails.py
```

- [ ] **Step 6: Request final broad review and GitHub verification**

Generate a whole-branch review package from final merge-base to HEAD. Resolve every Critical/Important finding and re-review. Push the branch, mark PR #493 ready, wait for all required GitHub checks, then squash merge only when the review verdict is ready and every required check is green.
