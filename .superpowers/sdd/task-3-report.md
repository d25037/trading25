# Task 3 report: reduce backend sync to RESET initial and incremental

## Commit

- `6c801d59c4765ed37662275e00dc89f23d1d268d` — `refactor(market): simplify sync modes`

## Implementation

- Reduced the public and application sync contract to `initial` and `incremental`; the default is now `incremental`.
- Enforced the request matrix: `initial` requires `resetBeforeSync=true`, while `incremental` rejects it. Removed AUTO resolution and the repair strategy, and made unknown strategy names fail closed.
- Changed RESET initial orchestration to enter the guarded writer reset factory with no old `MarketDb` or time-series-store handles. The newly opened handles are prepared only after reset succeeds.
- Preserved writer lease/path guards, ownership, common finalization, read-only reopen, and the original reset exception when no writer session was created.
- Renamed the stale `reset_and_open_v4` factory API to `reset_and_open` and updated its repository callers.
- Added a pre-write Market v5 compatibility fence: writable incremental open rejects legacy `BIGINT stock_data_raw.adjusted_volume` or `BIGINT stock_data.volume` before `ensure_schema`, with RESET initial guidance. No migration, alias, or compatibility read was added.
- Removed `resetBeforeSyncEligible` from backend stats/validation contracts. Recovery guidance now points to RESET initial for incompatible/uninitialized roots and incremental for missing dates, fundamentals, options, and provider-vintage recovery.

## TDD evidence

### RED

The request, service, strategy, reset-handle, finalizer, and BIGINT regression tests were added before production changes.

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_sync_service.py \
  tests/unit/server/services/test_sync_strategies.py \
  tests/unit/server/test_routes_db_sync.py -q
```

Result: **9 failed, 257 passed**. The failures showed the old service requiring existing handles for initial reset, AUTO/REPAIR and unknown fallback remaining accepted, the old default being AUTO, missing reset validation, and reset routes opening old handles.

The critical schema regression was also run alone:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/test_routes_db_sync.py::TestDbSyncRoutes::test_incremental_rejects_legacy_bigint_adjusted_volume_before_schema_write -q
```

Result: **1 failed** because the legacy BIGINT Market v5 root returned HTTP 202 and started incremental sync instead of failing before schema writes.

### GREEN

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_sync_service.py \
  tests/unit/server/services/test_sync_strategies.py \
  tests/unit/server/test_routes_db_sync.py -q
```

Result: **240 passed, 1 warning**.

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
```

Result: **54 passed, 1 warning**.

Additional writer-resource and MarketDb regression coverage:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/db/test_market_writer_resources.py \
  tests/unit/server/db/test_market_db.py \
  tests/unit/cli_bt/test_market_cli.py \
  tests/unit/server/db/test_market_compaction.py \
  tests/unit/server/db/test_market_growth_acceptance.py -q
```

Result: **121 passed, 1 warning**.

## Static verification

- Ruff over all modified Python source, tests, and benchmark callers: `All checks passed!`.
- Pyright over all modified backend modules: `0 errors, 0 warnings, 0 informations`.
- `rg` found no remaining `reset_and_open_v4`, `resetBeforeSyncEligible`, `is_reset_before_sync_eligible`, `SyncMode.AUTO`, `SyncMode.REPAIR`, or `RepairSyncStrategy` references under `apps/bt`.
- `git diff --check` passed.

## Concerns

None. OpenAPI/TypeScript regeneration and UI removal remain outside Task 3 scope.
