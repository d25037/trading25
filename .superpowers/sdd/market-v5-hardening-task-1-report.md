# Market v5 Hardening Task 1 Report

## Outcome

Implemented the Task 1 migration boundary for `initial + resetBeforeSync`:

- `MarketDb.is_reset_before_sync_eligible()` performs read-only schema, adjustment-mode, validation, and legacy-snapshot checks.
- `SyncService.start_sync()` rejects incompatible or unreadable roots before `sync_job_manager.create_job()` and before the reset callback can run.
- The route maps `IncompatibleMarketResetError` to HTTP 409 with this exact guidance:

  `resetBeforeSync is maintenance for an already-compatible Market v5 root only. Run bt market-cutover cutover for Market v4, older, malformed, or adjustment-mode-incompatible roots.`

- A real Market v4 DuckDB plus Parquet marker route fixture proves every regular file has the same SHA-256 before and after rejection.
- The existing successful maintenance-reset test now explicitly starts from schema v5 with `provider_adjusted_v1` and still completes with one reset callback.

No migration, compatibility alias, dual read, reset fallback, or mutation of rejected roots was added.

## RED evidence

Baseline before Task 1 changes:

```text
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py tests/unit/server/test_routes_db_sync.py -q
92 passed, 1 warning in 1.36s
```

Plan-prescribed service RED:

```text
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py -k incompatible_reset -q
8 failed, 43 deselected, 1 warning in 0.24s
```

All eight cases failed with `Failed: DID NOT RAISE RuntimeError`, demonstrating that the old path reached job creation instead of rejecting v4, v3, missing-schema, missing-mode, wrong-mode, invalid-validation, legacy-snapshot, and predicate-error inputs.

Additional route RED:

```text
uv run --directory apps/bt pytest tests/unit/server/test_routes_db_sync.py -k incompatible_reset -q
1 failed, 49 deselected, 1 warning in 0.25s
```

The response was HTTP 409 only because the test manager refused job creation; its message was `Another sync job is already running`, not the required `market-cutover cutover` recovery guidance. The file digest mapping was already unchanged in this controlled RED fixture.

## GREEN evidence

Targeted regression checks after the minimal implementation:

```text
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py -k incompatible_reset -q
8 passed, 43 deselected, 1 warning in 0.03s

uv run --directory apps/bt pytest tests/unit/server/test_routes_db_sync.py -k incompatible_reset -q
1 passed, 49 deselected, 1 warning in 0.08s
```

Plan-prescribed focused verification:

```text
uv run --directory apps/bt pytest tests/unit/server/services/test_sync_service.py tests/unit/server/test_routes_db_sync.py -q
101 passed, 1 warning in 1.37s

uv run --directory apps/bt ruff check src/infrastructure/db/market/market_db.py src/application/services/sync_service.py src/entrypoints/http/routes/db.py
All checks passed!

uv run --directory apps/bt pyright src/infrastructure/db/market/market_db.py src/application/services/sync_service.py src/entrypoints/http/routes/db.py
0 errors, 0 warnings, 0 informations
```

## Changed files

- `apps/bt/src/infrastructure/db/market/market_db.py`
- `apps/bt/src/application/services/sync_service.py`
- `apps/bt/src/entrypoints/http/routes/db.py`
- `apps/bt/tests/unit/server/services/test_sync_service.py`
- `apps/bt/tests/unit/server/test_routes_db_sync.py`
- `.superpowers/sdd/market-v5-hardening-task-1-report.md`

## Invariants

- Eligibility inspection is query-only and does not call `ensure_schema()` or a metadata writer.
- Rejection precedes job creation and reset callback invocation.
- Rejected DuckDB and Parquet regular-file bytes remain unchanged.
- Only schema v5 + `provider_adjusted_v1` + valid schema + non-legacy roots are eligible.
- Predicate exceptions fail closed with the same cutover recovery guidance.
- Compatible Market v5 maintenance reset still resets exactly once and runs the initial job.
- Pre-v5 upgrade remains isolated full rebuild via `bt market-cutover cutover` only.

## Residual risks

- Verification is intentionally limited to the Task 1 focused pytest, Ruff, and Pyright commands prescribed by the plan; the repository-wide suite was not required or run.
- The HTTP byte-invariance fixture covers a real Market v4 root. Other incompatible shapes are covered at the service boundary with parameterized cases, including predicate failure normalization.

## Commit identity

- Branch: `codex/market-v5-cutover`
- Subject: `fix(bt): reject incompatible reset before sync`
- Final full SHA: reported in the Task 1 handoff because a commit cannot embed its own content-derived SHA.
