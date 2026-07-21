# Market v5 Hardening Task 6 Report

## Base and scope

- Base: `db3e33edd0e8324bcd78d121b380c770e6d3beeb`
- Production: `apps/bt/src/infrastructure/db/market/time_series_store.py`
- Tests: `apps/bt/tests/unit/server/db/test_time_series_store.py`
- This report is the only third task artifact.
- Preflight confirmed the exact base HEAD and an empty index, worktree, and untracked-file set.

## Requirement checklist

- [x] Daily date-partition removal ignores only `FileNotFoundError`.
- [x] Minute date-partition removal ignores only `FileNotFoundError`.
- [x] Every other `OSError` propagates to the caller uncaught.
- [x] Injected deletion failure leaves the existing partition and its exact Parquet bytes unchanged.
- [x] Injected deletion failure retains the corresponding dirty-date set and keeps `has_pending_index()` true.
- [x] Restoring deletion and retrying removes the partition, then clears dirty and pending state.
- [x] Dirty clearing remains strictly after the complete partition loop and is unreachable on deletion failure.
- [x] Existing missing-partition idempotency is preserved by the narrow `FileNotFoundError` branch.
- [x] Existing DuckDB transaction, provider raw/projection, daily/minute SoT, and export layout are unchanged.
- [x] No fallback, broad exception swallowing, compatibility path, implicit rebuild, or unrelated refactor was added.
- [x] No assertion was deleted or weakened.

## TDD evidence

No production file was edited before both true RED runs. At that point only the scoped test file was modified.

Plan-exact selector RED:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -k 'partition_deletion_failure_keeps_dirty_state' -q
collected 99; selected 2; deselected 97; 2 failed
```

- `test_daily_partition_deletion_failure_keeps_dirty_state` failed with `DID NOT RAISE OSError`.
- `test_minute_partition_deletion_failure_keeps_dirty_state` failed with `DID NOT RAISE OSError`.

The assertions were then ordered to expose the persisted-state mismatch directly, still before any production edit. The same exact selector again collected 99, selected 2, deselected 97, and failed 2:

- The daily `options_225_data/date=2026-02-10/data.parquet` partition and its bytes remained, but `_dirty_partition_dates["options_225_data"]` was prematurely cleared to `set()` instead of `{"2026-02-10"}`.
- The minute `stock_data_minute_raw/date=2026-02-10/data.parquet` partition and its bytes remained, but `_dirty_stock_minute_dates` was prematurely cleared to `set()` instead of `{"2026-02-10"}`.
- Because `ignore_errors=True` returned as though deletion had succeeded, both methods reached end-of-method dirty clearing and lost pending retry state.

Minimal production change: one `_remove_partition_directory(Path)` helper calls `shutil.rmtree` and catches only `FileNotFoundError`; both daily and minute zero-row cleanup branches use it.

Focused GREEN:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -k 'partition_deletion_failure_keeps_dirty_state' -q
collected 99; selected 2; deselected 97; 2 passed
```

Each regression proves the non-`FileNotFoundError` exception surfaces with the original partition bytes and dirty/pending state intact. In the same test, restoring real deletion and retrying removes the partition and only then clears dirty/pending state.

## Final verification

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -k 'partition_deletion_failure_keeps_dirty_state' -q
2 passed, 97 deselected, 1 warning

uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -q
99 passed, 1 warning

uv run --directory apps/bt ruff check src/infrastructure/db/market/time_series_store.py tests/unit/server/db/test_time_series_store.py
All checks passed!

git diff --check
exit 0
```

Pyright was not run because this task changes no production type, signature, protocol, or public interface; the binding brief requires it only proportionately when typing changes. No repository-wide suite was run.

## Invariants, absent behavior, and residual risk

The helper changes only filesystem error classification. `FileNotFoundError` remains the already-absent success case. All other `OSError` subclasses leave control before the existing dirty-date/table clears, so the public index method remains observably pending and retryable. Successful deletion or an already-absent partition continues through the existing loop and clears state at the existing end-of-method location.

No table schema, query, transaction boundary, atomic Parquet copy path, provider lineage, raw/projection mutation, daily/minute partition layout, logging contract, fallback, compatibility path, or rebuild behavior changed.

Residual risk is limited to filesystem failures that partially mutate a directory before `shutil.rmtree` raises; Task 6 requires propagation and retained retry state, which this change provides, but does not introduce a new transactional filesystem primitive. The injected regression fails before mutation and proves exact bytes remain for the reviewed swallowed-error case.

Intended commit subject: `fix(bt): fail closed on parquet partition deletion`
