# Retained promotion post-smoke rollback diagnostic

Date: 2026-07-16 (Asia/Tokyo)

## Result

- Status: DONE
- Code/test commit: `c96ecf35` (`fix(bt): isolate promotion DuckDB temp state`)
- Operational mutation by this task: none. No recovery, cutover, sync, or server was run.

## Read-only operational evidence

Operation `market-v4-active-20260716-r2` durably recorded:

```text
VALIDATED
RUNTIMES_DETACHED
PREPARED
EXCHANGED
QUARANTINED
ACTIVE_SMOKE_PASSED
CLEANUP_STAGED
EXCHANGED_BACK (rollback_mode=atomic_exchange)
```

There is no `REPORT_PERSISTED`, `COMMITTED`, or `ROLLED_BACK` record. The report
directory contains `active-smoke.log` but no `report.json`.

The current exact layout is already exchanged back:

- active v3 Market directory inode `1384368`, DB inode `2191306`;
- retained r10 v4 Market directory inode `7005881`, DB inode `7006201`;
- quarantine absent;
- cleanup staging directory inode `8345992` remains.

The journal binds four detached artifacts in cleanup staging:

| Artifact | Kind | Device | Inode | Files |
|---|---|---:|---:|---:|
| `.cutover-runtime-market-v4-rehearsal-20260715-r10` | directory | 16777234 | 7005889 | 1306 |
| `.cutover-runtime-market-v4-retained-20260715-r12` | directory | 16777234 | 7210089 | 15 |
| `.cutover-runtime-market-v4-retained-20260715-r13` | directory | 16777234 | 7450168 | 16 |
| `duckdb-tmp` | directory | 16777234 | 7006202 | 0 |

The retained r10 canonical Market additionally contains a different, empty real
`duckdb-tmp` directory at inode `8346328`. The provenance-bound original remains
unchanged in cleanup staging at inode `7006202`.

## Proven original failure

`connect_market_duckdb` previously resolved its default temporary directory as
`db_path.parent / "duckdb-tmp"` and always created it. The owned smoke process:

1. receives `MARKET_DB_PATH=market.duckdb`;
2. changes cwd to the active Market directory descriptor;
3. opens read-only Market connections;
4. consequently recreates `active Market/duckdb-tmp` after the original artifact was
   detached.

After `CLEANUP_STAGED`, promotion immediately calls
`_validate_canonical_market_payload(active_market_fd)`. The new top-level directory
makes the payload non-canonical, so promotion fails before constructing or publishing
`report.json`. This exactly explains the log-only report directory and missing
`REPORT_PERSISTED` state.

## Proven rollback failure

Atomic exchange-back succeeded and was durably journaled. Restoration then sees:

- the exact provenance-bound original `duckdb-tmp` in cleanup staging; and
- a second `duckdb-tmp` in the restored retained Market.

`_restore_held_promotion_artifacts` requires each detached artifact to exist in exactly
one location. The duplicate violates that invariant before any staged artifact is
moved, producing `Promotion artifact set is incomplete or ambiguous during
restoration`. This explains the surviving complete cleanup staging and terminal
`EXCHANGED_BACK` state.

## Fix

### Future promotions

Owned runtime environment now sets:

```text
TRADING25_DUCKDB_TEMP_DIR=.cutover-runtime-<operation-id>/duckdb-tmp
```

`connect_market_duckdb` honors this explicit override when no direct
`temp_directory` argument is supplied. All owned-smoke DuckDB ephemeral state is
therefore inside the already isolated runtime tree, which is removed before canonical
payload validation. Normal connections retain their existing DB-parent default.

### Exact r2 same-ID recovery

Recovery permits one narrowly proven reconciliation only when all conditions hold:

- journal order proves `ACTIVE_SMOKE_PASSED`, then `CLEANUP_STAGED`, then terminal
  `EXCHANGED_BACK`;
- rollback mode is exactly `atomic_exchange`;
- journal detached evidence contains `duckdb-tmp` as a directory;
- cleanup staging still contains that exact provenance-bound artifact;
- the duplicate in the exact restored retained parent is a real directory and empty;
- there are no other unexpected or duplicated artifacts.

Only then recovery removes the empty duplicate with a retained parent descriptor,
fsyncs the parent, and restores the original inode from cleanup staging. Non-empty,
symlink, special-file, unproven-state, wrong-mode, or additional duplicate layouts
remain fail-closed. Staging is not relaxed or discarded.

The recovery regression also injects a crash immediately after durable empty-collision
removal. A second same-ID recovery resumes without another exchange or backup and
restores the original artifact exactly, proving idempotence across that boundary.

## RED/GREEN evidence

Initial RED run:

```text
collected 3 items
test_promotion_routes_owned_duckdb_temp_into_isolated_runtime FAILED
  Retained Market payload is not canonical
  Retained promotion failed and rollback recovery failed
test_promotion_recovery_reconciles_empty_owned_temp_duplicate_after_exchange_back FAILED
  Promotion artifact set is incomplete or ambiguous during restoration
test_duckdb_connection_honors_isolated_temp_directory_environment FAILED
  expected isolated runtime temp path, got market-timeseries/duckdb-tmp
3 failed, 1 warning in 0.89s
```

Focused GREEN:

```text
collected 3 items
tests/unit/server/services/test_market_v4_cutover.py .. [66%]
tests/unit/server/db/test_time_series_store.py . [100%]
3 passed, 1 warning in 0.28s
```

Crash-boundary recovery GREEN:

```text
collected 1 item
tests/unit/server/services/test_market_v4_cutover.py . [100%]
1 passed, 1 warning in 0.41s
```

## Gates

```text
$ uv run --directory apps/bt pytest \
    tests/unit/server/services/test_market_v4_cutover.py \
    tests/unit/cli_bt/test_market_cutover_cli.py \
    tests/unit/server/db/test_time_series_store.py -q
403 passed, 2 warnings in 22.18s

$ uv run --directory apps/bt ruff check \
    src/application/services/market_v4_cutover.py \
    src/infrastructure/db/market/duckdb_connection.py \
    tests/unit/server/services/test_market_v4_cutover.py \
    tests/unit/server/db/test_time_series_store.py
All checks passed!

$ uv run --directory apps/bt pyright \
    src/application/services/market_v4_cutover.py \
    src/infrastructure/db/market/duckdb_connection.py
0 errors, 0 warnings, 0 informations

$ python3 scripts/skills/refresh_skill_references.py --check
exit 0
```

## Operational next step

The repository fix makes exact same-ID recovery of the current r2 layout safe and
idempotent, but this task did not execute it. The only authorized next mutation remains
the same-ID recovery path for `market-v4-active-20260716-r2`.
