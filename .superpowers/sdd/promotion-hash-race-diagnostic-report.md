# Retained promotion identity-hash race diagnostic

Date: 2026-07-16 (Asia/Tokyo)

## Result

- Status: DONE_WITH_CONCERNS
- Code commit: `fbede2f0` (`fix(bt): expose retained identity hash races`)
- Production safety semantics: unchanged. There is no retry, relaxed comparison, or identity fallback.
- Operational data mutation: none. No cutover, sync, or server was run by this diagnostic task.

## Root-cause trace

The durable journal for `market-v4-active-20260716` records:

| Sequence | State | UTC timestamp |
|---:|---|---|
| 1 | `validated` | `2026-07-15T20:20:40.764907Z` |
| 2 | `runtimes_detached` | `2026-07-15T20:20:55.965963Z` |
| 3 | `prepared` | `2026-07-15T20:20:57.244574Z` |
| 4 | `exchanged` | `2026-07-15T20:21:09.412470Z` |
| 5 | `quarantined` | `2026-07-15T20:21:16.058808Z` |
| 6 | `exchanged_back` | `2026-07-15T20:21:50.319311Z` |
| 7 | `rolled_back` | `2026-07-15T20:22:02.489932Z` |

After `QUARANTINED`, `_promote_retained_under_leases_unchecked` can reach
`_regular_file_identity` through these groups:

1. Before the owned server is started, `_prepare_retained_runtime` hashes:
   - source `config/default.yaml` and every source `strategies/**` regular file;
   - source `config/default.yaml` again before copying it;
   - the source configuration fingerprint after copying;
   - the completed runtime snapshot's `config/default.yaml` and `strategies/**`.
2. After the owned server and all workers have joined, `active_after =
   _market_location_identity(...)` hashes canonical `market.duckdb` and all canonical
   Parquet files.
3. After `ACTIVE_SMOKE_PASSED`, report expectation/publication hashes the backup,
   active/quarantine payloads, and detached cleanup-staging artifacts.

The real operation has no
`operations/market-v4-cutover/reports/market-v4-active-20260716` directory. The
promotion code creates that directory immediately after `_prepare_retained_runtime`
returns successfully, with `exist_ok=False`. Promotion rollback does not delete that
directory. Therefore the failure occurred inside `_prepare_retained_runtime`, before
the owned server started. It did **not** occur in the post-smoke canonical Market
rehash. The rollback removes the incomplete cutover runtime, which explains why no
runtime snapshot remains after the successful rollback.

The former exception text was emitted for every source/runtime config or strategy
open/stat/stability failure as well as for Market DB/Parquet failures. It discarded
the relative path, failure stage, and before/after/current metadata, so the exact
configuration file and exact kind of race cannot be recovered from this completed
run.

## Working-path comparison

The successful retained rehearsal and active promotion use the same
`_prepare_retained_runtime`, read-only runtime capability, FastAPI smoke sequence,
joined shutdown, and post-smoke identity check. The successful r13 report records a
`167.82325s` retained smoke within `188.365413s` total. The failed active operation
entered rollback only `34.260503s` after `QUARANTINED`, and the rollback itself must
rehash the active v4 and quarantined v3 payloads before exchange-back. This timing is
consistent with the directory-presence proof that the failure preceded server start,
not with completion of the same full semantic smoke.

The meaningful location difference is that rehearsal prepares the runtime under the
retained rehearsal root, while promotion prepares it under the active data root after
atomic exchange. Eligibility had already fingerprinted both configurations before the
exchange.

## Hypothesis

Status: **not proven**.

Single hypothesis: an active-root source configuration or strategy file was replaced,
removed, or had stability metadata changed by a concurrent external config writer
during `_prepare_retained_runtime`'s source fingerprint/copy validation.

Supporting evidence:

- the journal and surviving filesystem state prove the failure was in runtime
  preparation;
- runtime preparation's only `_regular_file_identity` inputs are source/runtime
  `config/default.yaml` and `strategies/**` files;
- the operation itself creates runtime copies but does not mutate source config or
  strategy files;
- the same retained runtime path had passed r13 earlier.

The exact file, whether the failure was `open_failed`, `path_missing_after_hash`,
`path_stat_failed_after_hash`, `read_or_fstat_failed`, or `metadata_changed`, and the
external actor cannot be proven from the old error. No behavioral fix was made.

## Diagnostic change

`_regular_file_identity` now fails closed with:

- the validated canonical relative path;
- a machine-readable failure class;
- safe numeric/boolean `before`, `afterDelta`, and `currentDelta` metadata;
- `errno` when available.

The diagnostic contains device, inode, size, mtime/ctime nanoseconds, and regular-file
status only. It contains no file content or absolute managed-root path.

Changed files:

- `apps/bt/src/application/services/market_v4_cutover.py`
- `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`

## RED/GREEN evidence

RED, before the source change:

```text
test_regular_file_identity_reports_path_failure_class_and_metadata_deltas FAILED
AssertionError: assert 'path=parquet/stock_data/part.parquet' in
'Retained Market file changed during identity hashing'
1 failed, 1 warning in 0.41s
```

GREEN for the new test plus the existing DB/Parquet same-content replacement tests:

```text
collected 3 items
tests/unit/server/services/test_market_v4_cutover.py ... [100%]
3 passed, 1 warning in 1.10s
```

Focused identity and rollback verification:

```text
collected 15 items
tests/unit/server/services/test_market_v4_cutover.py ............... [100%]
15 passed, 1 warning in 0.84s
```

Required full focused suite:

```text
collected 372 items
tests/unit/server/services/test_market_v4_cutover.py ...
tests/unit/cli_bt/test_market_cutover_cli.py ..................... [100%]
372 passed, 2 warnings in 18.10s
```

Static checks:

```text
$ uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
All checks passed!

$ uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
0 errors, 0 warnings, 0 informations
```

## Concerns and next evidence

- This commit improves the next failure's evidence only; it does not identify or stop
  a non-cooperating external config writer.
- A future operational attempt should preserve the complete new exception. The
  canonical path, failure class, and deltas will distinguish replacement, in-place
  metadata/content mutation, disappearance, and open/stat failures without weakening
  the guard.
- Do not add blind retries: a retry would hide a real configuration-coherence breach.
