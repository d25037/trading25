# Retained promotion effective-config failure diagnostic

Date: 2026-07-16 (Asia/Tokyo)

## Result

- Status: DONE
- Diagnostic commit: `fbede2f0` (`fix(bt): expose retained identity hash races`)
- Root-cause fix commit: `6b1539c7` (`fix(bt): snapshot effective active config for promotion`)
- Identity safety semantics remain strict. There is no retry or relaxed comparison.
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

The diagnostic instrumentation then reproduced the active-root failure as
`path=config/default.yaml; failure=open_failed; errno=2`. The active XDG data root has
no `config/` directory. That is a valid state: the canonical effective configuration
uses the XDG override when present and otherwise uses the repository baseline at
`apps/bt/config/default.yaml`.

## Working-path comparison

The successful retained rehearsal and active promotion use the same
`_prepare_retained_runtime`, read-only runtime capability, FastAPI smoke sequence,
joined shutdown, and post-smoke identity check. The successful r13 report records a
`167.82325s` retained smoke within `188.365413s` total. The failed active operation
entered rollback only `34.260503s` after `QUARANTINED`, and the rollback itself must
rehash the active v4 and quarantined v3 payloads before exchange-back. This timing is
consistent with the directory-presence proof that the failure preceded server start,
not with completion of the same full semantic smoke.

The meaningful location difference is that rehearsal prepares the runtime under a
self-contained retained rehearsal root, while promotion prepares it under the active
XDG data root after atomic exchange. `configuration_fingerprint(self.data_root)`
already implemented the canonical XDG-override-or-repository-baseline rule, but
`_prepare_retained_runtime` bypassed it by calling
`_configuration_fingerprint_at(active_root_fd)` and then directly reading
`config/default.yaml` from that descriptor.

## Proven root cause

Status: **proven and fixed**.

The active promotion runtime preparation incorrectly required a physical
`<XDG data root>/config/default.yaml`, contradicting both the repository contract and
the canonical `configuration_fingerprint(self.data_root)` resolver. The absence was
misreported as a retained Market identity-hash race. There was no concurrent writer.

The fix is deliberately scoped:

- active-root runtime preparation snapshots an existing managed XDG override, or the
  safe regular repository baseline when the override is absent;
- it never creates an override at `<XDG data root>/config/default.yaml`; only the
  isolated runtime receives the copied file;
- source-before, source-after, and runtime fingerprints must remain semantically
  identical, including active strategies;
- repository fallback copy uses the existing no-follow regular-file copy path and
  revalidates the clean code identity before and after the copy;
- `_configuration_fingerprint_at(retained_root_fd)` remains strict, so retained
  rehearsal roots must still be self-contained and provenance-locked.

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

Root-cause RED:

```text
test_prepare_retained_runtime_uses_repository_config_when_active_override_missing FAILED
CutoverSafetyError: Retained Market file changed during identity hashing:
path=config/default.yaml; failure=open_failed;
metadata={"afterDelta":null,"before":null,"currentDelta":null,"errno":2}
1 failed, 1 warning in 0.64s
```

Root-cause GREEN:

```text
collected 1 item
tests/unit/server/services/test_market_v4_cutover.py . [100%]
1 passed, 1 warning in 0.10s
```

Post-fix full focused suite:

```text
collected 373 items
tests/unit/server/services/test_market_v4_cutover.py ...
tests/unit/cli_bt/test_market_cutover_cli.py ..................... [100%]
373 passed, 2 warnings in 18.56s
```

Static checks:

```text
$ uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
All checks passed!

$ uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
0 errors, 0 warnings, 0 informations

$ python3 scripts/skills/refresh_skill_references.py --check
exit 0
```

## Operational note

- No operational cutover, sync, or server was run while implementing or verifying the
  fix.
- The prior attempt is durably rolled back. A future promotion uses a fresh report ID;
  no blind retry or same-ID mutation was added.
