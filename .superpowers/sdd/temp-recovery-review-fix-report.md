# Retained promotion temp/recovery review fix

Date: 2026-07-16 (Asia/Tokyo)

## Result

- Status: DONE
- Code/test commit: `54e6d56d` (`fix(bt): constrain promotion temp recovery authority`)
- Operational mutation: none. No recovery, cutover, sync, server, or XDG data mutation was run.

## Finding 1: DuckDB temp override confinement

`connect_market_duckdb` now treats `TRADING25_DUCKDB_TEMP_DIR` as an owned-runtime
capability rather than a global configuration override:

- the ambient override is considered only when
  `TRADING25_RUNTIME_CAPABILITY=retained_market_smoke`;
- its only accepted form is
  `.cutover-runtime-<validated-operation-id>/duckdb-tmp`;
- the accepted relative path is resolved beneath the Market DB parent;
- empty, absolute, dot, dot-dot, repeated-separator, escaping, and extra-component
  forms are rejected before DuckDB is opened;
- directory creation walks from a retained descriptor and uses no-follow directory
  opens, rejecting symlink and special-file components;
- an explicit `temp_directory=` argument remains authoritative and ignores the
  ambient override;
- normal production connections ignore missing/wrong-capability ambient values and
  retain the DB-parent `duckdb-tmp` default.

The retained promotion environment already supplies both matching values, and the
promotion regression now asserts them together.

## Finding 2: recovery deletion authority

The empty duplicate `duckdb-tmp` reconciliation no longer accepts a bare deletion
boolean. The exact journal records are passed into restoration and the deletion branch
re-proves all of the following at the point of use:

- the terminal three records are consecutive
  `ACTIVE_SMOKE_PASSED -> CLEANUP_STAGED -> EXCHANGED_BACK`;
- their sequences are consecutive and their operation IDs match the exact holding
  root;
- rollback mode is exactly `atomic_exchange`;
- all three records carry the exact detached-artifact evidence from the preparation;
- exactly one provenance-bound `duckdb-tmp` artifact exists and is a directory;
- staging identity, retained contents, duplicate set, and unexpected-artifact set pass
  the existing descriptor-derived preflight;
- the collision leaf is an empty real directory opened without following symlinks.

The deletion remains limited to `rmdir("duckdb-tmp", dir_fd=retained_fd)`. Parent
fsync failure fences both leases, leaves the provenance-bound original in staging, and
does not append `ROLLED_BACK`. The fenced process cannot hand ownership to another
attempt; a later same-ID attempt is possible only after ownership is released and the
filesystem is re-inspected.

## RED evidence

Temp confinement RED:

```text
17 selected
17 failed

- canonical relative override was configured relative to ambient cwd instead of the
  DB parent;
- missing/wrong capability still honored hostile ambient values;
- invalid paths did not raise;
- symlink/special-file paths were followed or accepted.
```

Recovery authority RED:

```text
10 selected
10 failed

TypeError: MarketV4CutoverService._owned_temp_collision_recovery_proven()
takes 1 positional argument but 2 were given
```

This demonstrated that recovery proof was not bound to the exact preparation
evidence required by the new tests.

## GREEN behavior matrix

The tests cover:

- valid owned canonical path;
- missing/wrong capability with hostile absolute and escaping values;
- empty, `.`, `..`, absolute, repeated-separator, missing/extra-component paths;
- symlink ancestor, symlink leaf, and FIFO leaf;
- explicit `temp_directory=` precedence;
- nonempty duplicate, symlink duplicate, and special-file duplicate;
- wrong order, missing required state, and wrong rollback mode;
- missing and wrong provenance-bound staged evidence;
- an additional duplicated artifact and an unexpected artifact;
- parent fsync failure after empty `rmdir`.

Every pre-deletion rejection snapshots the complete active/retained/staging/journal
tree, including path, mode, device, inode, link count, size, mtime, regular-file hash,
and symlink target, and proves the snapshot is unchanged after rejection.

## Verification gates

```text
$ uv run --directory apps/bt pytest \
    tests/unit/server/services/test_market_v4_cutover.py \
    tests/unit/cli_bt/test_market_cutover_cli.py \
    tests/unit/server/db/test_time_series_store.py -q
430 passed, 2 warnings in 23.23s

$ uv run --directory apps/bt ruff check \
    src \
    tests/unit/server/services/test_market_v4_cutover.py \
    tests/unit/cli_bt/test_market_cutover_cli.py \
    tests/unit/server/db/test_time_series_store.py
All checks passed!

$ uv run --directory apps/bt pyright src
0 errors, 0 warnings, 0 informations

$ python3 scripts/skills/refresh_skill_references.py --check
exit 0

$ git diff --check
exit 0
```

## Self-review

- No compatibility alias or fallback was added.
- Ambient authority is narrower than the explicit API and cannot redirect normal
  production temp state.
- Invalid owned paths are rejected before connection creation.
- Recovery deletion authority is re-derived from exact journal and preparation
  evidence inside the restoration boundary.
- All pre-deletion validation completes before `rmdir`.
- The only intentional partial mutation is the proven empty `rmdir`; its durability
  failure is fail-stop and lease-fenced.
- `.codex/config.toml` was not read, staged, or modified.
