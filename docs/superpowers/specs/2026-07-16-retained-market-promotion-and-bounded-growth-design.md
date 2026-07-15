# Retained Market Promotion and Bounded DuckDB Growth Design

## Status

Approved direction: promote the already validated retained Market v4 tree into
the active XDG data plane without another sync, then remove the write patterns
that made the pre-refactor Market DuckDB grow on every sync. The detailed
transaction and maintenance contract below is pending final review before
implementation.

## Problem

The active Market data plane is still schema v3, while
`market-v4-rehearsal-20260715-r10` contains the complete schema-v4 data plane.
The current `cutover` implementation accepts the current-code retained smoke
report, but still creates an empty staging tree and calls `/api/db/sync` in
`_run_rebuild`. That repeats more than thirty minutes of external fetch and
materialization even though r13 proved the retained r10 Market tree unchanged
under the current code.

The old active database also demonstrated a separate long-term maintenance
problem: its file grew after repeated syncs because released DuckDB blocks were
not returned to the filesystem. Three current write paths can recreate this
problem after promotion:

1. advancing `materialized_through_date` classifies an otherwise unchanged
   adjustment basis as a catalog change and replaces its complete valuation
   history;
2. daily technical metrics are rebuilt with a full table `DELETE` followed by
   a full `INSERT` even when almost every row is unchanged;
3. compaction runs only after sync and only when both a 512 MiB threshold and a
   10% free-space ratio are met, so a large database can accumulate an
   unbounded absolute amount of free blocks below 10%.

The retained r10 tree and the active tree are on the same filesystem device.
The retained Market payload is approximately 6.5 GiB of DuckDB plus 835 MiB of
Parquet. About 3.6 GiB of additional directories are operation-owned smoke
runtimes and are not part of the Market payload.

## Goals

- Promote the exact r10 Market payload validated by r13 without calling sync,
  materialization, stock refresh, intraday ingest, or J-Quants.
- Keep an immutable, verified backup of the active v3 payload.
- Make the active-path transition atomic: a process must observe either the old
  v3 directory or the retained v4 directory, never a missing active path.
- Hold active and retained exclusive leases through validation, exchange,
  smoke, report persistence, and finalization.
- Make the promotion one-shot, crash-recoverable, and fail closed on identity,
  code, filesystem, process-lifetime, or report drift.
- Avoid full-history rewrites when only a frontier advances and avoid writes
  for semantically unchanged rows.
- Put an absolute upper bound on reclaimable DuckDB free space, independent of
  database size, while retaining a lower soft threshold for small databases.
- Apply the same maintenance finalizer to every high-churn Market writer, not
  only `/api/db/sync`.
- Prevent another process from retaining a writable handle to an inode that is
  being compacted or replaced.

## Non-goals

- No new initial or incremental sync during promotion.
- No schema-v3 migration, compatibility reader, dual data plane, report alias,
  or copy fallback for cross-device promotion.
- No deletion of the immutable pre-v4 backup.
- No compaction while a DuckDB connection, server, worker, or writer may still
  own the active file.
- No byte-for-byte stable DuckDB filename size after every transaction.
  DuckDB may reuse internal free blocks; the contract is bounded growth and
  deterministic reclamation, not zero free blocks at all times.

## Alternatives

### A. Atomic retained-tree exchange plus bounded-write maintenance (selected)

Use macOS `renameatx_np(..., RENAME_SWAP)` through a small tested filesystem
adapter to exchange active v3 and retained v4 in one atomic operation. Preserve
the payload inode identities, run a no-mutation active smoke, and exchange back
on failure. Separately make materialization and technical metrics differential,
then centralize threshold and hard-cap compaction.

### B. Two no-replace renames

Rename active v3 to quarantine and retained v4 to active. Each rename is
atomic, but a crash between them leaves the active pathname absent. A journal
can repair this after restart but cannot remove the outage window. Rejected.

### C. APFS clone or ordinary copy

Keeps the retained source path but changes payload identity, consumes extra
space, and introduces platform-dependent clone fallback behavior. Rejected.

### D. Existing staged rebuild

Preserves the current implementation but calls `/api/db/sync`, depends on
J-Quants, and repeats the cost the retained evidence was created to avoid.
Rejected for this promotion.

## Command Contract

Add a dedicated command rather than changing the meaning of the existing full
rebuild command:

```text
bt market-cutover promote-retained REPORT_ID \
  --retained-report-id market-v4-retained-20260715-r13 \
  --backup-id market-v3-pre-v4-20260716 \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

The command accepts no source path, force flag, copy fallback, or J-Quants
credential option. The retained root is derived from the retained report's
source rehearsal ID under the managed operations tree. An existing operation
ID, consumed source, cross-device tree, or non-macOS atomic-exchange
environment fails before mutation.

`promote-retained` itself creates the backup named by `--backup-id`; it does not
accept a separately prepared backup. Under the already-held active exclusive
lease it captures active identity, creates the backup with create-only paths,
fsyncs its payload and manifest, verifies it, rechecks active identity, and only
then prepares the exchange journal. An existing backup ID is rejected before
copying, and a failed backup never makes the operation exchange-eligible.

The existing `cutover` remains the explicit full-rebuild recovery workflow. It
must not silently select retained promotion or claim no-sync evidence.

## Promotion Eligibility

Before any Market-path mutation the service must prove:

1. The repository has a clean immutable current code identity. The r13 report
   remains bound to its own code version; the promotion records the newer
   implementation code separately and bridges it with pre-inspection and the
   post-activation current-code smoke.
2. r13 is an exact passing `retained_market_smoke` report with joined server
   and workers, complete semantic checks, exact smoke configuration, the r10
   provenance chain, and identical before/after Market payload identities.
3. The r10 root is confined to the managed rehearsal directory and is not a
   symlink or replacement. Its exclusive lease is acquired and retained.
4. Re-hashing `market.duckdb` and all 7,327 Parquet files matches the r13
   identity exactly. DuckDB inspection reports schema v4,
   `local_projection_v2_event_time`, and exact ready lineage.
5. The active exclusive lease is held, no writer or owned worker is running,
   WAL is checkpointed/empty, and active v3 identity is captured.
6. The immutable backup is newly created, its manifest is fsynced and verified,
   and its file set, sizes, and hashes exactly match the captured active v3
   tree immediately before exchange.
7. Active, retained, journal, and quarantine parents have the required stable
   descriptor identities; active and retained directories have the same
   `st_dev`. `EXDEV` and unavailable atomic exchange fail closed.

Locks are always acquired in the fixed order active then retained, and both are
held from validation until commit or proven rollback. The promotion does not
call the current full-rebuild `_preflight_under_lease` or
`_activate_staged_market` helpers: their capacity and two-rename assumptions do
not satisfy this transaction.

## Transaction and Crash Recovery

Promotion uses a create-only, descriptor-confined, fsynced journal. Each state
includes the exact active, retained, backup, and quarantine identities rather
than trusting path existence alone.

POSIX directory fsync failure after a publication rename is inherently
indeterminate: current path visibility cannot prove whether the rename will
survive a crash. The journal therefore has explicit `committed`,
`not_committed`, and `indeterminate` append outcomes backed by a separate
append-only intent/resolution control ledger. It never converts an indeterminate
append into success or best-effort cleanup. Ordinary journal reading rejects an
unresolved intent; the operation retains both leases and stops until dedicated
same-ID recovery adopts or rejects the exact candidate from durable evidence.

```text
validated
  -> runtimes_detached
  -> prepared
  -> exchanged
  -> quarantined
  -> active_smoke_passed
  -> report_persisted
  -> committed

failure before exchanged -> active_untouched
failure after exchanged  -> exchanged_back -> rolled_back
unjoined owned process    -> rollback_deferred_with_lease_held
```

Execution is:

1. Under both exclusive leases, detach only operation-owned
   `.cutover-runtime-*` directories and empty `duckdb-tmp`/WAL artifacts from
   the retained Market directory into an operation-specific holding directory
   using no-replace renames. Each runtime name must be proven by the exact r10,
   r12, or r13 report provenance; a prefix match alone is insufficient. Reject
   any other unexpected top-level entry and require the final canonical
   allowlist `market.duckdb` plus `parquet`. Re-hash the DB and Parquet identity
   after detachment.
2. Persist `prepared` and fsync the journal file and parent.
3. Atomically exchange the descriptor-relative active and retained Market
   directories with `RENAME_SWAP`, then fsync both parents. The active path now
   names the exact retained v4 inode; the retained path names v3 and is the
   immediate rollback tree.
4. Validate all three location identities from descriptors and persist
   `exchanged`.
5. Move the v3 tree from the retained location to an operation-specific
   quarantine below the active data root using no-replace rename, fsync both
   parents, verify its identity, and persist `quarantined`. Both active v4 and
   rollback v3 are now protected by the active lease inherited by owned
   children.
6. Start the owned server with a new isolated runtime and a
   `retained_market_smoke` capability. Remove J-Quants credentials from its
   environment and prohibit sync/materialization/refresh/intraday routes.
   Run the complete semantic smoke, stop and join all owned work, remove the
   new runtime, and prove the active DB/Parquet identity is still exact.
7. Delete the detached, provenance-proven operation runtimes, verify the final
   canonical active allowlist, and persist a passing report through the
   existing atomic report writer. Fsync the report and its parent.
8. Mark the source consumed and persist `committed`. The immutable backup and
   quarantined v3 tree remain available to the operator.

If any post-exchange check fails and all processes are joined, atomically
exchange active v4 with v3 at its current retained or quarantine location,
verify the original active v3 identity, move the displaced v4 tree back to its
retained source location, restore detached runtime placement only when needed
for evidence, and record `rolled_back`. The verified immutable backup is used
only if atomic exchange rollback cannot be proven. If an owned process is not
joined, its inherited active lease remains locked and rollback is deferred.

On restart, the same operation ID reads the journal and the three exact
location identities. A committed report is authoritative only when its final
validator passes. Without that report, any `exchanged` state is conservatively
rolled back; ambiguous identity combinations require operator recovery and
never start sync or overwrite a tree.

Journal recovery itself is serialized cross-process. It may adopt a candidate
only when a durable intent binds its operation/attempt ID, target sequence,
canonical payload SHA, previous-record SHA, expected state, and identities, and
after candidate plus parent fsync succeeds. A durable accepted resolution then
authorizes ordinary reading. Missing candidates are `not_committed` only after
the containing directory fsync succeeds. Torn, mismatched, extra, symlinked, or
unresolved candidates remain fail-stop; numbered paths are never reused.

## Promotion Report

The immutable report adds:

- `activationMode: retained_atomic_exchange`;
- current promotion code identity, retained r13 ID/code identity/report SHA,
  r10 source report ID/code identity/report SHA, and root/configuration
  fingerprints;
- active-before, backup, retained-source, activated, and active-after payload
  identities;
- same-device and atomic-exchange evidence;
- journal ID/final state and quarantine path;
- detached/removed runtime names;
- `noSync: true`, `noJQuants: true`, and the exact API checks;
- server/worker join verdicts and semantic smoke coverage;
- source-consumed marker and rollback instructions.

No local secrets or unrestricted personal paths are written to tracked files.

## Differential Adjusted-Metric Materialization

Replace the current binary “replace whole basis or do nothing” decision with
three explicit plans computed from canonical semantic rows:

1. **Structural replacement**: a basis interval, adjustment factor segment,
   source fingerprint, status, or historical semantic row changed. Replace
   only that affected basis graph atomically.
2. **Frontier extension**: lineage and existing history are unchanged and only
   `materialized_through_date` advanced. Update the catalog frontier, append
   valuation dates strictly after the stored frontier, and insert/update only
   statement rows whose canonical values differ. Do not delete historical
   valuation or statement rows.
3. **No-op**: the catalog, segments, statements, and valuations are
   semantically identical. Perform no DML and do not refresh timestamps.

Comparison excludes storage-only `created_at`/`updated_at` fields and uses
DuckDB `IS DISTINCT FROM` semantics for nullable values. Every conflict update
also gains a `WHERE` predicate over semantic columns so duplicate input cannot
create new row versions. The per-code/basis transaction and PIT validation
contract remain unchanged. Tests must prove that a one-session frontier
advance writes only the new suffix and that a repeated identical
materialization leaves row values, timestamps, and free-block count stable
within DuckDB checkpoint tolerance.

## Differential Technical Metrics

Compute the desired technical metrics into a temporary relation, then in one
transaction:

- delete keys no longer present;
- update rows only where a semantic metric is distinct;
- insert keys not present;
- preserve `created_at` for unchanged rows and set it only for inserted or
  genuinely changed rows.

The table is never globally deleted. The result count and metric semantics
remain identical to the current query. A second build over unchanged
`stock_data` must execute zero persistent row mutations.

## No-op High-Volume Market Upserts

Apply the same semantic no-op rule to the relation-based writers used by
incremental sync: `stock_data_raw`, projected `stock_data`, `topix_data`,
`indices_data`, `margin_data`, `options_225_data`, `statements`, adjustment
catalog rows, adjusted statement metrics, daily valuation, `stock_master_daily`,
`index_membership_daily`, `stock_master_intervals`, `stocks_latest`, `stocks`,
and `index_master`. Conflict updates use `IS DISTINCT FROM` predicates over
persisted semantic columns. The `statements` writer retains its
non-NULL-preferred merge contract and compares the merged result, rather than
allowing a NULL source field to erase or rewrite an existing value.

Stock-master derived tables are dependency-aware. If a stock-master batch and
its membership rows have no semantic delta, skip interval/latest rebuilds
entirely. When there is a delta, rebuild or merge only affected codes and dates:
do not drop/rename the whole interval table, globally delete latest rows, or
refresh timestamps for unaffected stocks. The same rule applies to local
`index_master` supplementation.

Writer results distinguish input rows, inserted rows, genuinely updated rows,
and unchanged rows. Partition Parquet export/index work is skipped when the
corresponding canonical table or partition had no semantic mutation. This
prevents an identical incremental fetch from generating fresh DuckDB row
versions or rewriting unchanged Parquet files. Tests cover every high-volume
writer. Source relations are anti-diffed before DML and a zero-row delta skips
the statement entirely; the conflict predicate is a second correctness guard,
not the only no-op mechanism. An identical second publish must report zero
mutations, preserve Parquet byte identities, and keep DuckDB size/free blocks
and semantic digests stable within checkpoint tolerance; a DuckDB whole-file
hash is not used as the no-op assertion.

## Bounded Compaction Policy

Centralize Market maintenance after the writer has stopped and all DuckDB
handles and workers owned by that process are joined. Sync, repair-triggered
adjusted materialization, standalone materialization, stock refresh, intraday
sync (HTTP and CLI), and technical-metric rebuild all use the same finalizer.

All writable Market DuckDB opens are moved behind one infrastructure factory
that acquires a cross-process `MarketWriterLease` before opening the file and
retains it until every writable handle and worker is closed. The FastAPI
process retains that lease for the writable store lifetime; standalone sync,
intraday, refresh, materialization, and maintenance commands use the same lock
and fixed lock ordering. Compaction runs only while its caller still owns this
lease but has closed the DB handles. A second server or writer therefore cannot
hold the old inode across replacement. Read-only Dataset/backtest/analytics
readers may finish against the old immutable inode; they cannot lose a write.

Direct `duckdb.connect(..., read_only=False)` for the Market file outside this
factory is prohibited by a source guard test. Race tests use two real processes
to prove a competing writer cannot open during compaction and that a queued
writer opens only the validated replacement after the lease is released.

After `CHECKPOINT`, read `PRAGMA database_size` and compact when either:

- the existing soft condition is met: free bytes are at least 512 MiB and the
  free ratio is at least 10%; or
- the absolute hard cap is exceeded: free bytes are at least 1 GiB,
  regardless of ratio.

The hard-cap branch prevents unbounded absolute growth of a large database.
Thresholds live in the infrastructure maintenance policy, not an HTTP route.
Before compacting, require enough filesystem capacity for the compact copy and
a safety reserve. Build and verify a compact sibling, atomically exchange it
with the closed source file, fsync the parent, and reopen read-only. Before
removing the old file, compare a complete schema-object fingerprint and every
table's row count against the source snapshot; critical Market/PIT tables also
require stable semantic digests and the full schema-v4 lineage validator.
Failure keeps or restores the original exact file and reports an actionable
maintenance error; it never deletes the last valid copy.

The finalizer returns structured before/after bytes, free bytes/ratio, trigger
(`soft_threshold` or `hard_cap`), duration, and validation result. Stats and
sync/materialization job details expose this evidence. A standalone explicit
maintenance command allows retry without another data sync.

## Tests and Acceptance

Promotion tests must cover report/provenance drift, current-code drift,
descriptor replacement, symlinks, live leases, WAL/writer activity, backup
mismatch, unexpected retained files, cross-device rejection, unavailable
atomic exchange, runtime detachment, exact inode/hash preservation, no sync or
J-Quants calls, joined and unjoined failures, exchange-back rollback, backup
fallback, journal recovery at every state, one-shot consumption, and final
report validation.

Growth tests must cover structural replacement, frontier-only append, exact
no-op, nullable comparisons, technical inserts/updates/deletes/no-op, soft
compaction, hard-cap compaction below 10%, identical high-volume upserts,
unchanged Parquet partition suppression, insufficient-disk handling,
verification failure rollback, cross-process writer exclusion, full schema and
table-copy parity, stock-master dependency rebuild suppression, and every
writer finalizer including intraday. A repeated unchanged synthetic
sync/materialization cycle, including the complete stock-master stage, must
execute zero semantic row mutations and must not monotonically grow the
database or rewrite Parquet. A forced hard-cap fixture must finish below the
cap after successful compaction.

Focused gates:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  tests/unit/cli_bt/test_market_cutover_cli.py \
  tests/unit/server/services/test_adjusted_metrics_materializer.py \
  tests/unit/server/db/test_market_compaction.py
uv run --directory apps/bt ruff check src tests
uv run --directory apps/bt pyright src
```

Operational acceptance is:

1. verified immutable active-v3 backup;
2. active v4 promotion from r10 in minutes rather than a new 30-minute sync;
3. complete active semantic smoke with no external fetch or mutation route;
4. exact active payload identity matching r13 before/after;
5. committed report, consumed retained source, quarantined v3, and removal of
   only operation-owned temporary runtimes;
6. differential-write and hard-cap compaction tests proving repeated
   maintenance no longer causes unbounded file growth.

## Follow-on Maintenance

After the active cutover and bounded-growth implementation, continue the
already approved repository maintenance plan: Python 3.12 fail-fast
maintainability tooling, application/HTTP DTO boundary cleanup, split the
cutover service into bounded modules without compatibility re-exports, run the
full backend/TypeScript/OpenAPI/skill/privacy gates, and complete a
requirement-by-requirement architecture audit.
