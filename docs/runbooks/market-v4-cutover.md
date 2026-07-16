# Market v4 Cutover Runbook

## Choose the supported path

Use the full-rebuild path when there is no eligible retained Market v4
rehearsal report and payload. `promote-retained` is unavailable unless the
exact retained report, its source rehearsal report, the service-owned retained
root, and their recorded identities all still validate.

The two supported paths are:

1. no retained v4 evidence: full rehearsal + immutable backup + full cutover;
2. eligible retained v4 evidence: retained rehearsal + atomic promotion.

A Web `Initial` sync with `resetBeforeSync=true` is not a cutover substitute.
It deletes the active `market.duckdb`, WAL, and Parquet tree before rebuilding,
does not create an immutable verified backup, and has no automatic rollback.

## Full rebuild when no retained v4 payload exists

Run all commands from a clean checkout at the exact commit being activated.
Choose unique, create-only IDs and keep them unchanged through rehearsal,
cutover, validation, and any recovery attempt.

### 1. Stop writers and load rebuild authorization

Stop FastAPI, sync workers, intraday workers, backtests, screening jobs, and
every process that can open or mutate the active Market root. Do not continue
until `market-cutover preflight` proves exclusive ownership.

The rebuild commands require `JQUANTS_PLAN` in the current shell. Loading an
env file in another terminal or only configuring the service process is not
sufficient:

```bash
set -a
source "${XDG_CONFIG_HOME:-$HOME/.config}/trading25/config.env"
set +a
test "${JQUANTS_PLAN:-}" = free \
  || test "${JQUANTS_PLAN:-}" = light \
  || test "${JQUANTS_PLAN:-}" = standard \
  || test "${JQUANTS_PLAN:-}" = premium
```

Use an isolated data root when verifying the procedure against a clean
checkout. Omit `--data-root` only for the real active cutover:

```bash
export CUTOVER_DATA_ROOT=/absolute/path/to/isolated/trading25
export REHEARSAL_ID=market-v4-rehearsal-20260717-r1
export BACKUP_ID=market-v3-pre-v4-20260717
export CUTOVER_ID=market-v4-active-20260717
uv run --directory apps/bt bt validate production/cutover_smoke
```

### 2. Preflight, rehearse, and create the immutable backup

The rehearsal rebuilds an isolated root and runs the complete semantic smoke;
it does not activate that root. Create the immutable backup only after the
rehearsal passes and immediately before active cutover.

```bash
uv run --directory apps/bt bt market-cutover preflight \
  --data-root "$CUTOVER_DATA_ROOT"

uv run --directory apps/bt bt market-cutover rehearse "$REHEARSAL_ID" \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket \
  --data-root "$CUTOVER_DATA_ROOT"

uv run --directory apps/bt bt market-cutover preflight \
  --data-root "$CUTOVER_DATA_ROOT"

uv run --directory apps/bt bt market-cutover backup "$BACKUP_ID" \
  --data-root "$CUTOVER_DATA_ROOT"
```

Do not edit rehearsal reports, backup manifests, or the active Market tree
between these commands. A changed code identity, smoke configuration, active
fingerprint, or backup identity invalidates the evidence.

### 3. Activate the full rebuild

The cutover command accepts only the exact passing rehearsal report and exact
verified backup. It performs the active reset/rebuild behind those gates and
restores the backup if activation or semantic smoke fails.

```bash
uv run --directory apps/bt bt market-cutover cutover "$CUTOVER_ID" \
  --rehearsal-report-id "$REHEARSAL_ID" \
  --backup-id "$BACKUP_ID" \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket \
  --data-root "$CUTOVER_DATA_ROOT"
```

### 4. Validate the active data plane

Start FastAPI only after the cutover command has returned a passing report.
Validate schema v4, `local_projection_v2_event_time`, adjusted-metric lineage,
time-series coverage, and the semantic smoke strategy:

```bash
uv run --directory apps/bt bt server --port 3002
curl --fail --show-error http://127.0.0.1:3002/api/db/stats
curl --fail --show-error http://127.0.0.1:3002/api/db/validate
uv run --directory apps/bt bt market-cutover smoke \
  --operation-id "$CUTOVER_ID-post-validate" \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket \
  --api-url http://127.0.0.1:3002 \
  --data-root "$CUTOVER_DATA_ROOT"
```

If validation fails, stop FastAPI and all writers before restoring:

```bash
uv run --directory apps/bt bt market-cutover restore "$BACKUP_ID" \
  --data-root "$CUTOVER_DATA_ROOT"
uv run --directory apps/bt bt market-cutover preflight \
  --data-root "$CUTOVER_DATA_ROOT"
```

`restore` never deletes the immutable backup. Preserve the failed cutover
report, rehearsal report, backup, logs, and operation directory for diagnosis.

### 5. Recreate required dataset snapshots

Market v4 does not resolve old dataset manifest payload schema 2 bundles.
After the active Market v4 validation passes, recreate every required archived
snapshot as `dataset.duckdb + parquet/ + manifest.v2.json` with payload
`schemaVersion: 3`. With FastAPI running against the validated root:

```bash
curl --fail --show-error \
  -H 'content-type: application/json' \
  -d '{"name":"primeMarket","preset":"primeMarket","overwrite":true}' \
  http://127.0.0.1:3002/api/dataset
```

Poll the returned `jobId` at `/api/dataset/jobs/{jobId}` and require a
successful result before using the snapshot for archived reproducibility.
Normal backtest, research, lab, and screening runs continue to use
`shared_config.data_source: market`; dataset snapshots are not their fallback.

## Retained rehearsal

Use a retained rehearsal only after correcting downstream smoke or application
code when the isolated Market v4 data plane itself is unchanged. Any change to
sync, ingest, schema, Parquet publication, PIT materialization, or other data
plane behavior requires a full `market-cutover rehearse` instead.

```bash
uv run bt market-cutover rehearse-retained market-v4-retained-20260715-r12 \
  --source-rehearsal-id market-v4-rehearsal-20260715-r10 \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

The source is identified only by its rehearsal report ID. The command accepts
no source path, force option, or compatibility mode. It reuses the service-owned
retained root, runs the current semantic smoke, and writes a new rehearsal
report bound to the current code identity.

The retained command makes zero J-Quants requests. It does not require or load
J-Quants secrets or plan configuration. Source eligibility requires an exact
report ID, rehearsal phase, passed or cleanly joined failed status, matching
smoke configuration and active-root fingerprint, successful server and worker
process joins, and validation of the current retained root. The source report's
`rehearsalMode` may predate the retained rehearsal contract; it is provenance
only and is never accepted directly as cutover evidence.

A retained rehearsal never invokes any Market mutation path:

- sync (`auto`, `initial`, or `incremental`);
- reset / `resetBeforeSync`;
- repair;
- stock refresh;
- intraday sync;
- adjusted-metric materialization.

The retained runtime enforces this boundary for every non-read-only `/api/db`
request. Dataset smoke job writes and the semantic POST reads used by the smoke
suite remain allowed.

The newly emitted report always has
`rehearsalMode: retained_market_smoke`, current-code smoke evidence, clean join
evidence, and the complete retained-source provenance. Cutover rejects legacy
reports that lack these new direct-evidence fields. Run a full
`market-cutover rehearse` when the retained source or root is ineligible.

## Retained promotion

Promote only a passing retained rehearsal whose report, source provenance,
configuration, root fingerprint, Market payload identity, schema v4 adjustment
mode, and PIT lineage still validate exactly:

```bash
uv run bt market-cutover promote-retained market-v4-active-20260716 \
  --retained-report-id market-v4-retained-20260715-r13 \
  --backup-id market-v3-pre-v4-20260716 \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

`REPORT_ID`, `--retained-report-id`, and `--backup-id` are create-only evidence
identities. The service derives the retained root from report provenance and
creates and verifies the named immutable backup inside the promotion. There is
no source-path, force, copy-fallback, compatibility, or J-Quants option. The
existing `market-cutover cutover` command remains the explicit full-rebuild
workflow and does not claim retained-promotion evidence.

The command passes an empty inherited environment. The owned smoke runtime
applies its small non-credential allowlist again, has no J-Quants credential or
plan capability, and rejects every Market mutation endpoint. Promotion does
not run sync, reset, repair, stock refresh, intraday sync, adjusted-metric
materialization, or rebuild. A successful report records `noSync: true`,
`noJQuants: true`, exact source/report SHA chains, backup and payload identities,
atomic-exchange and quarantine directory identities, semantic API checks, and
joined server/worker verdicts.

Promotion atomically exchanges the retained v4 payload with active v3, moves
v3 to the operation quarantine, starts current-code read-only semantic smoke,
and commits only after the active canonical payload is unchanged. Keep both
the immutable backup and quarantined v3 tree after success. They are recovery
evidence and are not ordinary cleanup targets.

## Recovery and cleanup

Journal append authorization is process-local. A live `PromotionJournal`
instance may continue only after its own durable append resolution or exact
same-attempt recovery. A fresh service instance cannot infer that authority
from files alone: rerunning `promote-retained` with the exact same report,
retained-report, and backup IDs first performs dedicated same-attempt recovery.
It validates immutable identities before returning a committed report,
finishing exact post-commit cleanup, or rolling an incomplete attempt back.
Never rename, edit, truncate, or delete journal/control records to make a retry
appear new; after a proven rollback, choose a new report and backup ID.

All joined failures route through journal-driven rollback. The preferred path
atomically exchanges exact v3/v4 payloads back; verified backup restore is used
only when exchange-back cannot be proven. Detached retained runtime artifacts
are restored from their exact holding or cleanup-staging identity. A failure
after commit can leave only the recorded per-operation cleanup staging/control
state; same-ID recovery completes that exact cleanup and writes its cleanup
result. Do not manually remove staging.

Preparation binds the complete artifact set and every artifact identity in the
`VALIDATED` journal record before the first detach rename. If any artifact
move, source-directory fsync, holding-directory fsync, or later journal append
fails, the service restores a split layout in two passes while both leases are
still held: first it proves every artifact exists in exactly one recorded
location, then it restores and revalidates the complete source set. A proven
restoration records terminal rollback before releasing either lease. If either
restoration or journal durability is unprovable, the precomputed evidence stays
durable and both leases remain fenced for same-ID recovery.

If a server or worker cannot be proven joined, rollback is deferred and both
Market leases remain fenced. Stop and account for the owned processes first;
do not unlock, delete lock files, start FastAPI, sync, or mutate either root.
Once ownership is resolved and inherited leases are released, rerun the same
command with the same three IDs so dedicated recovery can validate the durable
state. Successful acquisition of the active lease followed by the retained
lease is the proof that inherited child descriptors no longer own either lock;
only then may recovery continue `ROLLBACK_DEFERRED` to exact rollback. If either
lease is still owned, the rerun fails without mutation and remains deferred.
Changing any of the three IDs is an identity mismatch and never authorizes
recovery. If recovery reports indeterminate evidence, identity mismatch, or a
terminal rollback failure, preserve the active, retained, backup, quarantine,
journal, and cleanup-staging trees unchanged for operator review.
