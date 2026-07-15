# Market v4 Cutover Runbook

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

If a server or worker cannot be proven joined, rollback is deferred and both
Market leases remain fenced. Stop and account for the owned processes first;
do not unlock, delete lock files, start FastAPI, sync, or mutate either root.
Once ownership is resolved and inherited leases are released, rerun the same
command with the same three IDs so dedicated recovery can validate the durable
state. If recovery reports indeterminate evidence, identity mismatch, or
terminal rollback failure, preserve the active, retained, backup, quarantine,
journal, and cleanup-staging trees unchanged for operator review.
