# Retained Market v4 Rehearsal Design

## Status

Approved direction: reuse the completed Market v4 data plane from an isolated
rehearsal and rerun the full semantic smoke under the current code version,
without repeating initial sync.

## Problem

`market-v4-rehearsal-20260715-r10` completed initial sync, event-time PIT
materialization, database validation, fundamentals parity, Screening, and
Ranking. Its Dataset smoke then exposed an over-strict validator. The source
Market DB is healthy; the corrected validator is now committed as `0129a202`.

The existing `bt market-cutover rehearse` command always rebuilds the isolated
Market DB. Repeating that rebuild costs roughly 28--39 minutes even when the
only changed code affects a downstream smoke. The standalone `smoke` command
does not own a server or issue a rehearsal report, so it cannot provide the
exact evidence required by `cutover`. Editing report JSON by hand is not an
acceptable evidence path.

## Goals

- Reuse a retained isolated Market v4 root without running sync or PIT
  materialization again.
- Run the same complete semantic smoke used by a normal rehearsal, including
  Dataset create, job completion, info validation, and sample open.
- Produce a new, immutable passing rehearsal report bound to the current code,
  active-root fingerprint, smoke configuration, retained source report, and
  retained Market DB identity.
- Fail closed on provenance mismatch, mutable/active paths, stale code,
  configuration drift, schema drift, process shutdown failure, or source DB
  replacement.
- Remove rather than preserve compatibility with rehearsal reports that omit
  the new mode and process-join evidence.

## Non-goals

- Resume or partially rerun sync stages.
- Reuse an active Market root, a backup, a cutover staging root, or an arbitrary
  filesystem path.
- Repair or mutate the retained Market DB.
- Treat a source report as cutover evidence; it is provenance only. The new
  current-code smoke must independently prove readiness.
- Infer approval from manually constructed report files.

## Alternatives

### A. First-class retained rehearsal (selected)

Add `bt market-cutover rehearse-retained NEW_REPORT_ID
--source-rehearsal-id SOURCE_REPORT_ID`. The service owns the retained root,
server, lease, smoke, and report. This is the only option that removes the
rebuild cost while preserving the cutover evidence chain.

### B. Standalone smoke plus report adoption

Run an arbitrary server and convert its output into a rehearsal report. This
weakens server ownership and filesystem provenance, and would need an unsafe
report-adoption surface. Rejected.

### C. Repeat full rehearsal

Preserves current evidence semantics but wastes 28--39 minutes after every
smoke-only correction. Rejected for this workflow, while normal `rehearse`
remains the required path after data-plane-affecting changes.

## Command Contract

```text
bt market-cutover rehearse-retained NEW_REPORT_ID \
  --source-rehearsal-id SOURCE_REPORT_ID \
  --symbol 7203 \
  --strategy production/cutover_smoke \
  --dataset-preset primeMarket
```

The command accepts no source path and no reuse/force flag. The source root is
derived only as:

```text
operations/market-v4-cutover/rehearsals/<SOURCE_REPORT_ID>/root
```

This avoids a second path authority and prevents active-root reuse.

## Source Eligibility

Before starting a server, the service must prove all of the following:

1. New and source IDs pass the existing strict identifier validation and are
   distinct.
2. The new report and runtime destinations do not exist.
3. The source report is an authentic service report with:
   - `phase: rehearsal`;
   - matching `reportId`;
   - matching `smokeConfig`;
   - `serverProcessJoined: true` and `workerProcessJoined: true`;
   - a target-root fingerprint equal to the current active-root fingerprint.
4. The source report status is `passed` or `failed`; shutdown-deferred reports
   are ineligible. Error text is never parsed and source status is never copied
   into the new result.
5. The retained root is confined below the rehearsal operations directory, is
   not a symlink, has no live operation owner, and can acquire an exclusive
   `MarketOperationLease`.
6. Its configuration fingerprint equals the active configuration fingerprint.
7. DuckDB inspection reports exactly schema v4 and
   `local_projection_v2_event_time`, with ready adjusted-metric lineage.

The retained root need not be presumed complete from the source report. Schema,
lineage, DB validation, and every semantic consumer are independently checked
by the new current-code smoke. A partial retained rebuild therefore fails
closed without requiring compatibility logic for old phase arrays.

## Execution Flow

1. Capture current clean code identity, active-root fingerprint, active
   configuration fingerprint, retained-root fingerprint, and Market DB file
   identity (`device`, `inode`, size, and SHA-256).
2. Acquire an exclusive lease on the retained root.
3. Create a new report directory and a new runtime directory owned by the new
   report ID. Copy only repository/runtime configuration and strategy inputs;
   do not copy or rewrite the Market DB.
4. Start the current-code owned FastAPI server with its working directory bound
   to the retained `market-timeseries` directory.
5. Call the existing `smoke()` implementation unchanged. It validates:
   - schema and adjustment mode;
   - stats and exact adjusted-metric lineage;
   - `/api/db/validate` health;
   - Fundamentals GET/POST parity;
   - asynchronous Screening and result open;
   - Fundamental Ranking;
   - Dataset create, job completion, info validation, and sample open.
6. Stop the server and join all owned server and background workers.
7. Re-check current code identity, active-root fingerprint, retained-root
   identity, and retained Market DB identity. Dataset artifacts may be added
   below the retained rehearsal's Dataset directory, but the Market DB and its
   Parquet tree must remain byte-identical.
8. Atomically write the new passing report.

## Report Contract

Both normal and retained passing rehearsal reports must include:

- `phase: rehearsal`;
- `status: passed`;
- `rehearsalMode`: `full_rebuild` or `retained_market_smoke`;
- `codeVersion`;
- `targetRootFingerprint`;
- `smokeConfig`;
- `serverProcessJoined: true`;
- `workerProcessJoined: true`;
- complete `apiChecks`, schema coverage, lineage evidence, and phase timing.

A retained report additionally includes:

- `sourceRehearsalReportId`;
- `sourceRehearsalCodeVersion`;
- `sourceRetainedRootFingerprint`;
- `sourceMarketIdentityBefore` and `sourceMarketIdentityAfter`;
- a `retained_market_smoke` phase timing.

`cutover` must require the mode, join booleans, and full provenance fields. It
must reject legacy reports missing them. For `retained_market_smoke`, it also
re-resolves the source report and verifies its ID, target fingerprint, and
retained-root confinement. No alias or dual report reader is added.

## Failure and Cleanup

- Any eligibility failure happens before runtime creation.
- A smoke failure writes a failed report with redacted diagnostics and retained
  provenance, then stops and joins all owned work.
- If server or worker join fails, the report status is
  `stop_failed_cleanup_deferred`, the lease is not unlocked, and the report is
  never cutover-eligible.
- A failed Dataset build leaves no resolvable manifest, following the existing
  Dataset contract.
- The retained Market DB is never deleted or restored by this command.

## Tests

Service and CLI tests must prove:

- the command help and option mapping;
- no sync/materialization endpoint is called;
- the exact existing semantic smoke is called once;
- passing and cleanly joined failed source reports are provenance-eligible, but
  a partial source can pass only if the new complete smoke independently
  validates it;
- pre-rebuild failure, config drift, active fingerprint drift, wrong smoke
  config, symlink/path escape, schema mismatch, mode mismatch, live lease,
  source DB mutation/replacement, dirty/stale code, and process join failure all
  fail closed;
- success report contains the required mode/provenance/join evidence;
- `cutover` accepts exact full and retained reports and rejects reports missing
  the new contract;
- r10-shaped regression fixture reaches Dataset info/sample without invoking
  sync.

Focused verification:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt ruff check \
  src/application/services/market_v4_cutover.py \
  src/entrypoints/cli/market_cutover.py
uv run --directory apps/bt pyright \
  src/application/services/market_v4_cutover.py \
  src/entrypoints/cli/market_cutover.py
```

Operational acceptance is a retained rehearsal against r10 that completes the
full smoke and produces an exact current-code passing report without any
J-Quants request or sync job.

## Follow-on Work

After retained rehearsal and active cutover, continue the original maintenance
plan: Python 3.12 maintainability fail-fast, application/HTTP DTO boundary,
`market_v4_cutover.py` module split, CI/docs updates, full repository gates, and
requirement-by-requirement completion audit.
