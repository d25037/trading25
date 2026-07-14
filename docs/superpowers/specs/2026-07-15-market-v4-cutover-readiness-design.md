# Market v4 Cutover Readiness Design

## Status

Approved direction: implement the five maintenance stages in order and finish
with a verified production Market v4 cutover. This document fixes the safety,
ownership, and acceptance boundaries for that work.

## Problem

The repository now requires Market Data Plane schema v4 and
`stock_price_adjustment_mode=local_projection_v2_event_time`, but the active
XDG market database is still schema v3 with `local_projection_v1`. Four gaps
must be closed before that database can be rebuilt safely:

1. `repair` can update raw statements without rebuilding
   `statement_metrics_adjusted` and `daily_valuation`.
2. adjusted-metric materialization runs in a worker thread whose lifetime is
   not owned correctly across timeout and cancellation, and production-scale
   execution has insufficient chunking and progress.
3. TypeScript tests are skipped in normal CI, while Symbol Workbench does not
   preserve or present the typed `adjusted_metrics_pit` recovery response.
4. There is no current production-scale rehearsal and cutover gate covering
   schema v4 materialization, Fundamentals PIT, analytics, and Dataset v3
   bundles end to end.

Several current documents and standalone analytics guards still describe
schema v3 as supported and must be retired after cutover.

## Goals

- Make every successful fundamentals repair leave event-time adjusted metrics
  and valuations current, or fail the job with an actionable reason.
- Make validation prove source-statement-to-derived-row freshness rather than
  checking only row counts and frontiers.
- Give materialization one explicit lifecycle with cooperative cancellation,
  bounded per-code work, truthful progress, and no database teardown while a
  worker is alive.
- Restore TypeScript package and web tests as required CI gates.
- Preserve typed Fundamentals PIT errors through the shared client and show an
  operator recovery path without retrying deterministic 404/409/422 responses.
- Rehearse, back up, rebuild, validate, and smoke-test the active XDG Market v4
  Data Plane without deleting the rollback artifact.
- Remove current documentation and executable guards that still claim Market
  v3 support.

## Non-goals

- No in-place schema v3-to-v4 migration, dual read, compatibility alias, or
  current/latest data fallback.
- No automatic deletion of the pre-cutover database or Parquet tree.
- No change to the physical Dataset manifest filename `manifest.v2.json`; its
  payload remains `schemaVersion: 3`.
- No frontend-local financial calculation or recovery-side materialization.
- No attempt to make external J-Quants availability deterministic. External
  failures must stop cutover before activation and retain the old data plane.

## Architecture

### 1. Repair freshness and validation

Repair execution records whether raw fundamentals changed. If the repair
published at least one statement row, it must invoke the same
`adjusted_metrics_pit` application service used by initial and incremental
sync before reporting success. A repair with no statement changes skips the
stage.

Validation compares the bounded source statement keys and their materialized
provenance against every ready adjustment basis. Missing, extra, stale, or
wrong-basis adjusted rows are actionable warnings or errors with recovery
stage `adjusted_metrics_pit`. A mere matching row count is insufficient.

Repair orchestration remains I/O coordination. Fingerprint/key comparison
belongs in the market repository/query layer, and the validation service only
maps diagnostics to the public response.

### 2. Materializer lifecycle and bounded execution

Introduce a materialization run abstraction owned by the sync job. It exposes
cooperative cancellation and progress at code boundaries. Each code is read,
computed, and published as a bounded unit; the implementation must not retain
all-market statements, raw prices, bases, adjusted metrics, or valuations in
one Python collection.

Cancellation and timeout request cancellation, then await the shielded worker
to reach a code boundary before closing or reopening DuckDB resources. Job
state becomes cancelled or failed only after worker termination is observed.
Standalone materialization uses the same lifecycle.

Atomicity is per code and basis graph: catalog rows, segments, adjusted
metrics, and valuations for a code publish in one transaction. A cancelled
run may leave already committed codes current and later codes unchanged;
validation reports the remaining coverage and rerun is idempotent.

Progress reports completed codes, total codes, current code, published basis
count, and the stage name. It must not claim completion while a worker remains
alive.

### 3. TypeScript CI and recovery UI

CI gains a required `ts-tests` job that installs the pinned Bun workspace and
runs `bun run workspace:test`. The final CI gate depends on it. Existing lint,
typecheck, dependency audit, contract sync, and Playwright responsibilities
remain separate.

The shared HTTP client parses the unified FastAPI error body into a typed
error that preserves HTTP status, message, details, correlation ID, and PIT
recovery metadata. TanStack Query retries transient failures only; 404, 409,
and 422 Fundamentals responses are not retried.

Symbol Workbench displays the backend message and correlation ID. For a 409
whose recovery stage is `adjusted_metrics_pit`, it presents a link to Market
DB recovery guidance/status. The UI does not invoke materialization directly
and does not calculate a fallback response.

Dataset Info displays payload schema version, source Market schema version,
and adjustment mode so an operator can verify event-time lineage.

### 4. Rehearsal and production cutover

Cutover is a gated workflow, not an implicit startup behavior:

1. Record disk capacity and active DB metadata.
2. Stop writers and confirm no active sync/materialization job.
3. Create an immutable, timestamped backup of `market.duckdb` and the Parquet
   tree; record sizes and checksums in a cutover report.
4. Rehearse reset, initial sync, materialization, validation, API smoke, and
   Dataset creation against an isolated XDG root.
5. Require backend, TypeScript, contract, skill, and cutover smoke gates to
   pass.
6. Run the same reset-and-build workflow against the active XDG root.
7. Activate only after Market v4 metadata, event-time basis coverage,
   Fundamentals GET/POST parity, screening/ranking reads, and Dataset v3
   bundle validation pass.

If any pre-activation step fails, retain the old active data plane and the
backup. If a post-reset step fails, restore from the recorded backup rather
than adding a compatibility read path. The backup is never automatically
deleted by this project.

The smoke report records commands, timestamps, schema/mode, row and coverage
counts, durations, failure diagnostics, API results, and artifact paths. It
must redact credentials and local personal paths before any tracked report is
committed.

### 5. v3 retirement

Standalone analytics that assert schema v3 switch to the shared Market v4
compatibility guard. Current SoT documentation, PIT invalidation register,
skills, runbooks, and smoke reports are updated to state that schema v3 and
payload schemaVersion 2 are unsupported. Historical material under
`docs/archive/` may retain old facts but must be clearly archival and must not
be referenced as a current runbook.

## Data flow

```text
repair raw statements
  -> conditional adjusted_metrics_pit run
  -> per-code atomic basis/metrics/valuation publish
  -> source-derived validation diagnostics
  -> API/Market DB recovery state

Fundamentals 409
  -> shared typed TS error
  -> no deterministic retry
  -> Workbench recovery guidance

isolated rehearsal
  -> immutable backup + checksum
  -> active resetBeforeSync v4 rebuild
  -> materialize + validate + smoke
  -> activate or restore backup
```

## Error and cancellation semantics

- Repair cannot report success when its required materialization failed.
- Timeout is a cancellation request followed by worker join, not coroutine
  abandonment.
- Validation failures never trigger read-side repair or a legacy fallback.
- Deterministic Fundamentals 404/409/422 responses remain visible to the
  caller; transient network and 5xx failures retain bounded retry behavior.
- Cutover failure never deletes the rollback artifact.

## Testing strategy

- Unit tests: conditional repair invocation, source-derived freshness,
  per-code transactions, cancellation checkpoints, truthful progress, typed
  TS errors, retry predicates, Dataset Info lineage.
- Concurrency tests: a blocking real worker proves timeout/cancel waits before
  resource teardown; cancellation between codes leaves only complete atomic
  code graphs.
- Integration tests: repair statement change followed by PIT read, isolated
  Market v4 initial sync/materialization/validation, Fundamentals GET/POST
  parity, screening/ranking reads, Dataset v3 create/open.
- CI: backend tests, Ruff, Pyright, OpenAPI contract check, TS workspace tests,
  typecheck, dependency audit, lint, skill audit, privacy scan.
- Operational evidence: isolated rehearsal report and active cutover report
  with backup checksum and recovery instructions.

## Acceptance criteria

1. A repair that changes statements cannot finish successfully without a
   successful `adjusted_metrics_pit` stage.
2. Validation detects missing or stale derived rows for exact source
   statements and ready bases.
3. Timeout/cancel cannot close or replace database resources while the
   materializer worker is alive.
4. Materialization is bounded per code, reports code-level progress, is
   idempotent, and preserves per-code graph atomicity.
5. Normal CI runs all TS package/web tests and the final gate depends on them.
6. Workbench preserves typed PIT recovery errors, avoids deterministic retry,
   and presents the Market DB recovery path.
7. Dataset Info exposes manifest payload schema, source Market schema, and
   adjustment mode.
8. Isolated rehearsal passes before the active XDG reset begins.
9. The active database reports schema v4 and
   `local_projection_v2_event_time`; basis, adjusted-metric, and valuation
   validation pass.
10. Fundamentals GET/POST, screening, ranking, and Dataset creation/opening
    pass production smoke against the rebuilt data plane.
11. A checksummed pre-cutover backup and restoration instructions remain.
12. Current executable guards and SoT documentation no longer claim Market v3
    support.

