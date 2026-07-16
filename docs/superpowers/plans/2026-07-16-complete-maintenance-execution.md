# Complete Maintenance Execution Roadmap

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development`. Each task is implemented with TDD,
> followed by an independent specification review and an independent quality
> review before the task commit is accepted.

**Goal:** Complete the original repository-maintenance objective: promote the
already validated retained Market v4 payload without another sync, permanently
bound Market DuckDB growth, remove the remaining application-to-HTTP DTO debt,
make maintainability tooling Python-3.12-correct, split the cutover monolith,
and prove the complete repository state with full gates and an explicit
requirement audit.

**Architecture:** Work in dependency-ordered waves. The operational promotion
lands first and uses atomic directory exchange plus a durable journal. Growth
control then removes dead writer APIs, introduces semantic delta writes,
cross-process writer ownership, and verified hard-cap compaction. The final
maintenance wave removes boundary debt and the cutover monolith after active
evidence exists. No legacy aliases, dual readers, compatibility modules, or
new full sync are introduced.

**Tech stack:** Python 3.12, DuckDB, FastAPI, Typer, Pydantic 2, pytest, Ruff,
Pyright, Bun, React 19, OpenAPI-generated TypeScript contracts, GitHub Actions.

This file is the dependency and completion roadmap. Each wave receives a
separate executable writing-plans document with exact interfaces, named RED and
GREEN tests, commands, and commit boundaries before production edits begin.
Wave 1 is specified by
`2026-07-16-retained-market-promotion.md`; Wave 2 and Wave 3 plans are written
after the preceding operational gate so their file maps reflect the actual
post-wave repository rather than preserving stale paths.

## Authoritative execution status (2026-07-16)

This status block is the durable resume point for the complete-maintenance
roadmap. It takes precedence over unchecked task boxes below, which describe
acceptance requirements and are not maintained as the execution ledger.

- Wave 1 is complete. Retained Market v4 promotion
  `market-v4-active-20260716-r3` is committed and independently verified with
  `noSync=true` and `noJQuants=true`.
- Wave 2 Tasks 6-11 are complete and independently reviewed clean. The accepted
  Task 11 head is `7d24bbb3`; its focused gate is `722 passed`, with Ruff,
  Pyright, skill audit, and diff checks clean.
- Wave 2 Task 12 is complete (`df48a082`, `05dbe65a`, `b59e578d`). Verified
  soft/hard-cap compaction, closed-writer maintenance authority, cross-platform
  atomic regular-file exchange, complete catalog/data validation, durable
  recovery, and `market-maintain` are implemented. The final independent
  specification and quality review is clean; the broad related gate is
  `619 passed`. Acceptance used synthetic local fixtures only, without a full
  sync or operational XDG Market DB access.
- Wave 2 Task 13 is complete (`5b82e8e2`, `1257d786`, `4bd6cca7`,
  `9acada6e`). The common two-phase maintenance finalizer, strict fsynced
  evidence sidecar, job/SSE/HTTP/CLI lifecycle ordering, current-generation
  shutdown, and OpenAPI/TypeScript contracts are implemented. The accepted
  independent specification and quality review is clean; the final focused
  gate is `275 passed` and the related broad backend gate is `577 passed`.
- Wave 2 Task 14 is complete (`0ed85c69`, `a6c29d6b`). The accepted synthetic
  integration runs three identical cycles with explicit non-empty first-cycle
  evidence for all 14 named relation families (including technical metrics),
  exact zero mutations on cycles two and three, stable counts, 22 stable
  Parquet inode/hash artifacts, bounded DB/free-block growth, and verified
  forced hard-cap compaction. The final independent review is clean; the broad
  Market gate is `1,348 passed`. No full sync, J-Quants call, or operational
  XDG mutation was used.
- Wave 3 Task 15 is complete. The snapshot now fails fast below Python 3.12,
  parses every tracked Python source with its relative filename and Python 3.12
  grammar, rejects malformed syntax/UTF-8, contains no wall-clock artifact
  metadata, and is a deterministic always-on CI and pre-push gate against both
  committed artifacts. CI also executes the real script under Python 3.9 before
  the Python 3.12 snapshot check.
- Wave 3 Task 16 is complete. All application-to-HTTP schema imports are
  removed, canonical response/value DTO ownership is in application contracts,
  the Market maintenance contract is shared, response-only HTTP schema modules
  are deleted, and the blanket architecture guard plus exact OpenAPI/TypeScript
  gates are clean.
- Wave 3 Task 17 is complete (`2f414853`, `255f01a2`). The production and test
  monoliths are replaced by an acyclic explicit-composition package and
  responsibility-focused tests. The facade is 190 lines with 10 explicit
  collaborators; there are no mixins, method grafts, dynamic forwarders, root
  compatibility exports, or fixed-depth resource paths. The final independent
  review is clean; targeted `401 passed` and broad services `1,378 passed`.
- Wave 3 Task 18 is the current resume point: complete backend/TypeScript gates
  and the requirement-to-evidence completion audit.
- The user-owned untracked `.codex/config.toml` must remain untouched.

Resume order is fixed: Task 12 -> Task 13 -> Task 14 -> Task 15 -> Task 16 ->
Task 17 -> Task 18. Task 18 is the current resume point. The original
maintenance objective is not complete until
Task 18 produces a current requirement-to-evidence audit with a clean tracked
worktree.

## Global constraints

- Do not call `/api/db/sync` or J-Quants during retained promotion.
- Preserve the untracked user file `.codex/config.toml`.
- Work directly on `main`, as already approved, with one reviewed commit per
  bounded task.
- Every production change starts with a failing focused test.
- Application code must not import HTTP schema modules after the DTO wave.
- Market schema v4 and `local_projection_v2_event_time` remain the only active
  contract; no v3 migration/compatibility path is added.
- A job is not terminal until workers join, handles close, maintenance finishes,
  read-only resources reopen, and maintenance evidence is attached.
- Do not claim completion until every acceptance item at the end has direct
  current-state evidence.

---

## Wave 1: Atomic retained Market promotion

### Task 1: Atomic exchange and durable journal primitives

**Files:**

- Modify: `apps/bt/src/application/services/market_v4_cutover.py`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`

- [ ] Add RED tests for descriptor-relative atomic directory exchange: inode
  preservation, same-device enforcement, unavailable syscall, symlink/parent
  replacement, and injected fsync failure.
- [ ] Add RED tests for append-only numbered journal records, strict transition
  order, file and parent fsync, reload, torn record, duplicate state, and
  impossible identity combinations.
- [ ] Implement an injectable Darwin `renameatx_np(..., RENAME_SWAP)` adapter.
  It must use retained parent descriptors and fail closed on `EXDEV` or missing
  support; it must not copy or use two ordinary renames.
- [ ] Implement immutable promotion-state records for `validated`,
  `runtimes_detached`, `prepared`, `exchanged`, `quarantined`,
  `active_smoke_passed`, `report_persisted`, `committed`, `exchanged_back`,
  `rolled_back`, and deferred recovery.
- [ ] Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover.py -k 'atomic_exchange or promotion_journal' -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
```

### Task 2: Promotion eligibility, in-operation backup, and runtime detachment

**Files:** same service/test files as Task 1.

- [ ] Add RED rejection matrices for retained/current report SHA and provenance,
  code/config/root/schema/lineage/payload drift, source replacement, live lease,
  non-empty WAL, unexpected top-level entry, unproven runtime, existing IDs,
  insufficient backup capacity, and cross-device roots.
- [ ] Add `promote_retained(...)` with the fixed lock order active exclusive then
  retained exclusive; retain both locks until commit or proven rollback.
- [ ] Derive r10 only through r13 provenance. Re-hash the exact DB plus 7,327
  Parquet files and bind report SHA, report code versions, root fingerprints,
  smoke config, and current pre-inspection.
- [ ] Inside the active lease, capture v3 identity, create the named backup with
  create-only paths, fsync and verify its manifest, and prove active identity did
  not change. Do not call full-rebuild `_preflight_under_lease`.
- [ ] Detach only exact report-proven r10/r12/r13 runtime directories into an
  operation holding area via no-replace rename. Require the remaining retained
  top-level allowlist to be exactly `market.duckdb` and `parquet`.
- [ ] Prove cross-device/syscall/report/path/identity/ID eligibility failures
  occur before *any* backup, journal, holding-directory, or runtime mutation.
  The first allowed mutation is create-only backup construction after complete
  eligibility.

### Task 3: Happy-path promotion, no-mutation smoke, report, and CLI

**Files:**

- Modify: `apps/bt/src/application/services/market_v4_cutover.py`
- Modify: `apps/bt/src/entrypoints/cli/market_cutover.py`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`
- Test: `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`

- [ ] Add RED service and CLI tests proving the command has no source-path,
  force, copy, or J-Quants option and never calls `_run_rebuild` or a mutation
  endpoint.
- [ ] Exchange active v3 with retained v4 atomically, verify both identities,
  move v3 to managed quarantine with no-replace rename, and fsync each boundary.
- [ ] Start a current-code active server with
  `TRADING25_RUNTIME_CAPABILITY=retained_market_smoke`, a scrubbed environment,
  and a fresh owned runtime. Run the complete semantic smoke, join all work,
  remove the runtime, and prove the canonical Market payload unchanged.
- [ ] Write the exact promotion report and source-consumed evidence described in
  the approved design, including `noSync`, `noJQuants`, all identity/report SHA
  chains, backup/quarantine/journal evidence, and process join verdicts.
- [ ] Add `bt market-cutover promote-retained`; leave `cutover` as the explicit
  full-rebuild recovery command.
- [ ] After joined smoke, revalidate provenance for every detached runtime,
  delete only those held directories, and verify the active canonical allowlist
  before report/commit. Assert that all J-Quants key/token/plan variables are
  absent and logs contain no external fetch event.

### Task 4: Rollback and crash recovery

**Files:** service/test files from Tasks 1-3.

- [ ] Inject failures after every journal, exchange, quarantine, smoke, cleanup,
  and report-publish boundary.
- [ ] If all children joined, exchange active v4 and quarantine v3 back,
  validate exact v3 identity, and re-home v4 to retained source. Use immutable
  backup restore only when exchange rollback cannot be proven.
- [ ] Restore detached runtime locations when rollback evidence requires them.
- [ ] If any child is unjoined, inherit/retain both leases, write deferred state,
  and prohibit rollback until ownership is resolved. Extend the runtime adapter
  to pass both active and retained lease FDs to the owned child.
- [ ] On same-ID restart, validate the complete journal and location identities.
  A valid committed report ends recovery; every incomplete post-exchange state
  rolls back conservatively; ambiguity performs no mutation and requires
  operator recovery. Only a matching incomplete journal enters recovery;
  committed or mismatched operation identity is rejected.
- [ ] Prove a consumed committed source cannot be promoted again.

### Task 5: Promotion gates and real active cutover

**Files:**

- Modify: `docs/runbooks/market-v4-cutover.md`
- Modify: `AGENTS.md`

- [ ] Update the canonical command, evidence, rollback, and no-sync runbook; add
  no legacy alias or report upgrader.
- [ ] Run the complete focused service/CLI suite, Ruff, and Pyright.
- [ ] Confirm clean tracked HEAD and current r10/r13 identities.
- [ ] Run `promote-retained` with fresh 2026-07-16 report/backup IDs. Do not run
  another sync.
- [ ] Independently inspect the final report/log, active stats/validate,
  fundamentals, Screening, Ranking, Dataset create/info/sample, active payload
  identity, immutable backup, and quarantined v3.
- [ ] Remove only operation-owned runtimes; retain the backup and quarantine.

---

## Wave 2: Bounded Market growth (A+)

### Task 6: Delete unused writable APIs and define mutation results

**Files:**

- Modify: `apps/bt/src/infrastructure/db/market/market_db.py`
- Delete or reduce: `apps/bt/src/infrastructure/db/market/time_series_writers.py`
- Create: `apps/bt/src/infrastructure/db/market/market_mutations.py`
- Modify related Market DB/store unit tests.

- [ ] Add a source-usage test proving the legacy `MarketDb` time-series upserts
  and unused standalone adjusted writer methods have no production caller.
- [ ] Delete those APIs rather than maintaining two writer implementations.
  Keep `DuckDbParquetTimeSeriesStore`, atomic basis publication, and dated
  stock-master publication as the only supported paths.
- [ ] Move lineage validators out of writer-private imports into a focused
  validation module.
- [ ] Add `MarketMutationStats(input, inserted, updated, unchanged, deleted)`
  with `mutated_rows` and replace misleading input-count result contracts.

### Task 7: Semantic delta kernel and high-volume time-series writers

**Files:**

- Create/modify: `market_mutations.py`
- Modify: `time_series_store.py`, its publish/index/export helpers, and tests.

- [ ] RED-test nullable `IS DISTINCT FROM`, duplicate last-wins input, inserts,
  true updates, and exact no-op with stable timestamps.
- [ ] Stage deduplicated input, anti-diff it before DML, and skip the statement
  entirely for a zero-row delta; conflict predicates are a second guard.
- [ ] Convert raw/projected daily stock, minute stock, TOPIX, indices, margin,
  options, and statements. Preserve non-NULL-preferred statement merge.
- [ ] Track only mutated/deleted keys and partitions. Project only affected raw
  keys unless an adjustment semantic change requires a full affected-code
  reprojection. The full affected-code result is still anti-diffed and never
  delete/reinsert-all.
- [ ] Skip Parquet export/index for unchanged partitions and prove a second
  identical publish preserves Parquet inode/hash.

### Task 8: Differential stock-master family

**Files:**

- Modify: `stock_master_writers.py`, `metadata_writers.py`,
  `sync_stock_master.py`, `market_db.py`, and focused tests.

- [ ] Delta-merge `stock_master_daily`, `index_membership_daily`,
  `stock_master_intervals`, `stocks_latest`, `stocks`, and `index_master`
  without timestamp churn. Publication returns the affected code/date sets;
  counts alone cannot drive derived updates.
- [ ] If source/master membership has no delta, skip intervals/latest rebuilds.
- [ ] For real deltas, update only affected codes/dates; remove whole-table
  drop/rename and global delete/insert paths for intervals and latest tables.
- [ ] Prove a second complete identical stock-master stage performs zero DML and
  never calls derived rebuild functions.

### Task 9: Differential adjusted materialization

**Files:**

- Modify: `apps/bt/src/domains/fundamentals/adjustment_basis.py`
- Modify: `adjusted_metrics_materializer.py`, `valuation_writers.py`,
  `adjustment_basis_writers.py`, `market_db.py`, and focused tests.

- [ ] Introduce explicit `structural`, `frontier_extension`, and `no_op` plans.
- [ ] Define `source_fingerprint` over the structural adjustment graph rather
  than the moving materialization frontier, so a factor-1 session suffix keeps
  the fingerprint stable while an adjustment-event correction changes it.
  Treat existing frontier-dependent fingerprints as a one-time structural
  transition and cover that transition explicitly.
- [ ] Structural change replaces only named basis graphs atomically.
- [ ] Frontier extension updates the frontier conditionally, appends valuation
  dates after the old frontier, and delta-upserts changed statement rows without
  deleting history. It is legal only when the new frontier strictly advances
  and basis interval, adjustment-through, source fingerprint, status, exact
  segments, and every statement/valuation row through the stored frontier are
  identical (timestamps excluded). Historical insertion/deletion/change,
  segment drift, or frontier regression is structural or fails closed.
- [ ] Exact no-op begins no transaction and executes no DML.
- [ ] Prove a one-session advance writes only the suffix and a repeated run keeps
  timestamps, semantic digest, size, and free blocks stable within tolerance.

### Task 10: Differential technical metrics

**Files:** `technical_metric_writers.py`, `market_db.py`, focused tests.

- [ ] Materialize desired metrics into a temporary relation.
- [ ] In one transaction delete only stale keys, update distinct semantic rows,
  and insert missing rows; never globally delete the table.
- [ ] Preserve `created_at` for unchanged rows and report mutation stats plus
  final count.
- [ ] Prove exact second-run zero DML.

### Task 11: Cross-process writer ownership

**Files:**

- Create focused infrastructure lease/resource modules.
- Modify writable Market connection factories, FastAPI app/resource lifecycle,
  sync/refresh/materialization/intraday CLI and HTTP entrypoints.
- Add architecture/source-guard and multiprocess tests.

- [ ] Extract the reusable operation lease from the cutover monolith without a
  compatibility re-export.
- [ ] Require every writable Market open to use one `MarketWriterLease` factory;
  retain the exclusive cross-process lease until all writable handles/workers
  close. Read-only readers remain allowed on the old immutable inode.
- [ ] Add a guard forbidding direct writable `duckdb.connect` to Market outside
  the factory/compaction worker.
- [ ] Prove with real processes that a competing writer blocks and later opens
  only the validated replacement.

### Task 12: Verified soft/hard-cap compaction

**Files:**

- Modify: `market_compaction.py`
- Add focused compaction verifier/policy tests.

- [ ] Centralize the soft rule (512 MiB and 10%) and absolute hard cap (1 GiB,
  regardless of ratio).
- [ ] While holding writer ownership with closed handles, checkpoint, validate
  capacity for compact copy plus reserve, build a sibling, and compare complete
  schema-object fingerprint and all table counts. Critical tables also require
  semantic digests and full v4/PIT validation.
- [ ] Reuse the tested atomic-exchange/fsync primitive to exchange the verified
  file, revalidate the active source identity, then remove the old file.
  Any failure restores/preserves the exact original.
- [ ] Return structured trigger/before/after/free/duration/validation evidence.
- [ ] Cover hard cap below 10%, low disk, copy/verifier failure, fsync failure,
  rollback, and successful below-cap result.

### Task 13: Common maintenance finalizer and job lifecycle

**Files:**

- Create: common Market maintenance finalizer and sidecar contract.
- Modify: sync/materialization/refresh/intraday/technical rebuild orchestration,
  job managers, routes, CLIs, stats, schemas/contracts/tests.

- [ ] Replace the sync-route-only finalizer. Every high-churn writer uses:
  worker join -> writable handle close -> maintenance -> read-only reopen ->
  evidence attach -> terminal job status/HTTP return.
- [ ] Cover success, failure, timeout, and cancellation; a job cannot appear
  terminal while compaction remains in flight.
- [ ] Store evidence atomically in fsynced
  `market-timeseries/maintenance.v1.json`, avoiding DML immediately after
  compaction. Stats and job details read this sidecar.
- [ ] Wire sync, repair materialization, standalone materialization, stock
  refresh, HTTP/CLI intraday, and technical rebuild.
- [ ] Add `bt market-maintain` to retry maintenance without any data sync.
- [ ] Never swallow maintenance failure as a warning. Persist failed evidence,
  reopen read-only resources, and return an actionable failed/maintenance-
  incomplete terminal result. Published data remains, but hard-cap violation
  cannot be reported as clean success.
- [ ] Regenerate OpenAPI/TS contracts if public evidence changes.

### Task 14: Integrated growth acceptance

- [ ] Measure representative raw SQL/query execution counts for adjusted source
  planning (not only loader-call counts) and prove they remain O(codes), with a
  single run-level market-session snapshot on an exact no-op rebuild.
- [ ] Run a synthetic incremental cycle twice including stock-master,
  time-series, adjusted materialization, and technical metrics. The second cycle
  must report zero semantic mutations, rewrite no Parquet, and not monotonically
  increase DB size/free blocks.
- [ ] Force the 1 GiB-equivalent test threshold and prove verified compaction
  leaves free bytes below the configured cap.
- [ ] Run all Market DB/service/route/CLI tests, Ruff, Pyright, and contract sync.

---

## Wave 3: Remaining original maintenance

### Task 15: Python 3.12 maintainability fail-fast and CI gate

**Files:**

- Modify: `scripts/maintainability_snapshot.py`
- Modify/add its unit tests and CI/pre-push workflow coverage.

- [x] RED-test execution under Python 3.9 and a source SyntaxError.
- [x] Require Python >=3.12 and exit 2 with the exact recovery command
  `uv run --project apps/bt python scripts/maintainability_snapshot.py ...`.
- [x] Parse with filenames and never swallow `SyntaxError` or omit unsupported
  AST nodes silently.
- [x] Run the snapshot in CI under Python 3.12, regenerate its committed
  baseline/report, and prove current large-file/function findings are complete.

### Task 16: Remove all remaining application-to-HTTP schema imports

**Files:**

- Create/extend: `apps/bt/src/application/contracts/market_data_plane.py` and
  focused application contract modules as needed.
- Modify all 20 import nodes / 19 exact edges, moving ownership for the audited
  127 directly imported symbols; update HTTP schemas/routes, contract tests,
  architecture baseline, OpenAPI/TS generated contracts.

- [x] Inventory every imported response/value DTO and move canonical ownership
  to application contracts. HTTP-only request models remain at the HTTP edge.
- [x] Update application services directly; HTTP schemas/routes compose or use
  the application contracts without aliases, re-exports, forwarding modules,
  subclasses, or duplicate canonical models. HTTP-only request/response
  envelopes and explicit edge mapping remain allowed where required to preserve
  the exact public OpenAPI contract.
- [x] Delete emptied HTTP schema modules and reduce the exact architecture
  baseline to zero; strengthen the guard from a ratchet to a blanket ban.
- [x] Prove normalized OpenAPI semantic diff is zero, then run backend tests,
  `bt:sync`, TS contract/API-client/web tests, Ruff, and Pyright.

### Task 17: Split the cutover monolith with no compatibility module

**Files:**

- Replace `application/services/market_v4_cutover.py` with a focused package:
  contracts/errors, filesystem, leases, DuckDB identity, runtime, backup,
  journal, smoke, reports, rebuild, promotion, and service modules.
- Split its >5,000-line test file by responsibility.

- [ ] Add/import-cycle and module-size guards before moving code.
- [ ] Move responsibilities without behavior changes or old-path re-export.
  Update every caller/test/import atomically and delete the old module.
- [ ] Target <=700 lines/module, <=600 service lines, <=180 lines/method, and
  <=1,000 lines/test module; no package `__init__` compatibility exports.
- [ ] Fix resource-root calculations explicitly rather than retaining
  `Path(__file__).parents[...]` assumptions.
- [ ] Regenerate and commit both maintainability snapshot artifacts after the
  monolith/test split; Task 15's CI gate must pass against the post-split tree.
- [ ] Run all cutover tests including real atomic exchange/recovery and the
  retained active evidence validator.

### Task 18: Full gates and completion audit

- [ ] Backend: complete pytest suite, Ruff, Pyright, maintainability snapshot,
  architecture tests, skill audit, CI taxonomy, and privacy scan.
- [ ] TypeScript: workspace tests, lint, typecheck, dependency audit, Playwright
  smoke where supported, and OpenAPI `bt:check`/`bt:sync` cleanliness.
- [ ] Inspect active Market stats/validate, maintenance sidecar, compaction
  evidence, backup/quarantine, and no-sync promotion report.
- [ ] Search for v3 compatibility, deprecated query/routes, application HTTP
  schema imports, future-leak/latest fallback, dead writer APIs, old cutover
  module imports, and undocumented writable Market opens; every match must be
  current/intentional or removed.
- [ ] Build a requirement-evidence table for the original maintenance findings,
  every approved follow-on plan, Dataset PIT work, no-sync cutover, bounded
  growth, DTO cleanup, maintainability, module split, and all named gates.
- [ ] Only when every row has authoritative passing evidence and the tracked
  worktree is clean may the active goal be marked complete.

## Execution choice

The user already selected subagent-driven execution. Continue in this session:
one implementer per bounded task, then separate spec and quality reviewers. The
main agent owns operational cutover, full gates, commits, and final evidence.
