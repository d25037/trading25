# Market v5 Review Hardening Design

## Context

PR #493 changes the active Market Data Plane to schema v5 and makes provider-adjusted prices the consumer-facing source of truth. Pre-merge review found that the core publication and cutover design is sound, but several boundary cases can bypass the promised atomic migration, misstate provider provenance, or leave DuckDB and Parquet inconsistent.

This design closes those review findings without adding compatibility reads, in-place v4 migration, or a second upgrade path. Market v4 and earlier remain incompatible. The isolated `bt market-cutover cutover` command remains the only supported v4-to-v5 transition.

## Chosen approach

Implement the complete safety model in four independently testable units:

1. enforce the migration boundary at every public recovery surface;
2. make provider vintage provenance exact for each symbol window;
3. make repeated publication repair all physical projections and fail closed on filesystem errors;
4. make cutover activation recoverable after process death, not only caught exceptions.

The alternatives were rejected because a global provider label cannot prove mixed-window lineage, an operator-only hard-crash runbook cannot provide exact recovery, and splitting cutover safety out of #493 would leave the v5 contract partially implemented.

## 1. Migration boundary

`POST /api/db/sync` may retain `resetBeforeSync` only as maintenance for an already-compatible Market v5 root. It must inspect the active root before reset. If the root is v4, older, malformed, or otherwise incompatible, the request fails before deleting or moving any active resource and returns recovery guidance for `bt market-cutover cutover`.

The web Settings recovery action and database validation messages must not advertise live reset as a schema-upgrade mechanism. They should distinguish:

- compatible v5 repair/reset, which may use normal sync; and
- incompatible pre-v5 recovery, which must use the isolated cutover command.

Tests must prove that an incompatible active tree is byte-for-byte unchanged after a rejected reset request and that an already-v5 reset remains available only where intentionally supported.

## 2. Per-window provider provenance

`stock_provider_windows` becomes the source of truth for each symbol's provider plan, request frontier, observed coverage, and fingerprint. Add `provider_plan` to the physical schema, contract, row model, readers, diagnostics, and Dataset copy path.

`provider_as_of` is the request/stage frontier supplied by the caller. It is not derived from the last observed quote. `coverage_start` and `coverage_end` remain derived from rows actually present for the symbol. Therefore a suspended symbol can have `coverage_end < provider_as_of` while still belonging to the same exact provider vintage as actively traded symbols.

All publishing entry points must pass the stage-level plan and frontier explicitly. Dataset creation must require selected windows to agree on `provider_plan` and `provider_as_of`; it must not fall back to the mutable global metadata label. Global metadata may remain as an aggregate diagnostic, but it is not lineage authority.

Tests must cover:

- two symbols fetched under one frontier where one has no row on the frontier;
- a partial refresh after a plan change, proving mixed plans are detected rather than relabeled;
- Dataset manifest lineage derived from per-window values.

## 3. Publication correctness

### Corporate-action evidence on no-trade rows

An all-null price row with a non-unit `AdjFactor` is not safely discardable. The ingestion boundary must either preserve it as corporate-action evidence that triggers the affected-symbol full-window path or reject it as incomplete provider data for retry. It must never be treated as an ordinary no-trade row. Ordinary null rows with a unit/absent factor may continue to be skipped.

### Projection repair

`replace_stock_provider_window` must compare the desired consumer-facing `stock_data` projection as well as raw rows, adjustment events, window ledger, and metadata. Exact raw replay over a missing or corrupted projection must repair the projection instead of taking the no-op fast path. Compaction validation must treat provider projection mismatches as invalid input.

### Parquet deletion

Partition deletion is part of publication and must fail closed. Remove `ignore_errors=True` behavior for daily and minute partition removal. Dirty-date/table retry state is cleared only after deletion succeeds. An injected permission or I/O failure must surface, retain retry state, and leave the operation observably incomplete.

Each behavior gets a focused regression test that fails on the reviewed head and passes after the minimal implementation.

## 4. Durable cutover recovery

Caught-exception rollback remains, but activation also gains a create-only, fsynced attempt journal stored outside the trees being exchanged. Before activation it records the exact attempt/report ID, source identity, staged identity, active identity, immutable backup ID/path, and expected post-activation identity.

The journal advances through explicit durable states around the filesystem exchange:

- `prepared`: identities and backup are verified; no exchange has committed;
- `exchange_started`: recovery must inspect active/staged identities rather than assume either side;
- `activated`: active identity matches the expected v5 tree, but success publication may be incomplete;
- `reported`: the durable success report is published and verified.

Re-running the same cutover attempt ID invokes recovery before starting new work. Recovery verifies exact filesystem and DuckDB identities, then deterministically either restores the immutable backup or completes success publication. It must never infer ownership from filenames alone, reuse a journal for different IDs, or delete the backup/quarantine evidence.

Crash-injection tests cover process termination immediately before exchange, immediately after exchange, and after activation but before success-report publication. Each test starts a fresh service/process and proves same-ID recovery reaches one exact state with no unowned active tree.

## Contracts and documentation

Because the physical schema and API recovery guidance change, regenerate and validate:

- Market DB schema contract;
- FastAPI OpenAPI snapshot and generated TypeScript types;
- skill references derived from API/CLI contracts;
- Market v5 cutover runbook and AGENTS invariants.

Remove stale v4/local-projection wording and describe provider-adjusted publication precisely.

## Verification

Every unit follows red-green testing. After focused tests and per-task review, run the full bt test suite, ruff, pyright, TS workspace tests, typecheck, lint, OpenAPI sync/check, strict skill audit, research guardrails, and the isolated Market v5 benchmark/smoke set. The PR is marked ready only after the branch is integrated with final `main`, all generated artifacts are clean, a final broad review has no Critical or Important findings, and GitHub required checks are green.
