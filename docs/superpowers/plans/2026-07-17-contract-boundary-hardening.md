# Contract Boundary Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce FastAPI/Pydantic as the sole Python-to-TypeScript wire-contract source while retaining the existing runtime fetch clients.

**Architecture:** Canonical source export produces a committed OpenAPI snapshot and generated `openapi-typescript` paths/schemas. Stable TypeScript names alias or derive from those generated contracts, while a compile-time endpoint helper binds existing client methods to generated operations. CI enforces drift, backward compatibility, and absence of handwritten wire DTO duplicates.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, Bun 1.3.8, TypeScript 5, `openapi-typescript` 7.13.0, pytest, Bun test, GitHub Actions

## Global Constraints

- FastAPI/Pydantic is the only HTTP wire-contract source of truth.
- Keep the existing runtime fetch implementation; do not add `openapi-fetch` or another runtime HTTP client.
- `bt:sync` must fail when canonical local source export fails; no implicit HTTP or stale-snapshot fallback is allowed.
- `bt:check` and CI contract checks must not rewrite tracked files.
- Canonical export must fix every OpenAPI-affecting feature flag, including `BT_ENABLE_RESEARCH_API=1`.
- Breaking changes fail CI unless an exact fingerprint has a repository-tracked, non-empty reason and unexpired ISO date approval.
- Handwritten types remain allowed only for frontend view/form/URL state and non-HTTP configuration.
- Generated artifacts remain committed and must be regenerated with the pinned `openapi-typescript` version `7.13.0`.
- Every production behavior change follows red-green-refactor; generated artifacts are exempt from test-first editing.

---

### Task 1: Strict deterministic contract synchronization

**Files:**
- Modify: `apps/ts/packages/contracts/scripts/fetch-bt-openapi.ts`
- Modify: `apps/ts/packages/contracts/scripts/fetch-bt-openapi.test.ts`
- Modify: `apps/ts/packages/contracts/scripts/check-bt-types.ts`
- Modify: `apps/ts/packages/contracts/package.json`
- Modify: `apps/ts/package.json`
- Modify: `apps/ts/README.md`
- Test: `apps/bt/tests/unit/scripts/test_check_contract_sync.py`

**Interfaces:**
- Produces: `syncOpenApiSnapshot(config, deps): Promise<number>` that only uses canonical source generation.
- Produces: `bt:sync`, `bt:check`, and explicitly named `bt:generate-offline` commands.
- Consumes: `scripts/check-contract-sync.sh` as the repository non-destructive drift gate.

- [ ] **Step 1: Add failing Bun tests for strict source-only synchronization**

  Assert source failure returns `1`, does not call `fetch`, does not read/write an existing snapshot, and logs a source-export error. Assert source spawn receives `BT_ENABLE_RESEARCH_API: '1'` for both `.venv` and `uv` attempts.

- [ ] **Step 2: Run the focused Bun test and confirm the old fallback behavior fails it**

  Run: `bun --cwd apps/ts test packages/contracts/scripts/fetch-bt-openapi.test.ts`
  Expected: FAIL because HTTP/stale snapshot fallback still succeeds and the feature flag is not fixed.

- [ ] **Step 3: Implement strict deterministic source synchronization**

  Remove implicit calls to `tryFetchFromServer` from `syncOpenApiSnapshot`. Pass a canonical environment containing `BT_ENABLE_RESEARCH_API: '1'` to every local export attempt. Keep server fetching available only behind an explicitly exported offline/diagnostic function if existing tests or operators need it; it must not be reachable from `bt:sync`.

- [ ] **Step 4: Make check and offline commands truthful**

  Change `bt:check` to invoke `../../../scripts/check-contract-sync.sh` or `openapi-typescript ... --check` without rewriting the generated file. Add `bt:generate-offline` for generation from the committed snapshot. Pin `openapi-typescript` to exactly `7.13.0` and update documentation so `workspace:dev:sync` warnings cannot be mistaken for a successful contract sync.

- [ ] **Step 5: Run Task 1 verification**

  Run:
  `bun --cwd apps/ts test packages/contracts/scripts/fetch-bt-openapi.test.ts`
  `uv run --directory apps/bt pytest tests/unit/scripts/test_check_contract_sync.py -q`
  `bun --cwd apps/ts run --filter @trading25/contracts bt:check`
  Expected: all exit `0`, and `git status --short` contains no generated artifact change.

- [ ] **Step 6: Commit Task 1**

  Commit message: `fix(contracts): make OpenAPI sync strict and deterministic`

### Task 2: CI coverage and strict OpenAPI compatibility gate

**Files:**
- Create: `scripts/openapi_compat.py`
- Create: `contracts/openapi-breaking-approvals.json`
- Create: `apps/bt/tests/unit/scripts/test_openapi_compat.py`
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/ci/test_taxonomy.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_changed_scope.py`
- Modify: `apps/bt/tests/unit/scripts/test_ci_workflow.py`
- Modify: `scripts/check-contract-sync.sh`

**Interfaces:**
- Produces: `python scripts/openapi_compat.py --base BASE.json --candidate CANDIDATE.json --approvals contracts/openapi-breaking-approvals.json --today YYYY-MM-DD`.
- Produces: stable SHA-256 fingerprints from normalized `category + JSON pointer + before + after` finding payloads.
- Approval schema: `{ "version": 1, "approvals": [{ "fingerprint": "sha256:...", "reason": "...", "expiresOn": "YYYY-MM-DD" }] }`.

- [ ] **Step 1: Write failing compatibility-checker unit tests**

  Cover compatible additions plus path/operation/success-response/schema/property removal, required-field addition, type/format/nullability/ref/item change, enum narrowing, parameter removal/change/required promotion. Cover stable fingerprints and malformed, duplicate, expired, and unused approvals.

- [ ] **Step 2: Run compatibility tests and confirm missing implementation failure**

  Run: `uv run --directory apps/bt pytest tests/unit/scripts/test_openapi_compat.py -q`
  Expected: FAIL because `scripts/openapi_compat.py` does not exist.

- [ ] **Step 3: Implement the compatibility checker**

  Implement pure comparison functions plus a CLI in `scripts/openapi_compat.py`. Resolve local `$ref` values before comparing property constraints, report deterministic sorted findings, and fail with exit `1` for unapproved findings or invalid/unused approvals. Use only the Python standard library.

- [ ] **Step 4: Add failing CI workflow and scope tests**

  Require `contract-tests` whenever `product_ci == 'true'`. For pull requests, materialize the base snapshot with `git show BASE_SHA:path` and run the compatibility checker after the drift check. Assert changes to `app.py`, `openapi_config.py`, FastAPI dependency metadata, domain response contracts, and shared models set `contracts_ci=true`, or equivalently that product CI unconditionally runs the contract job.

- [ ] **Step 5: Implement CI integration and empty approval registry**

  Add `{ "version": 1, "approvals": [] }`. Pass `${{ github.event.pull_request.base.sha }}` only for PR compatibility checks; push runs still perform source/snapshot/type drift checks. Preserve fail-closed CI-gate semantics.

- [ ] **Step 6: Run Task 2 verification**

  Run:
  `uv run --directory apps/bt pytest tests/unit/scripts/test_openapi_compat.py tests/unit/scripts/test_ci_changed_scope.py tests/unit/scripts/test_ci_workflow.py tests/unit/scripts/test_check_contract_sync.py -q`
  `./scripts/check-contract-sync.sh`
  Expected: all exit `0`.

- [ ] **Step 7: Commit Task 2**

  Commit message: `feat(contracts): enforce OpenAPI compatibility in CI`

### Task 3: Generated endpoint type utilities

**Files:**
- Create: `apps/ts/packages/contracts/src/types/endpoint-types.ts`
- Create: `apps/ts/packages/contracts/src/types/endpoint-types.test-d.ts`
- Modify: `apps/ts/packages/contracts/src/index.ts`
- Modify: `apps/ts/packages/contracts/package.json`
- Modify: `apps/ts/packages/contracts/tsconfig.typecheck.json`

**Interfaces:**
- Produces: `ApiOperation<Path, Method>`, `ApiPathParams<Path, Method>`, `ApiQuery<Path, Method>`, `ApiJsonBody<Path, Method>`, and `ApiJsonResponse<Path, Method, Status>`.
- Type parameters are constrained by generated `paths`; unsupported methods/statuses resolve to `never` or fail their constraint.

- [ ] **Step 1: Add failing type tests**

  Use compile-only assertions for representative GET and POST operations: fundamentals, screening job creation, portfolio detail, and DB sync. Assert valid path/query/body/status response assignments compile and `@ts-expect-error` invalid fields/statuses do not.

- [ ] **Step 2: Run typecheck and confirm missing exports fail**

  Run: `bun --cwd apps/ts run --filter @trading25/contracts typecheck`
  Expected: FAIL because endpoint utility types do not exist.

- [ ] **Step 3: Implement type-only extraction helpers**

  Build conditional/indexed-access types on generated `paths`, handling absent parameters or request bodies as `never` and `application/json` response media explicitly. Export them through the contracts package without adding runtime code.

- [ ] **Step 4: Run Task 3 verification**

  Run:
  `bun --cwd apps/ts run --filter @trading25/contracts typecheck`
  `bun --cwd apps/ts test packages/contracts`
  Expected: both exit `0`.

- [ ] **Step 5: Commit Task 3**

  Commit message: `feat(contracts): derive endpoint types from OpenAPI paths`

### Task 4: Migrate stable contract facades to generated schemas

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Modify: `apps/ts/packages/contracts/src/index.ts`

**Interfaces:**
- Consumes: `BtApiSchemas = components['schemas']` and Task 3 endpoint utility types.
- Produces: the same stable exported names used by web/api-clients, now as generated aliases or generated indexed-access derivations.

- [ ] **Step 1: Add failing compile fixtures for currently duplicated domains**

  Add representative generated-component equality/assignability fixtures for screening, indices, N225 options, jobs/sync, datasets, portfolio/watchlist, stock lookup, DB stats/validation, refresh, fundamentals, margin, provenance, and lab contracts.

- [ ] **Step 2: Run contracts typecheck and verify the compatibility fixtures expose handwritten drift**

  Run: `bun --cwd apps/ts run --filter @trading25/contracts typecheck`
  Expected: FAIL for at least one stricter generated/manual mismatch or exactness assertion.

- [ ] **Step 3: Replace handwritten HTTP DTOs domain by domain**

  Convert matching names to `BtApiSchemas['Name']`. Where a nested OpenAPI value lacks its own component, derive it from its generated parent using indexed access and `NonNullable`. Keep only view/form/URL/configuration types that are not transmitted as FastAPI request/response bodies. Preserve public export names to avoid consumer churn.

- [ ] **Step 4: Resolve generated optionality at adapters, not in wire aliases**

  If consumers relied on stronger handwritten required fields, update fixtures or existing normalization points to handle generated optional/null values. Do not strengthen the alias or use assertions to hide a server/client mismatch.

- [ ] **Step 5: Run Task 4 verification**

  Run:
  `bun --cwd apps/ts test packages/contracts/src/types/api-response-types.test.ts`
  `bun --cwd apps/ts run --filter @trading25/contracts typecheck`
  `bun --cwd apps/ts run quality:typecheck`
  Expected: all exit `0`.

- [ ] **Step 6: Commit Task 4**

  Commit message: `refactor(contracts): alias FastAPI wire DTOs to generated schemas`

### Task 5: Bind API clients to generated endpoint contracts

**Files:**
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/types.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/BacktestClient.ts`
- Modify: `apps/ts/packages/api-clients/src/backtest/type-compatibility-check.ts`
- Modify: `apps/ts/packages/api-clients/type-tests/ranking-contracts.ts`
- Modify: `apps/ts/packages/api-clients/type-tests/factor-regression-contracts.ts`

**Interfaces:**
- Consumes: Task 3 endpoint helpers and Task 4 stable generated aliases.
- Produces: existing runtime client methods with generated query/body/success-response signatures and no unconstrained caller-selected response generic.

- [ ] **Step 1: Add failing type tests for client signatures**

  Assert representative analytics/backtest methods accept only generated request/query types and return the exact generated success response. Add a compile failure for choosing an arbitrary `getFundamentals<T>` result.

- [ ] **Step 2: Run api-client typecheck and confirm old signatures fail expectations**

  Run: `bun --cwd apps/ts run --filter @trading25/api-clients typecheck`
  Expected: FAIL on unconstrained or duplicated signatures.

- [ ] **Step 3: Replace local wire declarations with contracts imports/re-exports**

  Remove analytics and backtest request/response duplicates that correspond to FastAPI schemas. Preserve internal client options and runtime validation types that are not wire DTOs. Derive method signatures with endpoint helpers while leaving URL building and `requestJson` calls unchanged.

- [ ] **Step 4: Update adapters for generated optionality**

  Narrow or normalize at the existing response boundary when UI/domain code needs stronger invariants. Do not cast the whole response or reintroduce parallel interfaces.

- [ ] **Step 5: Run Task 5 verification**

  Run:
  `bun --cwd apps/ts test packages/api-clients`
  `bun --cwd apps/ts run --filter @trading25/api-clients typecheck`
  `bun --cwd apps/ts run quality:typecheck`
  Expected: all exit `0`.

- [ ] **Step 6: Commit Task 5**

  Commit message: `refactor(api-clients): bind requests to generated OpenAPI paths`

### Task 6: Prevent handwritten wire DTO regressions and finalize artifacts

**Files:**
- Create: `scripts/check-ts-wire-contracts.py`
- Create: `apps/bt/tests/unit/scripts/test_check_ts_wire_contracts.py`
- Modify: `scripts/check-contract-sync.sh`
- Modify: `.github/workflows/ci.yml`
- Modify: `apps/ts/AGENTS.md`
- Modify: `docs/greenfield-implementation-checklist.md`
- Regenerate: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Regenerate: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `apps/ts/bun.lock`

**Interfaces:**
- Produces: `python scripts/check-ts-wire-contracts.py --openapi ... --contracts ... --api-clients ...` with exit `0` only when generated component names are not restated as handwritten wire DTOs.
- Consumes: migrated files from Tasks 4 and 5.

- [ ] **Step 1: Write failing duplicate-detector tests**

  Create temporary OpenAPI and TypeScript fixtures proving a handwritten colliding interface fails, a generated alias passes, a derived indexed-access type passes, a distinct UI model passes, and a duplicate in api-clients fails.

- [ ] **Step 2: Run detector tests and confirm missing implementation failure**

  Run: `uv run --directory apps/bt pytest tests/unit/scripts/test_check_ts_wire_contracts.py -q`
  Expected: FAIL because the detector does not exist.

- [ ] **Step 3: Implement and integrate the duplicate detector**

  Parse generated schema names from OpenAPI JSON and exported TypeScript declarations with deterministic standard-library logic. Check designated contracts and api-client files, print file/line/name diagnostics, and add the command to `check-contract-sync.sh` and CI.

- [ ] **Step 4: Regenerate canonical artifacts and documentation**

  Run `bun --cwd apps/ts run --filter @trading25/contracts bt:sync`, commit only deterministic snapshot/type/lock changes, and update governance docs with the strict sync, offline generation, compatibility approval, endpoint helper, and no-duplicate rules.

- [ ] **Step 5: Run the complete verification matrix**

  Run:
  `./scripts/check-contract-sync.sh`
  `uv run --directory apps/bt pytest tests/unit/scripts/test_openapi_compat.py tests/unit/scripts/test_check_ts_wire_contracts.py tests/unit/scripts/test_check_contract_sync.py tests/unit/scripts/test_ci_changed_scope.py tests/unit/scripts/test_ci_workflow.py -q`
  `bun --cwd apps/ts run workspace:test`
  `bun --cwd apps/ts run quality:typecheck`
  `bun --cwd apps/ts run quality:lint`
  `uv run --directory apps/bt ruff check scripts/export_openapi.py ../../scripts/openapi_compat.py ../../scripts/check-ts-wire-contracts.py`
  `git diff --check`
  Expected: every command exits `0`; final `git status --short` contains only intentional tracked changes.

- [ ] **Step 6: Commit Task 6**

  Commit message: `test(contracts): prevent handwritten wire DTO drift`

### Task 7: Whole-branch review and remediation

**Files:**
- Review: all changes since merge-base with `origin/main`
- Modify: only files required to resolve reviewer findings

**Interfaces:**
- Consumes: completed Tasks 1-6 and their verification evidence.
- Produces: review-clean branch satisfying the design specification.

- [ ] **Step 1: Generate a whole-branch review package**

  Use the subagent-driven-development review-package script with `MERGE_BASE=$(git merge-base origin/main HEAD)` and `HEAD`.

- [ ] **Step 2: Dispatch an independent final code reviewer**

  Ask for spec compliance, contract correctness, compatibility-check completeness, false-positive/false-negative risks, TypeScript soundness, CI behavior, and test quality.

- [ ] **Step 3: Fix all Critical and Important findings with focused tests**

  Use one fix agent for the complete final finding list, rerun covering tests, and re-review until no Critical or Important findings remain.

- [ ] **Step 4: Run fresh final verification**

  Repeat Task 6 Step 5 after all review fixes and record exact exit results.

- [ ] **Step 5: Commit review fixes if any**

  Commit message: `fix(contracts): address final contract boundary review`
