# Maintenance Foundations Design

## Goal

Re-establish enforceable maintenance boundaries after the recent repository growth without changing trading, research, synchronization, or user-facing behavior.

This first slice covers three foundations:

1. Codex skill and instruction governance;
2. the TypeScript Daily Ranking response contract;
3. a ratchet that prevents additional `application -> entrypoints.http.schemas` dependencies in `apps/bt`.

## Scope

### Included

- Treat `.codex/skills/**` and every `AGENTS.md` as governance inputs rather than docs-only changes.
- Run the repository skill audit for governance-only changes in GitHub Actions and `scripts/prepush-ci.sh`.
- Make repository skill verification commands executable from the repository root.
- Extend skill validation tests so verification commands cannot silently refer to the wrong project working directory.
- Make the OpenAPI-generated Daily Ranking schemas the canonical TypeScript response types.
- Remove the duplicate handwritten Daily Ranking response model from `api-clients` and re-export the canonical contract.
- Add compile-time compatibility coverage for the Ranking contract.
- Add an explicit, exact-set architecture ratchet for the currently tolerated `application -> entrypoints.http.schemas` imports.
- Document the ratchet as temporary debt with a monotonically decreasing budget.

### Excluded

- A repository-wide typed HTTP request wrapper.
- Migrating all existing application-layer HTTP schema imports in one change.
- Moving or rewriting invalidated research modules.
- Splitting large runtime or research files.
- Changing FastAPI response payloads or OpenAPI schemas.
- Changing synchronization, strategy, screening, ranking, or backtest behavior.

## Chosen Approach

Use a guardrail-first incremental slice.

The repository already has source OpenAPI export, generated TypeScript types, architecture tests, changed-file classification, and a skill audit. The safest first step is to close the gaps in those existing mechanisms rather than introduce new infrastructure or attempt a broad rewrite.

Rejected alternatives:

1. A full application DTO migration would touch roughly half of the application layer and combine many unrelated behavior risks.
2. A repository-wide typed HTTP client is desirable, but it is too broad for the first contract correction and would make review of the confirmed Ranking drift harder.
3. Documentation-only cleanup would leave the same regressions mechanically possible.

## Governance Design

### Change Classification

Introduce an explicit governance classification for:

- `.codex/skills/**`;
- root and nested `AGENTS.md` files;
- `scripts/skills/**`;
- tests that validate skills and instruction governance.

Governance changes must set `docs_only=false`. They do not need to force the complete product test suite unless they also touch product paths, but they must run `repo-guardrails`.

The same classification function remains the source for GitHub Actions and local pre-push behavior so the two paths cannot diverge.

### Skill Verification Commands

Repository-local skill commands must be runnable from the repository root because agent execution starts there unless a skill explicitly changes directories.

Python commands use one of these forms:

```bash
uv run --directory apps/bt pytest <apps/bt-relative-test-path>
uv run --directory apps/bt ruff check <apps/bt-relative-source-path>
uv run --directory apps/bt pyright <apps/bt-relative-source-path>
```

TypeScript commands use:

```bash
bun --cwd apps/ts run <script>
```

Root scripts continue to use repository-relative commands such as:

```bash
python3 scripts/skills/audit_skills.py --strict-legacy
```

The audit will parse verification-section command spans sufficiently to reject known-invalid project invocation shapes. It will not execute full test suites during the audit.

### Instruction Corrections

Only contradictions directly relevant to this slice are corrected:

- CI runs on pushes to `main`, pull requests, and manual dispatch, not every branch push.
- unavailable `~/.agents/skills` are not advertised as guaranteed capabilities.
- nested strategy-location guidance is aligned with runtime constants and the root policy.

Broader documentation drift remains a later maintenance slice.

## TypeScript Ranking Contract Design

### Canonical Types

The committed OpenAPI schema and generated `components['schemas']` types remain the backend contract source.

`@trading25/contracts/types/api-response-types` exports stable aliases for:

- `RankingItem`;
- `Rankings`;
- `IndexPerformanceItem`;
- `MarketRankingResponse`;
- `MarketRankingSymbolResponse`;
- related Ranking enum/flag schemas where an exact generated schema exists.

No handwritten interface may redefine fields from those backend schemas. Frontend-only request state and presentation models remain local.

### API Clients

`@trading25/api-clients/analytics` imports and re-exports canonical Ranking response types from `@trading25/contracts`. It may continue to define request parameter types that are not response payload contracts.

To preserve the dependency direction, `contracts` must not build against or depend on `api-clients`. The existing compatibility checker will be moved or rewritten so compatibility verification does not invert that dependency.

### Optional Ranking Collections

The generated API schema currently permits omitted Ranking collections. Consumers must not assume a collection exists merely because the old handwritten interface marked it required.

Ranking presentation reads use an empty-array fallback at the normalization or component boundary. This prevents `undefined[0]` failures while preserving the backend contract exactly.

### Contract Verification

Compile-time tests verify that public aliases resolve to the generated schema and that the API client re-exports those aliases. Runtime component tests cover omitted Ranking collections.

This slice does not change endpoint paths, methods, query parameters, or server responses, so OpenAPI regeneration should produce no diff.

## bt Architecture Ratchet Design

The existing transitional prefix remains temporarily permitted, but its current usage becomes an explicit debt budget.

The architecture test will:

1. enumerate imports from `src/application/**/*.py` to `src.entrypoints.http.schemas`;
2. compare the exact `(source file, imported module)` set with a checked-in baseline;
3. fail on any new dependency;
4. fail when a baseline entry becomes stale, forcing the budget to decrease as imports are migrated.

An exact baseline is preferred over a single numeric count because it prevents replacing one removed dependency with a different new dependency while keeping the count unchanged.

The layering guide will describe the exception as migration debt, identify the baseline test, and require later DTO migrations to remove baseline entries in the same commit.

No production import is moved in this slice, so application behavior remains unchanged.

## Error and Failure Handling

- Governance classification failures report the changed path and selected scope.
- Skill audit failures report the skill file and invalid command shape.
- Contract compile failures identify the public alias that drifted from OpenAPI.
- Ranking UI treats an omitted collection as empty, not as a transport failure.
- Architecture ratchet failures show added and stale dependency entries separately.

## Testing Strategy

All behavior-bearing changes follow red-green-refactor.

### Governance

- classification test proving skill-only changes are not docs-only;
- classification test proving AGENTS-only changes are not docs-only;
- pre-push scope test where applicable;
- skill-audit test rejecting invalid root-relative `uv run --project` and unscoped Bun verification commands;
- strict audit against every repository skill.

### TypeScript

- failing compile-time contract assertion for the existing Ranking drift;
- contracts unit tests for canonical aliases;
- api-clients typecheck and analytics tests;
- Ranking component test with omitted collections;
- web typecheck and focused Ranking tests;
- full OpenAPI contract-sync check.

### bt Architecture

- failing test fixture demonstrating a dependency not present in the baseline;
- passing current-tree baseline test;
- existing architecture suite;
- Ruff and Pyright for the test/helper files changed.

## Acceptance Criteria

1. A change limited to `.codex/skills/**` or any `AGENTS.md` runs repository guardrails in CI and pre-push classification.
2. Every repository skill verification command uses an execution form valid from the repository root, and the audit rejects recurrence of the known-invalid forms.
3. Daily Ranking response types have one generated canonical definition exposed through contracts and re-exported by api-clients.
4. `@trading25/contracts` no longer depends on `@trading25/api-clients`.
5. Ranking UI safely renders a generated-contract response with omitted Ranking collections.
6. No new `application -> entrypoints.http.schemas` import can be added without an architecture-test failure.
7. Existing product behavior and the FastAPI/OpenAPI payload remain unchanged.
8. Focused tests, typechecks, skill audit, architecture tests, and contract sync all pass with a clean worktree except for the intended maintenance changes.
