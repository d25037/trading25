# Monorepo Migration Plan

## Goals
- Solidify the monorepo structure and daily workflow.
- Establish a stable contract boundary for datasets.
- Clarify dependency direction between `apps/ts` and `apps/bt`.
- (Optional) promote shared packages to the root `packages/`.

## Phase 1: Monorepo Ops Baseline
**Objective**: Enable consistent local + CI workflows from the repo root.

Tasks:
- [x] Add root CI workflow to run both `apps/ts` and `apps/bt` tests.
- [x] Add root scripts for tests / lint / typecheck.
- [x] Update root README with monorepo scripts and CI notes.

## Phase 2: Contracts in Practice
**Objective**: Make dataset changes safe and explicit across apps.

Tasks:
- [ ] Expand `contracts/dataset-schema.json` to reflect real dataset outputs.
- [ ] Implement dataset snapshot output in `apps/ts` (Parquet/Arrow or SQLite).
- [ ] Add snapshot reader in `apps/bt` with schema version validation.
- [ ] Document contract change rules (additive vs breaking) and versioning.

## Phase 3: Dependency Direction Cleanup
**Objective**: Avoid circular dependencies and clarify system boundaries.

Tasks:
- [ ] Audit and document `apps/ts -> apps/bt` and `apps/bt -> apps/ts` calls.
- [ ] Enforce the chosen direction (Pattern A) in code and docs.
- [ ] Add simple checks/notes to prevent regressions.

## Phase 4 (Optional): Promote Shared Packages
**Objective**: Move shared code to root `packages/` cleanly.

Tasks:
- [ ] Promote one shared package from `apps/ts/packages/*` to `packages/*`.
- [ ] Adjust build/test paths and imports.
- [ ] Update docs and scripts accordingly.

---

## Phase 1 Completion Log
- Root CI workflow added to `.github/workflows/ci.yml`.
- Root scripts added under `scripts/`.
- Root `README.md` updated with monorepo scripts and CI notes.
