# Issues 411-414 Contract Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve GitHub issues #411, #412, #413, and #414 by moving direct TS OpenAPI schema references behind stable contract exports, separating research API contracts from UI models, and verifying Hono/dependency cleanup.

**Architecture:** `@trading25/contracts/types/api-response-types` owns stable names for bt signal/indicator/research API contracts. `apps/ts/packages/web` imports those stable names and keeps UI normalized models in web-local type files. Hono archive references remain historical; runtime manifests and dependency audit remain the current SoT.

**Tech Stack:** TypeScript, Bun, React hooks, OpenAPI generated types, Biome, TypeScript compiler.

---

### Task 1: Add Stable bt Signal/Indicator Contract Exports

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Test: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`

- [ ] **Step 1: Write the failing type-contract test**
  Add sample `IndicatorComputeRequest`, `MarginIndicatorRequest`, and `SignalComputeRequest` values imported from `api-response-types.ts`.

- [ ] **Step 2: Run test to verify it fails**
  Run: `bun run --filter @trading25/contracts test src/types/api-response-types.test.ts`
  Expected: FAIL because these stable exports do not exist yet.

- [ ] **Step 3: Add stable aliases**
  Export aliases for indicator, margin indicator, and signal request/response schemas from `api-response-types.ts`.

- [ ] **Step 4: Run test to verify it passes**
  Run: `bun run --filter @trading25/contracts test src/types/api-response-types.test.ts`
  Expected: PASS.

### Task 2: Move Web Hooks to Stable Contract Types

**Files:**
- Modify: `apps/ts/packages/web/src/hooks/useBtMarginIndicators.ts`
- Modify: `apps/ts/packages/web/src/hooks/useBtSignals.ts`
- Modify: `apps/ts/packages/web/src/hooks/useBtIndicators.ts`

- [ ] **Step 1: Replace generated schema imports**
  Import stable request/response/spec/result types from `@trading25/contracts/types/api-response-types`.

- [ ] **Step 2: Run focused typecheck**
  Run: `bun run --filter @trading25/web typecheck`
  Expected: PASS.

### Task 3: Split Research API Contracts from UI Models

**Files:**
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.test.ts`
- Modify: `apps/ts/packages/web/src/types/research.ts`
- Modify: `apps/ts/packages/web/src/hooks/useResearch.ts`
- Check: `apps/ts/packages/web/src/components/Research/*`

- [ ] **Step 1: Write the failing stable research type test**
  Add sample research catalog/detail contract values imported from `api-response-types.ts`.

- [ ] **Step 2: Run test to verify it fails**
  Run: `bun run --filter @trading25/contracts test src/types/api-response-types.test.ts`
  Expected: FAIL because research API contract exports do not exist yet.

- [ ] **Step 3: Add stable research aliases**
  Export API contract aliases from `api-response-types.ts`.

- [ ] **Step 4: Keep web-local normalized models explicit**
  Update `web/src/types/research.ts` so `Api*` names come from contracts while UI normalized interfaces stay local.

- [ ] **Step 5: Run focused typecheck**
  Run: `bun run --filter @trading25/web typecheck`
  Expected: PASS.

### Task 4: Verify Hono and Dependency Cleanup

**Files:**
- Inspect: `apps/ts/package.json`
- Inspect: `apps/ts/packages/*/package.json`
- Inspect: `apps/ts/scripts/dependency-audit.ts`
- Inspect: `scripts/skills/audit_skills.py`
- Preserve: `docs/archive/**`

- [ ] **Step 1: Confirm no runtime Hono dependency**
  Run: `rg -n '"hono"|@hono|hono' apps/ts/package.json apps/ts/packages/*/package.json apps/ts/bun.lock`
  Expected: no matches.

- [ ] **Step 2: Run dependency audit**
  Run: `bun run quality:deps:audit`
  Expected: PASS.

- [ ] **Step 3: Keep archive references**
  Do not delete `docs/archive/**` Hono migration history unless runtime/docs current SoT contradicts it.

### Task 5: Final Verification and Issue Closeout

**Files:**
- Check: all touched files

- [ ] **Step 1: Verify no direct generated schema imports remain in scoped hooks/research facade**
  Run: `rg -n "components\\['schemas'\\]|components\\[\\\"schemas\\\"\\]|clients/backtest/generated/bt-api-types" apps/ts/packages/web/src/hooks/useBtMarginIndicators.ts apps/ts/packages/web/src/hooks/useBtSignals.ts apps/ts/packages/web/src/hooks/useBtIndicators.ts apps/ts/packages/web/src/types/research.ts`
  Expected: no matches.

- [ ] **Step 2: Run full relevant TS checks**
  Run: `bun run --filter @trading25/contracts test src/types/api-response-types.test.ts && bun run --filter @trading25/web typecheck && bun run quality:deps:audit`
  Expected: PASS.

- [ ] **Step 3: Commit in small slices**
  Commit #411/#412 type-boundary changes separately from #413/#414 audit/doc cleanup if there are code changes for both slices.
