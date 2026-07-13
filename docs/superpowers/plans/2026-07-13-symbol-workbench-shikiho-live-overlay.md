# Symbol Workbench Shikiho Live Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Workbench-only, 15-minute-delayed Shikiho provisional daily overlay, complete live score extraction, relocate the extension, and preserve the compact local-main UI.

**Architecture:** Keep J-Quants and React Query caches immutable. The extension exports a strictly validated optional quote; a pure Workbench composer appends one provisional daily bar and derives price-dependent presentation values only at the Symbol Workbench page boundary. The article and quote use separate freshness TTLs.

**Tech Stack:** TypeScript, Bun, React 19, TanStack Query, Vitest, Testing Library, Biome, Manifest V3

## Global Constraints

- Overlay scope is `/symbol-workbench` only; Ranking, Screening, Backtest, research, APIs, DuckDB, Parquet, and datasets are unchanged.
- Article TTL is 24 hours; quote TTL is 15 minutes; no timer polling, scheduled capture, or multi-symbol crawl.
- The provisional quote never overwrites or persists an official J-Quants row.
- Statement-derived fundamentals remain unchanged; only price-dependent presentation values may be derived.
- The source label is exactly `四季報 15分遅延・当日暫定` and the extension source is `会社四季報オンライン`.
- Preserve all existing privacy boundaries: rendered visible DOM only, no Shikiho fetch/XHR, cookies, credentials, raw HTML, telemetry, or backend storage.
- Keep package name `@trading25/shikiho-extension`; final unpacked path is `apps/ts/extensions/shikiho/dist`.
- Preserve the compact Daily Ranking UI from local `main`.

---

### Task 1: Preserve local-main compact UI and integrate it into the feature branch

**Files:**
- Main modify/commit: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx`
- Main modify/commit: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx`
- Main modify/commit: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Feature-branch conflict resolution: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`

**Interfaces:**
- Consumes: existing compact UI working-tree diff on local `main` and completed Shikiho branch.
- Produces: one main commit plus one merge commit with both behavior sets.

- [ ] **Step 1: Verify the exact main diff and focused tests**

From `/Users/shinjiroaso/dev/trading25`, confirm exactly the three listed files are dirty, run `git diff --check`, then from `apps/ts` run:

```bash
bun run --filter @trading25/web test -- \
  src/components/Ranking/DailyRankingSnapshot.test.tsx \
  src/pages/SymbolWorkbenchPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome lint \
  packages/web/src/components/Ranking/DailyRankingSnapshot.tsx \
  packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx \
  packages/web/src/pages/SymbolWorkbenchPage.test.tsx
```

Expected: focused tests and typecheck pass; scoped lint has no error.

- [ ] **Step 2: Commit only the compact UI files on main**

Stage exactly the three files, run `git diff --cached --check`, and commit:

```bash
git commit -m "feat(web): compact workbench ranking snapshot"
```

- [ ] **Step 3: Merge main into the feature worktree**

From `.worktrees/shikiho-workbench-bridge`, run `git merge --no-ff main`. If `SymbolWorkbenchPage.test.tsx` conflicts, retain main's compact labels/assertions and branch Shikiho hook mock/reset/default/refresh coverage. Use the exact `getByRole('button', { name: '四季報' })` selector.

- [ ] **Step 4: Verify the merge**

Run focused Daily Ranking, Shikiho hook/panel, and SymbolWorkbenchPage tests plus web typecheck. Confirm `git diff main -- DailyRankingSnapshot.tsx DailyRankingSnapshot.test.tsx` is empty.

### Task 2: Relocate the Shikiho extension package

**Files:**
- Move: `apps/ts/packages/shikiho-extension/**` -> `apps/ts/extensions/shikiho/**`
- Modify: `apps/ts/package.json`
- Modify: `apps/ts/bun.lock`
- Modify: `apps/ts/biome.json`
- Modify: `apps/ts/packages/web/tsconfig.json`
- Modify: `apps/ts/scripts/dependency-audit.ts`
- Modify if path assertions require it: `apps/ts/scripts/dependency-audit.test.ts`
- Modify if path assertions require it: `apps/ts/scripts/dependency-audit.coverage.test.ts`
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `apps/ts/extensions/shikiho/README.md`

**Interfaces:**
- Produces the unchanged package `@trading25/shikiho-extension` at `apps/ts/extensions/shikiho`.

- [ ] **Step 1: Write path-level failing checks**

Update dependency-audit/path assertions and documentation-path checks, if present, to require `extensions/shikiho`. Run them before moving the package and confirm failure.

- [ ] **Step 2: Move the intact package and update workspace configuration**

Use filesystem moves without editing generated `dist` directly. Change workspace, TS alias, Biome includes, dependency-audit globs, and operational documentation. Keep filter scripts and package name unchanged.

- [ ] **Step 3: Regenerate workspace lock data**

Run `bun install` from `apps/ts` and confirm `bun.lock` resolves `workspace:extensions/shikiho` with no old workspace locator.

- [ ] **Step 4: Verify and commit relocation**

Run package tests, typecheck, build, dependency audit, scoped lint, and assert `extensions/shikiho/dist/manifest.json` exists. Commit `refactor(extension): move Shikiho bridge package`.

### Task 3: Parse the observed live Shikiho score structure

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/extractor.ts`
- Modify: `apps/ts/extensions/shikiho/src/extractor.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/capture-controller.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/fixtures/7203-current-authenticated.html`

**Interfaces:**
- Produces all seven existing `ShikihoSnapshotV1['score']` values without changing optional-score status semantics.

- [ ] **Step 1: Add RED tests for the observed DOM**

Use fictional values in a minimal structure where the overall integer is a sibling of the exact `四季報スコア` title and six detail labels are visible `dt` elements with adjacent `dd` values. Test hidden/missing/malformed/out-of-range values and late score insertion after an initial core capture.

- [ ] **Step 2: Run focused tests and confirm RED**

Run extractor and capture-controller tests. Expected: overall is null and/or the later score mutation does not yield the complete score snapshot.

- [ ] **Step 3: Implement narrow score extraction**

Scope to the visible score region, parse the overall visible sibling and six exact semantic labels, accept integers 0 through 5 only, and retain missing score as optional.

- [ ] **Step 4: Run GREEN and commit**

Run focused tests and extension typecheck. Commit `fix(extension): parse live Shikiho score`.

### Task 4: Add the strict Shikiho quote contract and DOM extraction

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/contract.ts`
- Modify: `apps/ts/extensions/shikiho/src/contract.test.ts`
- Modify: `apps/ts/extensions/shikiho/src/extractor.ts`
- Modify: `apps/ts/extensions/shikiho/src/extractor.test.ts`
- Create: `apps/ts/extensions/shikiho/src/fixtures/7203-current-quote.html`
- Update typed fixtures in extension/web tests that construct `ShikihoSnapshotV1`.

**Interfaces:**
- Produces exported `ShikihoQuoteV1` and optional `quote?: ShikihoQuoteV1` on `ShikihoSnapshotV1`.

- [ ] **Step 1: Add RED contract tests**

Test exact keys and types for `tradingDate`, `observedAt`, literal `delayMinutes: 15`, positive finite current/open/high/low/previous-close, nullable nonnegative volume, nullable times, exact source label, and OHLC invariants. Test quote absence remains valid.

- [ ] **Step 2: Implement strict runtime validation**

Add the interface and validator inside `parseShikihoSnapshot`. Do not add code to the quote because snapshot URL/path already binds the symbol.

- [ ] **Step 3: Add RED extractor tests from a fictional visible quote fixture**

Anchor current price, quote update date/time, `始値`, `高値`, `安値`, `前日終値`, and `出来高` to their observed visible regions. Test hidden, malformed, zero, inconsistent, and missing quote cases.

- [ ] **Step 4: Implement quote extraction**

Use exact Japanese labels and section boundaries, not generated classes. Include quote in `snapshotWithoutCaptureTime` so hash changes when the quote changes. Quote failure must leave article capture intact.

- [ ] **Step 5: Run GREEN and commit**

Run contract/extractor/full extension tests and typecheck. Commit `feat(extension): capture delayed Shikiho quote`.

### Task 5: Separate 15-minute quote freshness from 24-hour article freshness

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/background-capture.ts`
- Modify: `apps/ts/extensions/shikiho/src/background-capture.test.ts`
- Modify as needed: `apps/ts/extensions/shikiho/src/storage.test.ts`
- Modify as needed: `apps/ts/extensions/shikiho/src/localhost-content.test.ts`

**Interfaces:**
- Produces `SHIKIHO_QUOTE_TTL_MS = 15 * 60 * 1000` resolution semantics while preserving the existing background coordinator API.

- [ ] **Step 1: Add boundary RED tests**

Cover a successful capture at 14:59.999, refresh at exactly 15:00, an already-15-minute-delayed source timestamp, missing quote, different JST date, future timestamp, explicit refresh, recent diagnostic suppression, same-code singleflight, FIFO, and owned-tab closure.

- [ ] **Step 2: Implement freshness separation**

Reuse a stored article only when article TTL and quote TTL are both valid. When only the quote is stale/missing, resolve the selected code once; do not add timers or polling. Preserve the prior article if quote refresh fails.

- [ ] **Step 3: Run GREEN and commit**

Run background/storage/localhost tests, full extension tests, typecheck, build, and privacy grep. Commit `feat(extension): refresh delayed quote safely`.

### Task 6: Compose a pure Workbench-only provisional daily overlay

**Files:**
- Create: `apps/ts/packages/web/src/lib/shikihoDailyOverlay.ts`
- Create: `apps/ts/packages/web/src/lib/shikihoDailyOverlay.test.ts`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Modify: `apps/ts/packages/web/src/hooks/useMultiTimeframeChart.ts`
- Modify: corresponding `useMultiTimeframeChart` test file.

**Interfaces:**
- Produces a pure result containing augmented daily OHLC, overlaid Daily Ranking item fields, price-derived latest metrics, price-derived market caps, and provenance; inputs remain immutable.

- [ ] **Step 1: Add RED pure-function tests**

Cover append current-JST-date bar, ignore older/different-date/symbol-mismatched quote, keep official same-date row, validate OHLC, preserve input immutability, calculate day change, current SMA5 relation/count/streak, and handle insufficient history. Disable overlay for relative mode because there is no same-session TOPIX quote.

- [ ] **Step 2: Add price-derived valuation tests**

Recalculate PER/forward PER/PBR/PSR/forward PSR and market caps from stable denominators when present; otherwise scale an existing price-linear value by `newPrice / officialPrice`. Preserve nulls and do not recalculate value score, percentiles, sector metrics, trading value, or statement-derived values.

- [ ] **Step 3: Implement the pure composer**

Append exactly one provisional daily bar in memory and return explicit provenance. Never mutate shared API responses or query caches.

- [ ] **Step 4: Thread the daily chart overlay through the Workbench chart hook**

Accept the optional page-local overlay in `useMultiTimeframeChart` and replace/append only the daily candlestick and local SMA5 point. Weekly/monthly and relative-mode series remain official and must not be labelled provisional.

- [ ] **Step 5: Apply page-local ranking/cap derivations**

In `SymbolWorkbenchPage`, compose after both selected-symbol data and Shikiho snapshot are available, then pass derived values to `ChartHeader` and panel content. Add page tests proving other hooks/pages are not mutated.

- [ ] **Step 6: Run GREEN and commit**

Run pure util, chart hook, and page tests plus web typecheck. Commit `feat(web): overlay delayed Shikiho daily quote`.

### Task 7: Apply consistent price-derived fundamentals and provenance UI

**Files:**
- Modify: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/SymbolWorkbenchPanels.tsx`
- Modify as required: `apps/ts/packages/web/src/components/Fundamentals/FundamentalsPanel.tsx`
- Modify as required: related Fundamentals/SymbolWorkbench panel tests.

**Interfaces:**
- Consumes Task 6 derived values and provenance.
- Produces consistent Workbench header/panel valuation values and accessible provisional labels.

- [ ] **Step 1: Add RED presentation tests**

Assert exact `四季報 15分遅延・当日暫定`, visible quote time, distinct provisional daily chart state, compact OHLC/previous-close/volume in the Shikiho panel, and no provisional label on fallback. Assert Fundamentals PER/PBR cards use the same derived price without mutating EPS/BPS/revenue/profit.

- [ ] **Step 2: Thread page-local latest-metrics overrides**

Pass optional derived metrics through Workbench panel composition into the fundamentals summary. Do not modify the cached `ApiFundamentalsResponse` or shared fundamentals hook.

- [ ] **Step 3: Render compact provenance and quote details**

Keep the compact local-main layout; add only a small source/time label and compact quote section. Ensure screen-reader text identifies provisional values.

- [ ] **Step 4: Run GREEN and commit**

Run Daily Ranking, Shikiho panel, Fundamentals, SymbolWorkbench panels, page tests, and web typecheck. Commit `feat(web): label provisional Shikiho metrics`.

### Task 8: Full verification and live integration

**Files:**
- Modify only if a scoped verification defect is found.

- [ ] **Step 1: Run automated gates**

From `apps/ts`, run extension tests/typecheck/build, focused web tests, full workspace tests, typecheck, dependency audit, lint, and workspace build. Run `git diff --check` and confirm a clean worktree.

- [ ] **Step 2: Review the whole branch**

Generate a merge-base-to-HEAD review package and dispatch final code review. Fix all Critical/Important findings and re-run covering tests.

- [ ] **Step 3: Reload the moved extension and perform live acceptance**

Load `apps/ts/extensions/shikiho/dist`, verify all seven score values and quote fields for `7203`, provisional daily chart/SMA5/valuation consistency, explicit source/time, background tab closure, 15-minute/force-refresh behavior, and official fallback.

- [ ] **Step 4: Complete main integration**

After live acceptance, merge the feature branch into local `main`, resolve no unrelated changes, re-run the final gate on `main`, and report final commit/branch state. Do not push unless separately requested.
