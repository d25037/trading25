# Symbol Workbench Shikiho Panel Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved Company Shikiho panel layout, score visualization, and earnings announcement date extraction/display.

**Architecture:** Extend the local Shikiho snapshot with one nullable ISO date extracted from the visible Japanese-labeled DOM, then render it through a pure urgency helper and focused React components. Keep all changes local to the extension and Symbol Workbench; use inline SVG for the fixed six-axis radar and preserve existing panel behavior.

**Tech Stack:** TypeScript, React 19, Bun test, happy-dom, Tailwind CSS v4, inline SVG, Chrome Manifest V3 extension.

## Global Constraints

- Do not add backend, OpenAPI, database, or network-request changes.
- Extract from visible DOM using Japanese labels; do not store raw HTML.
- Preserve schema version 1 compatibility by normalizing absent `earningsAnnouncementDate` to `null`.
- Use the approved urgency thresholds: 15+ neutral, 8-14 yellow, 4-7 orange, 0-3 red, past gray.
- Do not show industry median data.

---

### Task 1: Snapshot contract and DOM extraction

**Files:**
- Modify: `apps/ts/extensions/shikiho/src/contract.ts`
- Modify: `apps/ts/extensions/shikiho/src/extractor.ts`
- Modify: `apps/ts/extensions/shikiho/src/progressive-capture.ts`
- Modify: `apps/ts/extensions/shikiho/src/fixtures/7203-current-authenticated.html`
- Test: `apps/ts/extensions/shikiho/src/contract.test.ts`
- Test: `apps/ts/extensions/shikiho/src/extractor.test.ts`
- Test: `apps/ts/extensions/shikiho/src/progressive-capture.test.ts`

**Interfaces:**
- Produces: `ShikihoSnapshotV1.earningsAnnouncementDate: string | null` containing canonical `YYYY-MM-DD` or `null`.
- Produces: progressive field name `earningsAnnouncementDate`.

- [ ] **Step 1: Write failing tests** for extracting `2026/07/31`, rejecting invalid dates, parsing an older object without the field as `null`, and retaining the field through progressive candidates.
- [ ] **Step 2: Run tests to verify RED** with `bun test src/contract.test.ts src/extractor.test.ts src/progressive-capture.test.ts` from `apps/ts/extensions/shikiho` and confirm failures are property/expectation mismatches.
- [ ] **Step 3: Implement the minimal contract/extractor path** by locating visible text anchored by `決算発表予定日`, validating a single `YYYY/MM/DD` value as a real calendar date, serializing `YYYY-MM-DD`, defaulting missing parsed properties to `null`, and adding the field to hash/progressive metadata.
- [ ] **Step 4: Run focused tests to verify GREEN** with the same Bun command.

### Task 2: Score card and date presentation

**Files:**
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.tsx`
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx`
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/shikihoEarningsDate.ts`
- Create: `apps/ts/packages/web/src/components/SymbolWorkbench/shikihoEarningsDate.test.ts`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Test: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`

**Interfaces:**
- Consumes: `ShikihoSnapshotV1['score']` and `earningsAnnouncementDate`.
- Produces: `getShikihoEarningsDateState(date: string, now?: Date)` returning `neutral | yellow | orange | red | past` plus remaining-day copy.
- Produces: `ShikihoScoreCard` with accessible overall score, fixed six-axis radar, and numeric values.

- [ ] **Step 1: Write failing helper/component tests** covering all date thresholds, 3/5 filled stars for overall 3, six metric labels/values, radar accessibility, and no-radar fallback when one axis is null.
- [ ] **Step 2: Run tests to verify RED** with `bun test src/components/SymbolWorkbench/shikihoEarningsDate.test.ts src/components/SymbolWorkbench/ShikihoScoreCard.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx` from `apps/ts/packages/web`.
- [ ] **Step 3: Implement minimal pure/helper UI** using JST calendar-day arithmetic, inline SVG polygon/grid geometry, semantic labels, and existing theme/Tailwind tokens without adding dependencies.
- [ ] **Step 4: Integrate into `ShikihoPanel`** before textual content and add the compact date badge without changing refresh/collapse/diagnostic behavior.
- [ ] **Step 5: Run focused tests to verify GREEN** with the same Bun command.

### Task 3: Header cleanup and no-wrap layout

**Files:**
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`
- Modify: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.tsx`
- Test: `apps/ts/packages/web/src/components/SymbolWorkbench/ShikihoPanel.test.tsx`

**Interfaces:**
- Preserves: panel-local `四季報で開く`, `会社四季報を更新`, and collapse controls.

- [ ] **Step 1: Write failing assertions** that the header-level exact-name `四季報` button is absent, panel source link remains, and the panel header exposes separate left/right no-wrap groups.
- [ ] **Step 2: Run tests to verify RED** with `bun test src/pages/SymbolWorkbenchPage.test.tsx src/components/SymbolWorkbench/ShikihoPanel.test.tsx` from `apps/ts/packages/web`.
- [ ] **Step 3: Remove the old button/import/helper/test mock** and convert the panel header to a two-zone `flex-nowrap`/grid structure with truncating low-priority metadata.
- [ ] **Step 4: Run focused tests to verify GREEN** with the same Bun command.

### Task 4: Verification and Chrome acceptance

**Files:**
- Modify if needed: `apps/ts/extensions/shikiho/README.md` only if field behavior needs operator-facing documentation.

**Interfaces:**
- Verifies the complete extension-to-Workbench data path for symbol `6737`.

- [ ] **Step 1: Run extension gates**: focused tests, `bun run typecheck`, and `bun run build` in `apps/ts/extensions/shikiho`.
- [ ] **Step 2: Run web gates**: focused tests, `bun run typecheck`, and `bun run build` in `apps/ts/packages/web`.
- [ ] **Step 3: Run workspace gates** from `apps/ts`: `bun run quality:lint`, `bun run quality:deps:audit`, and `git diff --check`.
- [ ] **Step 4: Reload the unpacked Chrome extension**, refresh `6737`, and verify the visible date `2026/07/31`, neutral badge, full-width score card, single-line header, and removal of the old button.
- [ ] **Step 5: Review the scoped diff** and report automated proof separately from Chrome acceptance.
