# Symbol Workbench Ranking Strip Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the oversized Symbol Workbench Daily Ranking card grid with the approved two-row micro strip while keeping all data and Daily Ranking presentation semantics visible.

**Architecture:** Keep `dailyRankingPresentation.tsx` as the display source of truth and change only the Workbench-specific composition in `DailyRankingSnapshot.tsx`. Represent supplemental stock metadata as one compact semantic definition list, group the existing ranking metric definitions into an intentional seven-column desktop order, and render Regime and Signals as normal strip cells instead of full-width cards.

**Tech Stack:** React 19, TypeScript, Tailwind CSS v4, Vitest, Testing Library

## Global Constraints

- Work on the existing local `main` checkout as explicitly requested; do not create another worktree.
- Preserve `DAILY_RANKING_VALUE_METRICS`, `DailyRankingMetricValue`, `DailyRankingRegimeChip`, and `DailyRankingSignalChips` as the presentation source of truth.
- Keep every existing basic field and ranking metric visible without expansion or horizontal scrolling.
- Normal desktop uses seven columns and two ranking rows; mobile uses two columns.
- Preserve loading, unavailable, error, retry, `aria-live`, and existing test IDs.
- Do not change API contracts, data fetching, or Daily Ranking table behavior.

---

### Task 1: Lock the compact layout contract with failing tests

**Files:**
- Modify: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx`

**Interfaces:**
- Consumes: `DailyRankingSnapshot` and its existing test IDs.
- Produces: assertions for the accessible unnamed section, compact metadata list, seven-column desktop strip, paired PSR cell, and inline Regime/Signals cells.

- [ ] **Step 1: Replace the obsolete heading and full-row assertions**

Update the populated-state test so the visible `Daily Ranking Snapshot` heading is absent while the section has `aria-label="Daily Ranking Snapshot"`. Update the responsive test to require these layout classes:

```tsx
expect(screen.getByTestId('daily-ranking-basic-info')).toHaveClass('flex', 'flex-wrap');
expect(screen.getByTestId('daily-ranking-metrics')).toHaveClass('grid-cols-2', 'lg:grid-cols-7');
expect(screen.getByTestId('daily-ranking-regime')).not.toHaveClass('col-span-full');
expect(screen.getByTestId('daily-ranking-signals')).not.toHaveClass('col-span-full');
```

- [ ] **Step 2: Add the paired PSR behavior assertion**

Require one stable layout cell that contains both shared metric labels and values:

```tsx
const psrPair = screen.getByTestId('daily-ranking-psr-pair');
expect(within(psrPair).getByText('PSR')).toBeInTheDocument();
expect(within(psrPair).getByText('Fwd PSR')).toBeInTheDocument();
expect(psrPair).toHaveTextContent('2.00x');
expect(psrPair).toHaveTextContent('1.50x');
```

- [ ] **Step 3: Run the focused test and verify the intended failure**

Run:

```bash
bun run --filter @trading25/web test -- DailyRankingSnapshot.test.tsx
```

Expected: FAIL because the current component still shows the heading, uses the old responsive classes, lacks `daily-ranking-psr-pair`, and makes Regime/Signals full-width.

### Task 2: Implement the two-row micro strip

**Files:**
- Modify: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx`
- Test: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx`

**Interfaces:**
- Consumes: the existing `DailyRankingMetric` definitions and rendering components from `dailyRankingPresentation.tsx`.
- Produces: Workbench-only layout grouping with stable metric keys and no duplicated presentation rules.

- [ ] **Step 1: Define Workbench metric order by shared metric key**

Add two key arrays and derive the shared definitions by lookup:

```tsx
const PRIMARY_METRIC_KEYS = [
  'currentPrice',
  'changePercentage',
  'per',
  'forwardPer',
  'forecastOperatingProfitGrowthRatio',
  'pbr',
  'valueCompositeScore',
] as const;

const SECONDARY_METRIC_KEYS = [
  'sectorStrengthScore',
  'liquidityResidualZ',
  'tradingValue',
  'sma5AboveCount5d',
] as const;
```

Resolve definitions from `DAILY_RANKING_VALUE_METRICS`; do not recreate their labels, formatters, or color resolvers.

- [ ] **Step 2: Replace card helpers with semantic strip cells**

Change the field and metric helpers to render `<div>` groups containing `<dt>` and `<dd>`. Metric cells use compact classes equivalent to:

```tsx
className="min-w-0 border-l border-border/70 px-2 py-0.5 first:border-l-0 first:pl-0"
```

Labels use `text-[9px] leading-3`; values use `text-xs leading-4 font-semibold tabular-nums`. Remove rounded backgrounds, card padding, shadows, and extra top margins.

- [ ] **Step 3: Render metadata as one compact inline list**

Keep the existing values for Market, Index Membership, Sector 17, Sector 33, Market Cap, Free-Float Market Cap, and the snapshot date. Render them as compact label/value pairs in `daily-ranking-basic-info` using:

```tsx
className="flex flex-wrap items-center gap-x-3 gap-y-0.5"
```

Use subtle separators between pairs, use the shared `As of` date as the seventh field, and remove the visible section heading/date row. Keep `aria-label="Daily Ranking Snapshot"` on the section.

- [ ] **Step 4: Render two intentional ranking rows**

Use one `daily-ranking-metrics` `<dl>` with `grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7`. Render the seven primary definitions first. Render the second row as Sector Strength, a paired PSR/Fwd PSR cell, Liquidity Z, Trading Value, SMA5 5D, Regime, and Signals.

The PSR pair must call `DailyRankingMetricValue` twice with the shared `psr` and `forwardPsr` definitions. Regime and Signals must retain their existing components and test IDs without `col-span-full`.

- [ ] **Step 5: Compact the non-success states and focus treatment**

Keep existing state copy and roles. Remove nested card framing from the error state, use compact vertical padding, and add:

```tsx
focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-500/60
```

to the Retry button.

- [ ] **Step 6: Run focused tests to verify green**

Run:

```bash
bun run --filter @trading25/web test -- DailyRankingSnapshot.test.tsx
```

Expected: 6 tests pass with no warnings.

### Task 3: Verify integration and rendered density

**Files:**
- Verify: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx`
- Verify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`

**Interfaces:**
- Consumes: the completed compact Snapshot component.
- Produces: test, type, and browser evidence that the change is safe and meets the approved visual target.

- [ ] **Step 1: Run the related page and presentation tests**

Run:

```bash
bun run --filter @trading25/web test -- DailyRankingSnapshot.test.tsx dailyRankingPresentation.test.tsx SymbolWorkbenchPage.test.tsx
```

Expected: all selected test files pass.

- [ ] **Step 2: Run type checking**

Run:

```bash
bun run --filter @trading25/web typecheck
```

Expected: exit code 0 with no TypeScript errors.

- [ ] **Step 3: Run the full Web test suite**

Run:

```bash
bun run --filter @trading25/web test
```

Expected: all Web tests pass.

- [ ] **Step 4: Verify the live page at desktop and mobile widths**

Open `http://localhost:5173/symbol-workbench?symbol=5711` in the built-in browser. At approximately 1180 × 800, verify the snapshot has no metric cards, all values are visible, there is no horizontal overflow, and `Daily Chart` appears in the first viewport. Repeat at a mobile width and verify the two-column wrap and Retry focus behavior if the error state can be exercised safely.

- [ ] **Step 5: Compare the implementation with the approved concept**

Capture the live desktop page and inspect it together with `.superpowers/brainstorm/37803-1783641509/content/ultra-compact-options.html`. Compare metadata inclusion, two-row ordering, typography, separators, absence of cards, semantic colors, and chart start position. Fix any implementation drift before completion.

- [ ] **Step 6: Review the final diff**

Run:

```bash
git diff --check
git diff -- apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx
```

Expected: no whitespace errors and only the approved compact-layout changes.
