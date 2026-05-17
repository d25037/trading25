# Fundamental Analysis Forecast Operating Profit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show forecast operating profit in Symbol Workbench `Fundamental Analysis` with the same actual + forecast + change-rate treatment as EPS.

**Architecture:** Backend already exposes `forecastOperatingProfit` and `forecastOperatingProfitChangeRate` on `ApiFundamentalDataPoint`. Keep financial calculation in `apps/bt`; the web change only adds a configurable summary-card metric and renders backend values. The display should use the existing `ForecastMetricCard` pattern, formatted in millions/trillions like actual operating profit and cash-flow values.

**Tech Stack:** FastAPI/Pydantic/OpenAPI contracts, React 19, TypeScript, Vitest, Bun.

---

### Task 1: Add Forecast Operating Profit To Fundamental Metric Settings

**Files:**
- Modify: `apps/ts/packages/web/src/constants/fundamentalMetrics.ts`
- Test: `apps/ts/packages/web/src/constants/fundamentalMetrics.test.ts`

- [ ] **Step 1: Write the failing constants test**

Add this test if the file does not already have an equivalent metric coverage test:

```ts
it('includes forecast operating profit as a default visible metric', () => {
  expect(FUNDAMENTAL_METRIC_IDS).toContain('forecastOperatingProfit');
  expect(FUNDAMENTAL_METRIC_DEFINITIONS).toContainEqual({
    id: 'forecastOperatingProfit',
    label: '予想営業利益',
  });
  expect(DEFAULT_FUNDAMENTAL_METRIC_ORDER).toContain('forecastOperatingProfit');
  expect(DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY.forecastOperatingProfit).toBe(true);
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
bun run --filter @trading25/web test -- fundamentalMetrics.test.ts
```

Expected: fail because `forecastOperatingProfit` is not a `FundamentalMetricId`.

- [ ] **Step 3: Add the metric id, label, order, and default visibility**

Update `apps/ts/packages/web/src/constants/fundamentalMetrics.ts`:

```ts
export const FUNDAMENTAL_METRIC_IDS = [
  'per',
  'pbr',
  'roe',
  'roa',
  'eps',
  'forecastOperatingProfit',
  'bps',
  'dividendPerShare',
  'payoutRatio',
  'operatingMargin',
  'netMargin',
  'cashFlowOperating',
  'cashFlowInvesting',
  'cashFlowFinancing',
  'cashAndEquivalents',
  'fcf',
  'fcfYield',
  'fcfMargin',
  'cfoYield',
  'cfoMargin',
  'cfoToNetProfitRatio',
  'tradingValueToMarketCapRatio',
] as const;
```

Add the definition:

```ts
{ id: 'forecastOperatingProfit', label: '予想営業利益' },
```

Add default visibility:

```ts
forecastOperatingProfit: true,
```

Place it immediately after `eps` so the card groups EPS and operating-profit forecast context near the top.

- [ ] **Step 4: Run the constants test**

Run:

```bash
bun run --filter @trading25/web test -- fundamentalMetrics.test.ts
```

Expected: pass.

---

### Task 2: Merge Latest Forecast Operating Profit Into The Summary Metrics

**Files:**
- Modify: `apps/ts/packages/web/src/components/Chart/FundamentalsPanel.tsx`
- Test: `apps/ts/packages/web/src/components/Chart/FundamentalsPanel.test.tsx`

- [ ] **Step 1: Write a failing merge test**

Extend an existing `FundamentalsPanel` test around `latestMetrics` merge, or add this assertion to the fixture that already checks forecast EPS merge:

```ts
expect(metrics?.forecastOperatingProfit).toBe(6_200_000);
expect(metrics?.forecastOperatingProfitChangeRate).toBe(24);
```

Use `latestMetrics` with:

```ts
forecastOperatingProfit: 6_200_000,
forecastOperatingProfitChangeRate: 24,
```

and FY row with a different or null value so the test proves the latest metrics payload wins.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
bun run --filter @trading25/web test -- FundamentalsPanel.test.tsx
```

Expected: fail because `mergeLatestMetrics` does not copy the new forecast operating-profit fields.

- [ ] **Step 3: Merge backend-provided forecast operating-profit fields**

Update `mergeLatestMetrics` in `apps/ts/packages/web/src/components/Chart/FundamentalsPanel.tsx`:

```ts
forecastOperatingProfit: latestMetrics?.forecastOperatingProfit ?? fyData.forecastOperatingProfit ?? null,
forecastOperatingProfitChangeRate:
  latestMetrics?.forecastOperatingProfitChangeRate ?? fyData.forecastOperatingProfitChangeRate ?? null,
```

Place this near the existing `forecastEpsChangeRate` merge so forecast fields stay together.

- [ ] **Step 4: Do not add frontend calculation fallback**

Keep `applyForecastChangeRates` limited to EPS, dividends, and payout ratio for now. `forecastOperatingProfitChangeRate` should come from backend, because backend owns consolidated/FY-vs-Q forecast selection and actual operating-profit basis.

- [ ] **Step 5: Run the test**

Run:

```bash
bun run --filter @trading25/web test -- FundamentalsPanel.test.tsx
```

Expected: pass.

---

### Task 3: Render Forecast Operating Profit In Fundamental Analysis

**Files:**
- Modify: `apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.tsx`
- Test: `apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.test.tsx`

- [ ] **Step 1: Write the failing summary-card test**

Add fields to `baseMetrics`:

```ts
forecastOperatingProfit: 6_200_000,
forecastOperatingProfitChangeRate: 24,
```

Add a test:

```ts
it('renders forecast operating profit like EPS', () => {
  render(<FundamentalsSummaryCard metrics={baseMetrics} />);

  expect(screen.getByText('予想営業利益')).toBeInTheDocument();
  expect(screen.getByText('予: 6.2兆')).toBeInTheDocument();
  expect(screen.getByText('(+24.0%)')).toBeInTheDocument();
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
bun run --filter @trading25/web test -- FundamentalsSummaryCard.test.tsx
```

Expected: fail because no metric card exists for `forecastOperatingProfit`.

- [ ] **Step 3: Allow forecast metric cards to format large monetary values**

Update `ForecastMetricCardProps` in `apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.tsx`:

```ts
format: 'percent' | 'yen' | 'millions';
```

No other rendering changes are needed because `formatFundamentalValue` already supports `millions`.

- [ ] **Step 4: Add the card**

Add this entry to `buildMetricCards` immediately after `eps`:

```tsx
forecastOperatingProfit: (
  <ForecastMetricCard
    label="予想営業利益"
    actualValue={metrics.operatingProfit ?? null}
    forecastValue={metrics.forecastOperatingProfit ?? null}
    changeRate={metrics.forecastOperatingProfitChangeRate}
    format="millions"
  />
),
```

This makes the card read as actual operating profit with `予:` forecast below it, matching EPS semantics.

- [ ] **Step 5: Run the summary-card test**

Run:

```bash
bun run --filter @trading25/web test -- FundamentalsSummaryCard.test.tsx
```

Expected: pass.

---

### Task 4: Contract And Type Consistency Check

**Files:**
- Verify: `apps/ts/packages/contracts/src/types/api-types.ts`
- Verify: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Verify: `apps/ts/packages/api-clients/src/backtest/types.ts`

- [ ] **Step 1: Confirm the API fields exist in TS types**

Search:

```bash
rg "forecastOperatingProfit|forecastOperatingProfitChangeRate" apps/ts/packages/contracts/src apps/ts/packages/api-clients/src
```

Expected: both fields exist in manual and generated API types.

- [ ] **Step 2: Re-run contract generation check**

Run:

```bash
bun run --filter @trading25/contracts bt:check
```

Expected: pass with no generated diff.

---

### Task 5: Focused Verification And Browser Check

**Files:**
- Test: `apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.test.tsx`
- Test: `apps/ts/packages/web/src/components/Chart/FundamentalsPanel.test.tsx`
- Test: `apps/ts/packages/web/src/constants/fundamentalMetrics.test.ts`

- [ ] **Step 1: Run focused web tests**

Run:

```bash
bun run --filter @trading25/web test -- FundamentalsSummaryCard.test.tsx FundamentalsPanel.test.tsx fundamentalMetrics.test.ts
```

Expected: pass.

- [ ] **Step 2: Run web typecheck**

Run:

```bash
bun run --filter @trading25/web typecheck
```

Expected: pass.

- [ ] **Step 3: Run diff hygiene check**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 4: Verify in local browser**

With Vite running on `:5173`, open:

```text
http://localhost:5173/symbol-workbench?symbol=7203
```

Expected:
- `Fundamental Analysis` shows a `予想営業利益` card.
- The card shows actual operating profit as the main value.
- The forecast row uses `予: ...` and shows the backend change rate when present.
- The existing `業績履歴` `予想営業利益` column still renders.

---

### Task 6: Commit Scope

**Files:**
- Stage only the files changed for Fundamental Analysis display unless deliberately combining with the already-uncommitted DB/history work.

- [ ] **Step 1: Inspect status**

Run:

```bash
git status --short
```

- [ ] **Step 2: If committing this as a separate unit, stage only this feature**

Run:

```bash
git add apps/ts/packages/web/src/constants/fundamentalMetrics.ts \
  apps/ts/packages/web/src/constants/fundamentalMetrics.test.ts \
  apps/ts/packages/web/src/components/Chart/FundamentalsPanel.tsx \
  apps/ts/packages/web/src/components/Chart/FundamentalsPanel.test.tsx \
  apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.tsx \
  apps/ts/packages/web/src/components/Chart/FundamentalsSummaryCard.test.tsx
```

- [ ] **Step 3: Commit**

Run:

```bash
git commit -m "feat(web): show forecast operating profit in fundamentals summary"
```

Expected: commit succeeds after verification.

