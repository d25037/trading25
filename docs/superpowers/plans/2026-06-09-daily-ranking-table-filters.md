# Daily Ranking Table Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add shareable, URL-backed display filters to Daily Ranking so users can narrow the current individual-stock result table after sorting.

**Architecture:** Keep existing backend ranking filters as the API source of truth for market/date/preset/state narrowing. Add a separate frontend-only table filter state for text, categorical, and numeric display filters; filter the fetched all-stock result set before applying the existing client-side column sort. Persist active table filters in `/ranking` search params so reloads and shared URLs restore the same scan surface.

**Tech Stack:** React 19, TanStack Router search params, TypeScript, Vitest, existing shadcn-style `Button` / `Input` / `Select` / `Dialog`, lucide icons.

---

## Current State

- `apps/ts/packages/web/src/pages/RankingPage.tsx` already passes `sortBy` / `order` from route state into `RankingTable`.
- `apps/ts/packages/web/src/components/Ranking/RankingTable.tsx` sorts the fetched item array in memory when `enableColumnSort` is true.
- `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx` already exposes market/date/preset/advanced API filters.
- `apps/ts/packages/web/src/stores/screeningStore.ts` sets `DEFAULT_RANKING_PARAMS.limit = 0`, so the individual-stock Daily Ranking view fetches the full result set before client-side sort.
- `apps/bt/src/entrypoints/http/routes/analytics_complex.py` already supports backend filters for `sector33Name`, `sector17Name`, `forwardEpsDisclosedWithinDays`, `regimeState`, `riskState`, and `technicalState`, but not text search or numeric table ranges.

## Product Decision

Implement "table filters" for the Individual Stocks Daily Ranking view, not another backend ranking query contract yet.

Initial filter set:

- Text: code, company name, sector name.
- Categorical: market, sector33, regime, valuation signal, warning flag, technical flag.
- Numeric ranges: change percentage, trading value, market cap, forward PER, PBR, liquidity Z, sector strength score.

Out of scope for the first pass:

- Backend OpenAPI changes.
- Server-side sort.
- Filters on Technical Events and Indices views.
- New dependencies such as popover libraries.

## Files

- Modify: `apps/ts/packages/web/src/types/ranking.ts`
  - Add `DailyRankingTableFilters`, `DailyRankingValuationSignalFilter`, and numeric range keys.
- Modify: `apps/ts/packages/web/src/lib/routeSearch.ts`
  - Add search params for table filters and serialize only non-empty values.
- Modify: `apps/ts/packages/web/src/hooks/usePageRouteState.ts`
  - Return `rankingTableFilters` and `setRankingTableFilters` from `useRankingRouteState`.
- Create: `apps/ts/packages/web/src/components/Ranking/rankingTableFilters.ts`
  - Pure filter helpers and active-filter counting.
- Create: `apps/ts/packages/web/src/components/Ranking/RankingTableFilters.tsx`
  - Compact filter dialog or collapsible panel using existing UI primitives.
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingTable.tsx`
  - Apply filters before sort, show filtered count, expose `Clear` and filter trigger through header actions.
- Modify: `apps/ts/packages/web/src/pages/RankingPage.tsx`
  - Wire route-backed table filter state only for `activeDailyView === 'stocks'`.
- Modify tests:
  - `apps/ts/packages/web/src/components/Ranking/RankingTable.test.tsx`
  - `apps/ts/packages/web/src/components/Ranking/RankingFilters.test.tsx` only if labels/options move
  - `apps/ts/packages/web/src/lib/routeSearch.test.ts`
  - `apps/ts/packages/web/src/hooks/usePageRouteState.test.tsx`
  - `apps/ts/packages/web/src/pages/RankingPage.test.tsx`

## Tasks

### Task 1: Add Route-Backed Filter State

- [ ] Add `DailyRankingTableFilters` to `apps/ts/packages/web/src/types/ranking.ts`.
- [ ] Add route search keys with a `rankingFilter*` prefix, for example `rankingFilterText`, `rankingFilterMarket`, `rankingFilterSector33`, `rankingFilterSignal`, `rankingFilterMinChangePct`.
- [ ] Reuse `normalizeString` and `normalizeFiniteNumber` in `routeSearch.ts`; add enum normalizers for valuation signal filter values.
- [ ] Extend `getRankingStateFromSearch`, `serializeRankingSearch`, and `serializeRankingSearchForNavigation` to include `rankingTableFilters`.
- [ ] Extend `useRankingRouteState` to return `rankingTableFilters` and `setRankingTableFilters`.
- [ ] Add route tests covering parse, serialize, default omission, and invalid-value dropping.

### Task 2: Implement Pure Filtering Logic

- [ ] Create `rankingTableFilters.ts`.
- [ ] Implement `hasActiveDailyRankingTableFilters(filters)`.
- [ ] Implement `countActiveDailyRankingTableFilters(filters)`.
- [ ] Implement `filterDailyRankingItems(items, filters)` with inclusive numeric min/max checks.
- [ ] Match text case-insensitively across `code`, `companyName`, and `sector33Name`.
- [ ] Treat null numeric fields as non-matching only when a bound for that field is set.
- [ ] Use existing `getValuationSignal(item)`, `riskFlags`, `technicalFlags`, and `liquidityRegime` for signal/state filtering.
- [ ] Add focused tests for text, category, signal, numeric range, null handling, and multiple filters combined.

### Task 3: Add the Filter UI

- [ ] Create `RankingTableFilters.tsx`.
- [ ] Use a small `Button` with a lucide `SlidersHorizontal` icon in the table header.
- [ ] Use existing `Dialog` for the panel to avoid adding popover dependencies.
- [ ] Include controls for search, market, sector, regime, signal, warning, confirmation, and numeric ranges.
- [ ] Build market/sector option lists from the current fetched rows so the UI only offers values present in the current result set.
- [ ] Add a `Clear` action that resets only table filters, preserving API filters, date, preset, and sort.
- [ ] Keep labels compact and avoid explanatory body copy inside the app.

### Task 4: Wire Filtering Into RankingTable

- [ ] Add optional props to `RankingTable`: `filterState`, `onFilterChange`, `enableTableFilters`.
- [ ] Filter `currentItems` first, then sort `filteredItems`.
- [ ] Keep row numbers based on the displayed order, matching the current sorted-table behavior.
- [ ] Show count as `(filtered / total)` only when filters are active; otherwise keep the current `(count)` display.
- [ ] Preserve virtualization thresholds by passing the final displayed array into `EquityRankingTable`.
- [ ] Add tests proving filtering happens before sort and count display reflects active filters.

### Task 5: Wire Individual Stocks Page Only

- [ ] Update `RankingPage.tsx` to read and set `rankingTableFilters`.
- [ ] Pass table filters only to the Individual Stocks `RankingTable`.
- [ ] Do not pass table filters to Technical Events or Indices.
- [ ] When switching daily view, keep URL filters serialized but inactive outside Individual Stocks so returning to the tab restores them.
- [ ] Add page tests that table filters are passed in stocks view and omitted in other views.

### Task 6: Verification

- [ ] Run `bun run --filter @trading25/web test -- RankingTable RankingPage routeSearch usePageRouteState`.
- [ ] Run `bun run quality:typecheck`.
- [ ] If implementation touches shared exports or route parsing broadly, run `bun run workspace:test`.
- [ ] Start the local stack if not already running: backend `uv run bt server --port 3002`, frontend `bun run workspace:dev`.
- [ ] Browser QA on `localhost:5173/ranking`: verify Individual Stocks filters, sort interaction, URL reload restore, and tab switching.
- [ ] Historical QA dates should include the known Ranking regression surface: `2026-05-20`, `2026-05-27`, and `2026-03-18`; check `Crowded All`, `Crowded Good`, `Neutral Good`, and `Momentum Value`.

## Risks

- Filtering after a limited API response would hide valid matches. Current Individual Stocks default is `limit=0`, so keep that invariant in tests.
- Header controls must not increase table height enough to reduce the main scan area. Keep the trigger in `headerActions`; put the full form in a dialog.
- API state filters and table display filters can look redundant. Use names and state boundaries that make the API preset rail distinct from row-level table filters.
