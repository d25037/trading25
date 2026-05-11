# Ranking Daily Table Sort Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Daily Ranking a single full-scope stock table whose sorting is complete inside the table, and move 250-day high/low style lists into a separate technical-events surface.

**Architecture:** Daily stock ranking should first reuse the existing `rankings.tradingValue` array as the full market-scoped snapshot table by requesting `limit=0`, matching the current Indices sector-stock table behavior. The table owns sort state for visible scalar columns. Period high/low remains available as a technical event list with its own filters because it selects an event population rather than sorting a shared population.

**Tech Stack:** FastAPI + Pydantic/OpenAPI in `apps/bt`, React 19 + TanStack Router search params + generated contracts in `apps/ts`.

---

## File Structure

- No backend changes for the first implementation.
  - Existing `/api/analytics/ranking?limit=0&includeValuation=true` already supports full-scope rows and is used by the Indices sector-stock table.
  - Add a backend table route only later if `limit=0` across five existing buckets proves too slow in browser QA.
- Modify: `apps/ts/packages/web/src/stores/screeningStore.ts`
  - Change Daily Ranking defaults to full scope: `limit: 0`.
- Modify: `apps/ts/packages/web/src/lib/routeSearch.ts`
  - Preserve old `rankingLimit` URLs but stop serializing the default `0`.
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx`
  - Remove `Results per ranking` from the main Daily Ranking filters.
  - Keep `Markets`, `Lookback Days`, and `Date`.
  - Move `Period Days` to technical-events filters only.
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingTable.tsx`
  - Remove the external `売買代金 / 値上がり / 値下がり / 期間高値 / 期間安値` selector for the main table.
  - Sort the full item set using column headers.
- Modify: `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
  - Ensure the visible daily columns that should be sortable have `SortHeader`.
- Modify: `apps/ts/packages/web/src/pages/RankingPage.tsx`
  - Add `Technical Events` as a separate Daily View, or add a separate panel below the Daily table if keeping only two views initially.
- Tests:
  - `apps/bt/tests/unit/server/services/test_ranking_service.py`
  - `apps/bt/tests/server/routes/test_analytics_complex.py` if route-level coverage exists.
  - `apps/ts/packages/web/src/components/Ranking/RankingTable.test.tsx`
  - `apps/ts/packages/web/src/pages/RankingPage.test.tsx`
  - `apps/ts/packages/web/src/lib/routeSearch.test.ts`
  - `apps/ts/packages/web/src/hooks/useRanking.test.tsx`

## Placement Decision

`売買代金`, `騰落率`, `PER`, `forward PER`, `PBR`, `時価総額`, `流動性Z`, and `ADV60/FF` are scalar properties of the same stock population. They belong as sortable columns in Daily Ranking.

`250日高値`, `250日安値`, `60日高値`, and `120日高値` are event-population selectors. They should not be another sort mode for the same table because they answer "which names triggered a range event?" rather than "sort this population by a metric." Place them under `Daily Ranking > Technical Events` with `Event Type` and `Period Days` filters. If the event later becomes an executable strategy condition, expose it through Screening as a strategy result, not as a generic ranking bucket.

## Task 1: Reuse Existing Full-Scope Dataset

**Files:**
- Read: `apps/bt/src/application/services/ranking_service.py`
- Read: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Read: `apps/ts/packages/web/src/pages/IndicesPage.tsx`
- Modify: `apps/ts/packages/web/src/stores/screeningStore.ts`
- Test: `apps/ts/packages/web/src/pages/RankingPage.test.tsx`

- [x] **Step 1: Confirm the existing contract**

Verify `_limit_clause(limit)` returns no SQL `LIMIT` when `limit <= 0`, and `SectorStocksList` already sends `limit: 0` with `includeValuation: true`.

- [x] **Step 2: Change Daily Ranking default limit**

Set `DEFAULT_RANKING_PARAMS.limit` to `0`. This makes the main Daily Ranking request full-scope by default, the same way Indices sector-stock tables already behave.

- [x] **Step 3: Keep compatibility**

Do not remove `rankingLimit` URL parsing. Explicit shared URLs with `rankingLimit=20` should keep working.

- [x] **Step 4: Run focused tests**

Run:

```bash
bun run --filter @trading25/web test -- RankingPage.test.tsx routeSearch.test.ts
```

Expected: all commands pass.

## Task 2: Main Daily Ranking Table

**Files:**
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingTable.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingTable.test.tsx`

- [x] **Step 1: Write failing UI tests**

Update `RankingTable.test.tsx` so it asserts:

- no `売買代金 / 値上がり / 値下がり / 期間高値 / 期間安値` selector is rendered for the main Daily Ranking table;
- clicking the `騰落率` header reorders the full provided item set;
- clicking `売買代金` reorders the same full provided item set back by trading value.

- [x] **Step 2: Remove external ranking-type selection**

Remove `activeRankingType`, `rankingTabs`, and the category `<Select>` from the main `RankingTable` path. The table should use `rankings.tradingValue` as its full population and own only column sort state.

- [x] **Step 3: Ensure sortable visible columns**

Add sortable headers for `現在値` if needed, and keep existing sortable headers for `コード`, `売買代金`, `騰落率`, valuation, and liquidity fields. Non-scalar display columns such as `状態` can stay non-sortable unless a clear ranking key exists.

- [x] **Step 4: Run web tests**

Run:

```bash
bun run --filter @trading25/web test -- RankingTable.test.tsx
bun run --filter @trading25/web typecheck
```

Expected: tests and typecheck pass.

## Task 4: Filters And URL State

**Files:**
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx`
- Modify: `apps/ts/packages/web/src/stores/screeningStore.ts`
- Modify: `apps/ts/packages/web/src/lib/routeSearch.ts`
- Test: `apps/ts/packages/web/src/lib/routeSearch.test.ts`
- Test: `apps/ts/packages/web/src/pages/RankingPage.test.tsx`

- [x] **Step 1: Change defaults**

Keep `DEFAULT_RANKING_PARAMS.limit` at `0` for full Daily Ranking scope. Keep old URL parsing for `rankingLimit` so shared URLs do not break.

- [x] **Step 2: Simplify stock filters**

Remove `Results per ranking` and `Period Days (High/Low)` from the normal stock Daily Ranking sidebar. Keep `Markets`, `Lookback Days`, and `Date`.

- [x] **Step 3: Run URL and page tests**

Run:

```bash
bun run --filter @trading25/web test -- routeSearch.test.ts RankingPage.test.tsx
```

Expected: defaults serialize cleanly and old explicit `rankingLimit` still parses.

## Task 5: Technical Events Surface

**Files:**
- Modify: `apps/ts/packages/web/src/types/ranking.ts`
- Modify: `apps/ts/packages/web/src/lib/routeSearch.ts`
- Modify: `apps/ts/packages/web/src/pages/RankingPage.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/RankingFilters.tsx` or create `apps/ts/packages/web/src/components/Ranking/TechnicalEventFilters.tsx`
- Test: `apps/ts/packages/web/src/pages/RankingPage.test.tsx`

- [x] **Step 1: Add a Daily View**

Add `technicalEvents` to `RankingDailyView` and a `Technical Events` item next to `Individual Stocks` and `Indices`.

- [x] **Step 2: Add event filters**

Expose `Event Type` with `New High` and `New Low`, and `Period Days` with `60`, `120`, and `250`. Keep `Markets` and `Date`.

- [x] **Step 3: Reuse existing period high/low data initially**

For the first version, use existing `rankings.periodHigh` / `rankings.periodLow` results in this view. Set a bounded default like `limit=50` for this event view because it is a top-event list, not a full population table.

- [x] **Step 4: Add tests**

Assert that `250日高値` is visible only in `Technical Events`, not as a selector on the main stock table.

## Task 6: Verification And Commit

**Files:** all touched files.

- [x] **Step 1: Run focused validation**

```bash
bun run --filter @trading25/contracts bt:check
bun run --filter @trading25/web test -- RankingTable.test.tsx RankingPage.test.tsx routeSearch.test.ts
(cd apps/ts && bun run quality:typecheck)
git diff --check
```

Expected: all commands pass.

- [x] **Step 2: Browser QA**

Open `http://localhost:5173/ranking?tab=ranking&dailyView=stocks` and verify:

- the main stock view has no external ranking-type selector;
- row count is full market scope rather than 20;
- `騰落率`, `売買代金`, valuation, and liquidity headers sort the full visible population;
- `250日高値` appears under `Technical Events`.

- [x] **Step 3: Commit**

```bash
git add apps/bt apps/ts docs/superpowers/plans/2026-05-11-ranking-daily-table-sort.md
git commit -m "feat: simplify daily ranking table sorting"
```

## Self-Review

- Spec coverage: The plan removes external stock ranking buckets, makes table sorting operate on full scope, keeps market/date controls in the sidebar, and relocates 250-day high/low to technical events.
- Placeholder scan: No open-ended placeholders remain; each task has concrete files and commands.
- Type consistency: `RankingDailyView`, `RankingParams`, table row response, and route search changes are called out together so URL, API, and UI state remain aligned.
