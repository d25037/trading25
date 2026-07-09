# Symbol Workbench Daily Ranking Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Symbol Workbench basic-information grid and Prime Liquidity strip with an integrated, latest-date Daily Ranking snapshot whose values and presentation semantics come from the Daily Ranking source of truth.

**Architecture:** FastAPI exposes one symbol-scoped wrapper around the existing Daily Ranking service. The TypeScript client and TanStack Query hook fetch that response without preserving previous-symbol data. A shared ranking presentation module owns labels, formatting, evidence classes, and badges for both the existing table and the new compact Workbench snapshot.

**Tech Stack:** Python 3.12, FastAPI, Pydantic, DuckDB, pytest, React 19, TypeScript, TanStack Query, Vitest, Bun, Tailwind CSS v4, OpenAPI-generated contracts.

## Global Constraints

- FastAPI on port 3002 is the only backend; do not add a TypeScript backend.
- `GET /api/analytics/ranking/symbol/{code}` returns required-but-nullable `date` and `item`, plus `lastUpdated`.
- Ranking values must be produced by `RankingService.get_rankings`; do not duplicate valuation, liquidity, sector-strength, technical-flag, or value-score calculations.
- Resolve the symbol's listed market first, then run the existing Daily Ranking with that market's canonical scope, `limit=0`, `lookback_days=1`, `period_days=250`, `include_valuation=True`, `include_sector_strength=True`, and `balanced_sector_strength`.
- Normalize compatible J-Quants suffix-zero codes (`72030` to `7203`, `285A0` to `285A`) while preserving the existing `RankingItem.code` representation returned by Daily Ranking.
- The snapshot always uses the latest market Daily Ranking date, never Symbol Workbench `Matched Date` or chart timeframe.
- Keep Index Membership and Free-Float Market Capitalization in the header from existing supplemental data; never approximate missing Daily Ranking metrics from fundamentals.
- Remove only the Symbol Workbench header use of `liquidityProfile`; keep the fundamentals API contract intact.
- Preserve existing Daily Ranking table behavior, labels, formatting, evidence colors, desktop/mobile switching, sorting, virtualization, and navigation.
- Use current Daily Ranking labels as canonical. Layout-specific placement may differ, but shared metric labels, formatting, evidence classes, Regime badges, and Signal badges must have one implementation.
- Do not use `placeholderData` or `keepPreviousData` for symbol snapshots.
- Follow TDD for every production change: add a focused failing test, observe the expected failure, add minimal implementation, and rerun the focused test.
- After backend schema changes, run `bun run --filter @trading25/contracts bt:sync` from `apps/ts` and commit generated OpenAPI artifacts.

---

### Task 1: Add the symbol-scoped Daily Ranking backend contract

**Files:**
- Modify: `apps/bt/src/entrypoints/http/schemas/ranking.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`
- Test: `apps/bt/tests/unit/server/routes/test_analytics_complex.py`

**Interfaces:**
- Consumes: existing `RankingService.get_rankings(...)`, `normalize_equity_code(...)`, `canonical_market_label(...)`, `_resolve_latest_stock_data_date_query(...)`, and `MarketDbReader.query_one(...)`.
- Produces: `MarketRankingSymbolResponse` and `RankingService.get_symbol_ranking_snapshot(code: str) -> MarketRankingSymbolResponse`; route `GET /api/analytics/ranking/symbol/{code}`.

- [ ] **Step 1: Write failing service tests for latest-date, market-scoped reuse and code normalization**

Add tests that seed or reuse the ranking fixture, call both the existing collection method and the new method, and assert the same item payload:

```python
snapshot = service.get_symbol_ranking_snapshot("72030")
expected = service.get_rankings(
    date=snapshot.date,
    markets="prime",
    limit=0,
    lookback_days=1,
    period_days=250,
    include_valuation=True,
    include_sector_strength=True,
).rankings.tradingValue
expected_item = next(item for item in expected if normalize_equity_code(item.code) == "7203")

assert snapshot.date is not None
assert snapshot.item == expected_item
assert service.get_symbol_ranking_snapshot("7203").item == snapshot.item
```

Also add one test for an unranked symbol returning the latest `date` with `item is None`, and one reader stub test where latest-date resolution raises `ValueError("No trading data available in database")`, expecting both `date` and `item` to be `None`.

- [ ] **Step 2: Run the service tests and verify RED**

Run:

```bash
cd apps/bt
uv run pytest tests/unit/server/services/test_ranking_service.py -k 'symbol_ranking_snapshot' -q
```

Expected: failure because `get_symbol_ranking_snapshot` and `MarketRankingSymbolResponse` do not exist.

- [ ] **Step 3: Implement the response schema and service wrapper**

Add the response schema directly after `MarketRankingResponse`:

```python
class MarketRankingSymbolResponse(BaseModel):
    date: str | None
    item: RankingItem | None
    lastUpdated: str
```

Implement `get_symbol_ranking_snapshot` with this flow:

```python
def get_symbol_ranking_snapshot(self, code: str) -> MarketRankingSymbolResponse:
    normalized_code = normalize_equity_code(code.strip().upper())
    try:
        target_date = _resolve_latest_stock_data_date_query(self._reader)
    except ValueError as error:
        if str(error) != "No trading data available in database":
            raise
        return MarketRankingSymbolResponse(date=None, item=None, lastUpdated=_now_iso())

    stock = self._reader.query_one(
        f"""
        WITH {stocks_canonical_cte()}
        SELECT code, market_code
        FROM stocks_canonical
        WHERE normalized_code = ?
        LIMIT 1
        """,
        (normalized_code,),
    )
    if stock is None:
        return MarketRankingSymbolResponse(date=target_date, item=None, lastUpdated=_now_iso())

    response = self.get_rankings(
        date=target_date,
        markets=canonical_market_label(str(stock["market_code"])),
        limit=0,
        lookback_days=1,
        period_days=250,
        include_valuation=True,
        include_sector_strength=True,
        sector_strength_family="balanced_sector_strength",
    )
    item = next(
        (row for row in response.rankings.tradingValue if normalize_equity_code(row.code) == normalized_code),
        None,
    )
    return MarketRankingSymbolResponse(date=response.date, item=item, lastUpdated=response.lastUpdated)
```

Import helpers from their existing modules; do not create another normalization helper.

- [ ] **Step 4: Run the service tests and verify GREEN**

Run the same focused command. Expected: all `symbol_ranking_snapshot` tests pass.

- [ ] **Step 5: Write failing route tests**

Add `TestRankingSymbolSnapshot` covering:

```python
def test_200_latest_symbol_snapshot(self, analytics_client):
    response = analytics_client.get("/api/analytics/ranking/symbol/72030")
    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"date", "item", "lastUpdated"}
    assert payload["item"]["code"] in {"7203", "72030"}

def test_200_unranked_symbol(self, analytics_client):
    response = analytics_client.get("/api/analytics/ranking/symbol/9999")
    assert response.status_code == 200
    assert response.json()["item"] is None
```

Add the existing no-database 422 pattern and verify the OpenAPI response points to `MarketRankingSymbolResponse`.

- [ ] **Step 6: Run route tests and verify RED**

Run:

```bash
cd apps/bt
uv run pytest tests/unit/server/routes/test_analytics_complex.py -k 'RankingSymbolSnapshot' -q
```

Expected: 404 for the missing route.

- [ ] **Step 7: Implement the FastAPI route**

Add a route before other dynamic analytics paths:

```python
@router.get(
    "/api/analytics/ranking/symbol/{code}",
    response_model=MarketRankingSymbolResponse,
    summary="Get latest Daily Ranking snapshot for a symbol",
)
async def get_ranking_symbol_snapshot(request: Request, code: str) -> MarketRankingSymbolResponse:
    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Database not initialized")
    try:
        return RankingService(reader).get_symbol_ranking_snapshot(code)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except Exception as error:
        logger.exception(f"Ranking symbol snapshot error: {error}")
        raise HTTPException(status_code=500, detail=f"Failed to get ranking symbol snapshot: {error}") from error
```

- [ ] **Step 8: Verify backend task and commit**

Run:

```bash
cd apps/bt
uv run pytest tests/unit/server/routes/test_analytics_complex.py tests/unit/server/services/test_ranking_service.py -q
uv run ruff check src/application/services/ranking_service.py src/entrypoints/http/routes/analytics_complex.py src/entrypoints/http/schemas/ranking.py
uv run pyright src/application/services/ranking_service.py src/entrypoints/http/routes/analytics_complex.py
```

Expected: all tests, Ruff, and Pyright pass.

Commit:

```bash
git add apps/bt/src/entrypoints/http/schemas/ranking.py apps/bt/src/application/services/ranking_service.py apps/bt/src/entrypoints/http/routes/analytics_complex.py apps/bt/tests/unit/server/services/test_ranking_service.py apps/bt/tests/unit/server/routes/test_analytics_complex.py
git commit -m "feat(bt): add symbol ranking snapshot endpoint"
```

---

### Task 2: Synchronize OpenAPI and add the typed Web query path

**Files:**
- Modify generated: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Modify generated: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `apps/ts/packages/contracts/src/types/api-response-types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/index.ts`
- Test: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Create: `apps/ts/packages/web/src/hooks/useRankingSymbolSnapshot.ts`
- Create: `apps/ts/packages/web/src/hooks/useRankingSymbolSnapshot.test.tsx`

**Interfaces:**
- Consumes: Task 1 endpoint and generated `BtApiSchemas['MarketRankingSymbolResponse']`.
- Produces: `MarketRankingSymbolResponse`, `AnalyticsClient.getMarketRankingSymbol(symbol)`, `rankingSymbolSnapshotKeys`, `normalizeRankingSymbol`, and `useRankingSymbolSnapshot(symbol)`.

- [ ] **Step 1: Refresh generated OpenAPI artifacts**

Run:

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:sync
```

Expected: the OpenAPI JSON and generated TypeScript include `/api/analytics/ranking/symbol/{code}` and `MarketRankingSymbolResponse`.

- [ ] **Step 2: Write a failing API-client path test**

Add:

```typescript
test('getMarketRankingSymbol encodes the symbol path', async () => {
  await client.getMarketRankingSymbol('285A/0');
  expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe(
    'http://localhost:3002/api/analytics/ranking/symbol/285A%2F0'
  );
});
```

Run:

```bash
cd apps/ts
bun test packages/api-clients/src/analytics/AnalyticsClient.test.ts
```

Expected: TypeScript failure because the method is missing.

- [ ] **Step 3: Add shared response types and API-client method**

Add the generated response alias to `api-response-types.ts`:

```typescript
export type MarketRankingSymbolResponse = BtApiSchemas['MarketRankingSymbolResponse'];
```

Add a matching API-client type with the existing `RankingItem`:

```typescript
export interface MarketRankingSymbolResponse {
  date: string | null;
  item: RankingItem | null;
  lastUpdated: string;
}
```

Add and export:

```typescript
async getMarketRankingSymbol(symbol: string): Promise<MarketRankingSymbolResponse> {
  return this.request<MarketRankingSymbolResponse>(
    `/api/analytics/ranking/symbol/${encodeURIComponent(symbol)}`
  );
}
```

Run the API-client test again. Expected: pass.

- [ ] **Step 4: Write failing hook tests for normalization and stale-data prevention**

Create hook tests that assert:

```typescript
expect(normalizeRankingSymbol(' 72030 ')).toBe('7203');
expect(normalizeRankingSymbol('285A0')).toBe('285A');
expect(normalizeRankingSymbol(null)).toBeNull();
```

Mock `analyticsClient.getMarketRankingSymbol`, render with `7203`, resolve it, rerender with `6758` whose promise remains pending, and assert `result.current.data` becomes `undefined` until the second promise resolves. Also assert a null symbol disables the query.

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/hooks/useRankingSymbolSnapshot.test.tsx
```

Expected: failure because the hook does not exist.

- [ ] **Step 5: Implement the TanStack Query hook**

```typescript
export function normalizeRankingSymbol(symbol: string | null | undefined): string | null {
  const normalized = symbol?.trim().toUpperCase();
  if (!normalized) return null;
  return normalized.length === 5 && normalized.endsWith('0') ? normalized.slice(0, -1) : normalized;
}

export const rankingSymbolSnapshotKeys = {
  detail: (symbol: string) => ['ranking', 'symbol', normalizeRankingSymbol(symbol)] as const,
};

export function useRankingSymbolSnapshot(symbol: string | null | undefined) {
  const normalizedSymbol = normalizeRankingSymbol(symbol);
  return useQuery<MarketRankingSymbolResponse>({
    queryKey: ['ranking', 'symbol', normalizedSymbol],
    queryFn: () => analyticsClient.getMarketRankingSymbol(normalizedSymbol as string),
    enabled: normalizedSymbol != null,
    staleTime: 60_000,
    gcTime: 300_000,
  });
}
```

Do not add placeholder data. Rerun the hook test and expect pass.

- [ ] **Step 6: Verify contracts, client, and hook; commit**

Run:

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:check
bun test packages/api-clients/src/analytics/AnalyticsClient.test.ts
bun run --filter @trading25/web test --run src/hooks/useRankingSymbolSnapshot.test.tsx
bun run quality:typecheck
```

Commit all generated and handwritten contract/client/hook files:

```bash
git add apps/ts/packages/contracts apps/ts/packages/api-clients/src/analytics apps/ts/packages/web/src/hooks/useRankingSymbolSnapshot.ts apps/ts/packages/web/src/hooks/useRankingSymbolSnapshot.test.tsx
git commit -m "feat(ts): add symbol ranking snapshot query"
```

---

### Task 3: Extract shared Daily Ranking presentation semantics

**Files:**
- Create: `apps/ts/packages/web/src/components/Ranking/dailyRankingPresentation.tsx`
- Create: `apps/ts/packages/web/src/components/Ranking/dailyRankingPresentation.test.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/EquityRankingTable.tsx`
- Modify: `apps/ts/packages/web/src/components/Ranking/index.ts`
- Test: `apps/ts/packages/web/src/components/Ranking/RankingTable.test.tsx`

**Interfaces:**
- Consumes: `EquityRankingItem` and the existing functions in `rankingEvidenceTiers.ts` and `rankingState.ts`.
- Produces: `DAILY_RANKING_VALUE_METRICS`, `DailyRankingMetric`, `DailyRankingMetricValue`, `DailyRankingRegimeChip`, `DailyRankingSignalChips`, and `SectorStrengthScoreChip`.

- [ ] **Step 1: Write failing presentation-contract tests**

Define expected metric keys and canonical labels:

```typescript
expect(DAILY_RANKING_VALUE_METRICS.map(({ key, label }) => [key, label])).toEqual([
  ['sectorStrengthScore', 'Sector Strength'],
  ['currentPrice', '現在値'],
  ['changePercentage', '騰落率'],
  ['sma5AboveCount5d', 'SMA5 5D'],
  ['per', 'PER'],
  ['forwardPer', 'Fwd PER'],
  ['forecastOperatingProfitGrowthRatio', 'Fwd OP/OP'],
  ['psr', 'PSR'],
  ['forwardPsr', 'Fwd PSR'],
  ['pbr', 'PBR'],
  ['valueCompositeScore', 'Value Score'],
  ['liquidityResidualZ', '流動性Z'],
  ['tradingValue', '売買代金'],
]);
```

Render representative positive, negative, missing, value, warning, Regime, and Signal cases. Assert exact formatted text and existing evidence CSS classes.

- [ ] **Step 2: Run the presentation test and verify RED**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/components/Ranking/dailyRankingPresentation.test.tsx
```

Expected: missing module failure.

- [ ] **Step 3: Extract the shared presentation module**

Move the existing formatting, evidence-class, Sector Strength, Regime, and Signal semantics from `EquityRankingTable.tsx` without changing their output. Define metrics with functions rather than preformatted values:

```typescript
export interface DailyRankingMetric {
  key: DailyRankingMetricKey;
  label: string;
  title?: string;
  format: (item: EquityRankingItem) => string;
  resolveClassName?: (item: EquityRankingItem) => string | undefined;
}
```

`DailyRankingMetricValue` receives `item` and `metric`, uses `metric.format(item)`, and applies `metric.resolveClassName?.(item)`. Keep price and trading-value formatting wired to the existing shared formatters. Export Regime and Signals as explicit components because they render multiple semantic badges rather than one scalar value.

- [ ] **Step 4: Run the presentation test and verify GREEN**

Run the same focused command. Expected: pass.

- [ ] **Step 5: Refactor the existing Ranking table to consume shared semantics**

Replace local formatting/class helpers and local `RegimeChip`, `SignalChips`, and `SectorStrengthScoreChip` with imports. Use shared metric definitions for labels and values while preserving the current desktop column order and mobile-card layout. Do not change `EQUITY_SORT_FIELDS`, sort behavior, `columnCount`, virtualization thresholds, or click handlers.

- [ ] **Step 6: Verify table regression and commit**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/components/Ranking/dailyRankingPresentation.test.tsx src/components/Ranking/RankingTable.test.tsx
bun run quality:typecheck
```

Expected: new presentation tests and all existing RankingTable tests pass without snapshot or text changes.

Commit:

```bash
git add apps/ts/packages/web/src/components/Ranking
git commit -m "refactor(web): share daily ranking presentation"
```

---

### Task 4: Integrate the Daily Ranking Snapshot into Symbol Workbench

**Files:**
- Create: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.tsx`
- Create: `apps/ts/packages/web/src/components/Ranking/DailyRankingSnapshot.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx`
- Modify: `apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx`

**Interfaces:**
- Consumes: Task 2 `useRankingSymbolSnapshot`, Task 3 presentation exports, `StockInfoResponse`, and `ChartHeaderMarketCaps`.
- Produces: one integrated `Daily Ranking Snapshot` inside `ChartHeader`; no `liquidityProfile` header prop.

- [ ] **Step 1: Write failing component tests for the integrated snapshot**

Render `DailyRankingSnapshot` with a complete response and assert:

- `Daily Ranking Snapshot` and `As of 2026-07-09`;
- retained Market, Index Membership, Sector 17, Sector 33, Market Cap, and Free-Float Market Cap;
- all 13 scalar shared metrics plus Regime and Signals;
- ranking values win for Market, Sector 33, and Market Cap;
- `item: null` uses supplemental basic fields but shows `Daily Ranking data unavailable`;
- partial ranking fields show `-` rather than fundamentals substitutions;
- loading and retry-capable error states;
- grid includes `grid-cols-2` and Regime/Signals span their row.

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/components/Ranking/DailyRankingSnapshot.test.tsx
```

Expected: missing component failure.

- [ ] **Step 2: Implement `DailyRankingSnapshot`**

Use explicit view-oriented props:

```typescript
interface DailyRankingSnapshotProps {
  response: MarketRankingSymbolResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onRetry: () => void;
  stockInfo: StockInfoResponse | undefined;
  latestMarketCaps: ChartHeaderMarketCaps;
}
```

Render one border-top section with a title/reference-date row, a responsive basic-info grid, and a responsive metric grid driven by `DAILY_RANKING_VALUE_METRICS`. Render `DailyRankingRegimeChip` and `DailyRankingSignalChips` from the shared module. Keep missing scalar positions visible as `-`.

- [ ] **Step 3: Run component tests and verify GREEN**

Run the same focused command. Expected: pass.

- [ ] **Step 4: Write failing Symbol Workbench integration tests**

Mock `useRankingSymbolSnapshot`. Replace the existing Prime Liquidity test with assertions that:

```typescript
expect(screen.getByText('Daily Ranking Snapshot')).toBeInTheDocument();
expect(screen.getByText('As of 2026-07-09')).toBeInTheDocument();
expect(screen.queryByText('Prime Liquidity')).not.toBeInTheDocument();
expect(screen.queryByText('Med ADV60 / Free Float')).not.toBeInTheDocument();
expect(screen.queryByText(/流動性等価株価/)).not.toBeInTheDocument();
```

Set `matchedDate` to a different date and confirm it remains only in the existing execution-context field. Add a refresh test asserting invalidation of `rankingSymbolSnapshotKeys.detail('7203')`.

- [ ] **Step 5: Run the Workbench test and verify RED**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/pages/SymbolWorkbenchPage.test.tsx
```

Expected: failure because the page does not call the new hook and still renders Prime Liquidity.

- [ ] **Step 6: Wire the hook and replace the old header sections**

In `SymbolWorkbenchPage`, call `useRankingSymbolSnapshot(selectedSymbol)`, pass its `data/isLoading/error/refetch` to `ChartHeader`, and invalidate its key in `invalidateSelectedSymbolQueries`. Keep `useFundamentals` for free-float capitalization, provenance, and lower panels.

In `SymbolWorkbenchHeader`, remove `ApiLiquidityProfile`, `LiquidityProfileStrip`, its Workbench-only formatting helpers, and the old six-field grid. Render `DailyRankingSnapshot` in their place. Keep identity, actions, execution context, warnings, and feedback banners unchanged.

- [ ] **Step 7: Verify frontend integration and commit**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test --run src/components/Ranking/RankingTable.test.tsx src/components/Ranking/dailyRankingPresentation.test.tsx src/components/Ranking/DailyRankingSnapshot.test.tsx src/hooks/useRankingSymbolSnapshot.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
bun run quality:typecheck
bun run quality:lint
```

Commit:

```bash
git add apps/ts/packages/web/src/components/Ranking apps/ts/packages/web/src/pages/SymbolWorkbenchHeader.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.tsx apps/ts/packages/web/src/pages/SymbolWorkbenchPage.test.tsx
git commit -m "feat(web): show daily ranking snapshot in workbench"
```

---

### Task 5: Cross-stack verification and live UI proof

**Files:**
- Modify only if verification reveals a scoped defect.

**Interfaces:**
- Consumes: completed Tasks 1-4.
- Produces: verified OpenAPI/reference freshness, cross-stack tests, and desktop/mobile browser evidence.

- [ ] **Step 1: Run cross-stack automated verification**

```bash
python3 scripts/skills/refresh_skill_references.py --check
uv run --project apps/bt pytest apps/bt/tests/unit/server/routes/test_analytics_complex.py apps/bt/tests/unit/server/services/test_ranking_service.py -q
uv run --project apps/bt ruff check apps/bt/src/application/services/ranking_service.py apps/bt/src/entrypoints/http/routes/analytics_complex.py apps/bt/src/entrypoints/http/schemas/ranking.py
uv run --project apps/bt pyright apps/bt/src/application/services/ranking_service.py apps/bt/src/entrypoints/http/routes/analytics_complex.py
cd apps/ts
bun run --filter @trading25/contracts bt:check
bun test packages/api-clients/src/analytics/AnalyticsClient.test.ts
bun run --filter @trading25/web test --run src/components/Ranking/RankingTable.test.tsx src/components/Ranking/dailyRankingPresentation.test.tsx src/components/Ranking/DailyRankingSnapshot.test.tsx src/hooks/useRankingSymbolSnapshot.test.tsx src/pages/SymbolWorkbenchPage.test.tsx
bun run quality:typecheck
bun run quality:lint
```

Expected: all commands pass with no generated contract drift.

- [ ] **Step 2: Run local services and inspect the endpoint**

Start FastAPI on 3002 and Web on 5173 using the repository's normal development commands. Request one Prime and one non-Prime symbol snapshot and confirm the response date and item are present.

- [ ] **Step 3: Verify Daily Ranking against Symbol Workbench in the browser**

At the normal approximately 1180px working width:

1. Open Daily Ranking for a Prime symbol and record date, valuation metrics, Value Score, Liquidity Z, Regime, and Signals.
2. Open the symbol in Symbol Workbench and confirm the same values, labels, evidence colors, badges, and date.
3. Repeat for a Standard or Growth symbol.
4. Confirm the old Prime Liquidity diagnostics are absent and header actions still work.
5. Resize to a mobile viewport and confirm the two-column grid, full-width badge rows, and chart accessibility.

- [ ] **Step 4: Commit only scoped verification fixes, if any**

If no fixes are required, do not create an empty commit. If fixes are required, rerun their focused tests first and commit only the affected files.
