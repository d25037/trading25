# Ranking Display Path Elimination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Ranking display-time full-history scans, hidden-section computation, full-market symbol recursion, and event-loop blocking while preserving Market v5 financial semantics.

**Architecture:** Add an additive `scope` selector to the existing Ranking endpoint, bound every shared provider-price relation before deduplication, and replace symbol recursion with a dedicated one-item path. Keep the response schema compatible, run synchronous DuckDB services in FastAPI's threadpool, and make the React Query key/retry behavior match the actual transport request.

**Tech Stack:** Python 3.12, FastAPI, DuckDB, Pydantic, pytest, React 19, TanStack Query/Router, TypeScript, Vitest, Bun.

## Global Constraints

- Preserve Market v5 / `provider_adjusted_v1` and current-basis PIT validation; do not add current/latest fallbacks.
- Keep the existing `MarketRankingResponse` shape; unrequested collections are empty.
- Omitted `scope` means `all` for API compatibility.
- Financial calculations remain in `apps/bt`; TypeScript only selects scopes, transports requests, and displays results.
- No GET path may write, sync, rebuild, or export MarketDB state.
- Write a failing test and observe the expected failure before each production change.

---

### Task 1: Bound provider-price relations before deduplication

**Files:**
- Modify: `apps/bt/src/application/services/ranking_fundamental_queries.py`
- Modify: `apps/bt/src/application/services/ranking_liquidity.py`
- Modify: `apps/bt/src/application/services/ranking_value_composite_features.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`

**Interfaces:**
- Produces: `provider_price_cte(where_clause: str) -> str`, whose `where_clause` is inserted directly into the inner `FROM stock_data AS price` source.
- Consumes: caller-owned positional parameters matching the bound embedded in the CTE.

- [ ] **Step 1: Write failing SQL-shape tests**

Add tests that capture SQL passed to `MarketDbReader.query()` and assert an exact-date caller generates this ordering:

```python
source_index = sql.index("FROM stock_data AS price")
bound_index = sql.index("WHERE price.date = ?", source_index)
row_number_index = sql.index("ROW_NUMBER() OVER")
assert row_number_index < source_index < bound_index
```

Add a liquidity test asserting `WHERE price.date >= ? AND price.date <= ?` exists inside `provider_price`, rather than only in `price_features`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -k "provider_price and bound" -q
```

Expected: failure because `provider_price_cte()` has no inner date predicate.

- [ ] **Step 3: Implement bounded CTEs**

Change the helper to require a predicate:

```python
def provider_price_cte(where_clause: str) -> str:
    if not where_clause.strip():
        raise ValueError("provider_price_cte requires a bounded stock_data predicate")
    price_norm = normalized_code_sql("price.code")
    price_order = prefer_4digit_order_sql("price.code")
    return f"""
        provider_price AS (
            SELECT normalized_code, date, open, high, low, close, volume
            FROM (
                SELECT
                    {price_norm} AS normalized_code,
                    price.date,
                    price.open,
                    price.high,
                    price.low,
                    price.close,
                    price.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {price_norm}, price.date
                        ORDER BY {price_order}
                    ) AS rn
                FROM stock_data AS price
                WHERE {where_clause}
            )
            WHERE rn = 1
        )
    """
```

Use `price.date = ?` for exact-session valuation/fundamental callers and `price.date >= ? AND price.date <= ?` for liquidity and value-composite lookback callers. Update positional parameter tuples in the same order as CTE appearance.

- [ ] **Step 4: Verify GREEN and regression coverage**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/ranking_fundamental_queries.py apps/bt/src/application/services/ranking_liquidity.py apps/bt/src/application/services/ranking_value_composite_features.py apps/bt/tests/unit/server/services/test_ranking_service.py
git commit -m "perf(bt): bound ranking provider price reads"
```

### Task 2: Compute only the requested Ranking section

**Files:**
- Modify: `apps/bt/src/application/contracts/ranking.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`
- Test: `apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py`

**Interfaces:**
- Produces: `RankingScope = Literal["all", "tradingValue", "periodHigh", "periodLow", "indexPerformance"]`.
- Produces: a `scope: RankingScope = "all"` keyword on `RankingService.get_rankings()`.
- Preserves: `MarketRankingResponse` with empty unrequested arrays.

- [ ] **Step 1: Write failing service tests per scope**

Patch each ranking query/enrichment loader and assert, for example:

```python
response = service.get_rankings(scope="tradingValue")
trading_value_query.assert_called_once()
gainers_query.assert_not_called()
period_high_query.assert_not_called()
index_performance_loader.assert_not_called()
assert response.rankings.gainers == []
assert response.indexPerformance == []
```

Add corresponding cases for `periodHigh`, `periodLow`, `indexPerformance`, and default `all`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -k "ranking_scope" -q
```

Expected: failure because `scope` is not accepted and all collections are always built.

- [ ] **Step 3: Implement scope-gated orchestration**

Build empty collection defaults, invoke only selected base queries, and apply equity enrichments only when at least one requested equity collection is non-empty. Load sector strength for equity scopes only when requested by the response, and load `indexPerformance` only for `all` or `indexPerformance`.

Expose the query parameter in FastAPI:

```python
scope: ranking_contracts.RankingScope = Query("all")
```

Pass it unchanged into `get_rankings()`.

- [ ] **Step 4: Add and run route contract tests**

Assert `scope=tradingValue` is forwarded and an invalid value returns 422. Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/test_routes_analytics_fundamentals.py tests/unit/server/services/test_ranking_service.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/contracts/ranking.py apps/bt/src/application/services/ranking_service.py apps/bt/src/entrypoints/http/routes/analytics_complex.py apps/bt/tests/unit/server/services/test_ranking_service.py apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py
git commit -m "perf(bt): scope ranking response computation"
```

### Task 3: Replace full-market symbol recursion

**Files:**
- Modify: `apps/bt/src/application/services/ranking_daily_queries.py`
- Modify: `apps/bt/src/application/services/ranking_service.py`
- Test: `apps/bt/tests/unit/server/services/test_ranking_service.py`

**Interfaces:**
- Produces: `ranking_by_trading_value_symbol(reader, date, code, market_codes) -> RankingItem | None`.
- Preserves: `RankingService.get_symbol_ranking_snapshot(code) -> MarketRankingSymbolResponse`.

- [ ] **Step 1: Write failing no-recursion and semantic-equivalence tests**

Add a test that replaces `service.get_rankings` with a function that raises and verifies symbol lookup still succeeds. Add a DuckDB fixture test comparing the dedicated symbol item with the same code from `get_rankings(scope="all", limit=0)`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -k "symbol_snapshot" -q
```

Expected: the no-recursion test fails when `get_symbol_ranking_snapshot()` calls `self.get_rankings()`.

- [ ] **Step 3: Implement a ranked target-session symbol query**

Use a bounded target-date price relation, compute `RANK()`/`ROW_NUMBER()` over the market's trading value, and filter to the normalized symbol only after rank calculation. Wrap the single item in one-element collections for existing enrichment helpers, then run valuation, liquidity, technical flags, daily materialized metrics, and sector enrichment only for that item.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_ranking_service.py -q
```

Expected: all tests pass, including semantic equivalence.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/application/services/ranking_daily_queries.py apps/bt/src/application/services/ranking_service.py apps/bt/tests/unit/server/services/test_ranking_service.py
git commit -m "perf(bt): query ranking symbol directly"
```

### Task 4: Move synchronous Ranking services off the event loop

**Files:**
- Modify: `apps/bt/src/entrypoints/http/routes/analytics_complex.py`
- Test: `apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py`

**Interfaces:**
- Consumes: `starlette.concurrency.run_in_threadpool`.
- Preserves: route signatures, response models, and exception mapping.

- [ ] **Step 1: Write failing threadpool-boundary tests**

Patch the module's `run_in_threadpool`, call each synchronous Ranking route, and assert the service callable plus keyword arguments crossed the boundary.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/test_routes_analytics_fundamentals.py -k "threadpool" -q
```

Expected: failure because routes invoke service methods directly.

- [ ] **Step 3: Wrap synchronous service calls**

Use:

```python
return await run_in_threadpool(
    service.get_rankings,
    date=date,
    limit=limit,
    markets=markets,
    lookback_days=lookbackDays,
    period_days=periodDays,
    scope=scope,
)
```

Apply the same boundary to symbol, fundamental, value-composite, and score handlers in this route module.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/test_routes_analytics_fundamentals.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/bt/src/entrypoints/http/routes/analytics_complex.py apps/bt/tests/unit/server/test_routes_analytics_fundamentals.py
git commit -m "perf(api): isolate blocking ranking reads"
```

### Task 5: Align frontend views, query keys, and retry semantics

**Files:**
- Modify: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Modify: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`
- Modify: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts`
- Modify: `apps/ts/packages/web/src/pages/RankingPage.tsx`
- Modify: `apps/ts/packages/web/src/hooks/useRanking.ts`
- Modify: `apps/ts/packages/web/src/providers/QueryProvider.tsx`
- Test: `apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts`
- Test: `apps/ts/packages/web/src/pages/RankingPage.test.tsx`
- Test: `apps/ts/packages/web/src/hooks/useRanking.test.tsx`
- Create: `apps/ts/packages/web/src/providers/QueryProvider.test.ts`

**Interfaces:**
- Consumes generated `scope` query type from the OpenAPI contract.
- Produces frontend view-to-scope mapping with no local financial calculation.
- Produces exported/testable `shouldRetry(failureCount, error)` recognizing `ApiError` and `HttpRequestError`.

- [ ] **Step 0: Synchronize the additive backend contract**

Run:

```bash
bun --cwd=apps/ts run --filter @trading25/contracts bt:sync
```

Expected: the generated Ranking query type gains optional `scope`; no unrelated schema drift is introduced.

- [ ] **Step 1: Write failing view-scope and URL tests**

Assert Individual Stocks sends `tradingValue`, New High sends `periodHigh`, New Low sends `periodLow`, and Indices sends `indexPerformance`. Assert the AnalyticsClient URL contains the selected `scope`.

- [ ] **Step 2: Write failing retry tests**

Use `new HttpRequestError("bad request", "http", { status: 400 })` and assert `shouldRetry(0, error) === false`; cover 408/429/500/network and the attempt cap.

- [ ] **Step 3: Run tests and verify RED**

Run:

```bash
bun --cwd=apps/ts run --filter @trading25/web test -- src/pages/RankingPage.test.tsx src/hooks/useRanking.test.tsx src/providers/QueryProvider.test.ts
bun --cwd=apps/ts run --filter @trading25/api-clients test -- src/analytics/AnalyticsClient.test.ts
```

Expected: failures because scope is not sent and `HttpRequestError` is not classified.

- [ ] **Step 4: Implement scope mapping and retry classification**

Derive scope in `buildRankingQueryParams()` from active view/event type, forward it through `useRanking` and `AnalyticsClient`, and remove any key-only field that is not transmitted. Classify HTTP status consistently for both error classes while retaining three bounded retries for retryable failures.

- [ ] **Step 5: Verify GREEN**

Run the commands from Step 3. Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add apps/ts/packages/contracts/openapi/bt-openapi.json apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts apps/ts/packages/api-clients/src/analytics/AnalyticsClient.ts apps/ts/packages/api-clients/src/analytics/AnalyticsClient.test.ts apps/ts/packages/web/src/pages/RankingPage.tsx apps/ts/packages/web/src/pages/RankingPage.test.tsx apps/ts/packages/web/src/hooks/useRanking.ts apps/ts/packages/web/src/hooks/useRanking.test.tsx apps/ts/packages/web/src/providers/QueryProvider.tsx apps/ts/packages/web/src/providers/QueryProvider.test.ts
git commit -m "perf(web): request only visible ranking data"
```

### Task 6: Verify contracts and prove the performance change

**Files:**
- Modify: `apps/ts/packages/contracts/openapi/bt-openapi.json`
- Modify: `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts`

**Interfaces:**
- Produces generated TypeScript query type containing optional `scope`.

- [ ] **Step 1: Verify the generated OpenAPI contract is current**

Run:

```bash
bun --cwd=apps/ts run --filter @trading25/contracts bt:check
```

Expected: generated contract includes `scope` and the check reports no drift.

- [ ] **Step 2: Run focused and cross-project verification**

Run:

```bash
uv run --directory apps/bt pytest tests/unit/server/test_routes_analytics_fundamentals.py tests/unit/server/services/test_ranking_service.py -q
uv run --directory apps/bt ruff check src/application/services src/entrypoints/http/routes tests/unit/server/services/test_ranking_service.py tests/unit/server/test_routes_analytics_fundamentals.py
uv run --directory apps/bt pyright src/application/services src/entrypoints/http/routes
bun --cwd=apps/ts run --filter @trading25/contracts bt:check
bun --cwd=apps/ts run quality:typecheck
bun --cwd=apps/ts run workspace:test
```

Expected: all commands pass.

- [ ] **Step 3: Re-run the diagnosed local MarketDB benchmark**

Measure `scope=tradingValue`, `scope=indexPerformance`, fundamental Ranking, and symbol snapshot against `/Users/mirage/.local/share/trading25/market-timeseries/market.duckdb`. Record elapsed time, SQL count, response size, and `/usr/bin/time -l` maximum resident set size. Confirm the exact-date valuation plan no longer windows 7.9 million `stock_data` rows.

- [ ] **Step 4: Review generated and implementation diffs**

Run:

```bash
git diff --check
git status --short
git diff --stat origin/main...HEAD
```

Expected: only Ranking performance, tests, contract generation, design, and plan files are changed.

- [ ] **Step 5: Commit any verification-only corrections**

If verification exposes a real contract or implementation defect, add a failing regression test first, fix it, rerun the covering checks, and commit only those corrective files. If no defect is found, do not create an empty commit.

### Task 7: Final review and publication

**Files:**
- Review all changes from `origin/main...HEAD`.

**Interfaces:**
- Produces a pushed `codex/fix-ranking-waste` branch and draft PR targeting the repository default branch.

- [ ] **Step 1: Request independent code review**

Provide the reviewer the approved design, this plan, base SHA `9586584a`, current HEAD, and require findings ordered by severity with file/line evidence.

- [ ] **Step 2: Resolve all Critical and Important findings**

For each production fix, add or adjust a failing test first, verify RED, implement, and verify GREEN. Re-run focused checks after corrections.

- [ ] **Step 3: Run final verification from a clean status snapshot**

Repeat Task 6 Step 2 and `git diff --check`. Do not rely on earlier output.

- [ ] **Step 4: Push and create the PR**

```bash
git push -u origin codex/fix-ranking-waste
gh pr create --draft --base main --head codex/fix-ranking-waste \
  --title "perf: eliminate wasteful ranking display paths" \
  --body $'## Summary\n- bound Ranking price reads before deduplication\n- compute only the visible Ranking section\n- replace full-market symbol recursion and isolate blocking reads\n- align frontend scope and retry behavior\n\n## Validation\n- backend Ranking tests, ruff, and pyright\n- frontend tests, typecheck, and contract check\n- local MarketDB before/after benchmark'
```

The PR body must document root cause, implementation, before/after performance, compatibility, and exact validation commands.
