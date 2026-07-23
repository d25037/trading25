# Ranking Display Path Elimination Design

## Goal

Eliminate request-time work that is unrelated to the Ranking surface currently being displayed, while preserving Market v5 PIT/current-basis semantics and the existing response schema for callers that do not opt into the narrower path.

## Confirmed problem

The current Ranking display combines four independent sources of avoidable cost:

1. `provider_price_cte()` deduplicates every historical `stock_data` row before a caller applies its target-date or lookback bound. On the current MarketDB this windows 7.9 million rows to return roughly 1,500 current-session rows.
2. `/api/analytics/ranking` always builds five equity ranking collections plus index performance, even though each Ranking view renders only one collection.
3. `/api/analytics/ranking/symbol/{code}` obtains one item by invoking the full-market `get_rankings(limit=0)` path and searching its result.
4. `async def` HTTP handlers invoke synchronous DuckDB work directly, blocking the FastAPI event loop until the query and Python enrichment finish.

The frontend also repeats expensive requests when a technical-event selector changes without changing the transmitted request, and its global retry predicate does not recognize the shared API client's `HttpRequestError` type.

## Selected approach

Use an additive request-scope contract and bounded SQL, rather than introducing another persisted Ranking snapshot.

### Request scope

Add an optional `scope` query parameter to `GET /api/analytics/ranking` with these values:

- `all`: preserve the existing response and computation for compatibility; this remains the default when the parameter is omitted.
- `tradingValue`: compute only the trading-value collection and its requested enrichments.
- `periodHigh`: compute only the period-high collection and its requested enrichments.
- `periodLow`: compute only the period-low collection and its requested enrichments.
- `indexPerformance`: compute only index performance and the sector-strength data it requires.

Unrequested ranking arrays and `indexPerformance` remain present but empty, so the response schema is unchanged. The web Ranking page always supplies the scope matching its active view. The stock view may retain `limit=0`, because the virtualized table and local filters intentionally need the complete trading-value collection; it must no longer cause four hidden collections to be computed or transferred.

### Bounded provider-price reads

Change the reusable provider-price CTE builder to require or accept an explicit inner predicate. Every Ranking caller must place its exact target date or required lookback window inside the `stock_data` source before code/date deduplication and window functions.

The following invariants remain unchanged:

- 4-digit/5-digit code normalization and 4-digit precedence.
- Provider-adjusted `stock_data` as the consumer price SoT.
- Target-date and PIT filtering before latest-row selection.
- Current-basis provider-window and fundamentals integrity validation.

Tests must inspect the generated SQL or captured query to prove the bound appears inside the provider-price source, not only in an outer consumer.

### Dedicated symbol snapshot

Replace the recursive full-market call in `get_symbol_ranking_snapshot()` with a dedicated service path:

1. Resolve the symbol and target market session.
2. Compute its trading-value row and rank with a bounded target-session cross-sectional query.
3. Enrich only that item. Cross-sectional work may still read the target-session valuation universe or the bounded liquidity/technical windows when required by the metric definition, but it must not build or serialize the five full ranking collections.
4. Return the existing `MarketRankingSymbolResponse` contract.

The symbol result must remain semantically equal to the corresponding item from the compatibility `scope=all&limit=0` result for the same database snapshot.

### Event-loop isolation

Keep the HTTP API asynchronous, but execute synchronous Ranking service calls through Starlette's threadpool boundary. Apply this to daily Ranking, fundamental Ranking, value-composite Ranking, score, and symbol snapshot handlers that use `MarketDbReader` synchronously. Error mapping and correlation behavior remain unchanged.

### Frontend request correctness

Map active views to backend scopes:

- Individual Stocks -> `tradingValue`
- Technical Events / New High -> `periodHigh`
- Technical Events / New Low -> `periodLow`
- Indices -> `indexPerformance`

The query key and transmitted parameters must describe the same request. A High/Low switch therefore changes the backend scope instead of producing a new cache key for an identical URL.

The shared React Query retry predicate must recognize both the web `ApiError` and API-client `HttpRequestError`. Non-retriable 4xx errors stop immediately; 408, 429, 5xx, timeout, and network failures retain the existing bounded retry behavior.

Do not eagerly fetch daily and fundamental Ranking simultaneously. The existing mutually exclusive `enabled` behavior remains.

## Data flow

```text
Ranking view
  -> scope-specific React Query key
  -> GET /api/analytics/ranking?scope=<active-scope>
  -> FastAPI threadpool boundary
  -> RankingService builds one requested section
  -> bounded MarketDB reads
  -> unchanged response schema with unrequested sections empty

Symbol Workbench
  -> GET /api/analytics/ranking/symbol/<code>
  -> FastAPI threadpool boundary
  -> dedicated bounded symbol snapshot path
  -> one RankingItem
```

## Compatibility and error handling

- Omitting `scope` retains `all`, so existing API clients and tests continue to receive every collection.
- Invalid scope values return FastAPI validation errors through the unified error response.
- No GET route writes, rebuilds current-basis state, triggers sync, or exports Parquet.
- Missing/inconsistent Market v5 provider or current-basis state continues to return the existing typed recovery error.
- No frontend-local financial calculation or fallback is introduced.

## Testing

Implementation follows red-green-refactor and adds coverage for:

1. Provider-price SQL bounds are applied before deduplication for exact-date and lookback callers.
2. Each scope invokes only its required query/enrichment functions and returns empty unrequested sections.
3. Omitted scope preserves the existing full response.
4. Symbol snapshot does not call `get_rankings()` and matches the corresponding compatibility item.
5. Ranking HTTP handlers cross a threadpool boundary without changing error mapping.
6. RankingPage sends the correct scope for all four views.
7. High/Low switching produces distinct URLs whose query keys match them.
8. `HttpRequestError` 4xx responses are not retried, while retryable transport/server failures remain bounded.
9. Existing PIT stability and current-basis validation tests remain green.

## Performance acceptance

On the local 4 GB MarketDB fixture/state used during diagnosis:

- An exact-date provider-price query must not physically window all historical `stock_data` rows.
- Page-equivalent Individual Stocks must compute one ranking collection, not five.
- Symbol snapshot must not invoke the full-market aggregate service path.
- Repeat measurements must show a material reduction from the diagnosed approximately 1.8 GB peak RSS; the PR records before/after elapsed time, SQL count, response size, and peak RSS where available.

## Non-goals

- Adding a new persisted Ranking snapshot table.
- Changing Ranking formulas or PIT/current-basis semantics.
- Removing intentional full-universe cross-sectional calculations required for percentiles, ranks, or liquidity regression.
- Redesigning the Ranking UI.
- Changing screening or backtest execution paths.
