---
name: jquants-api-optimization
description: JQuants API optimization strategies for efficient data fetching.
globs: "packages/shared/**/*.ts, packages/api/**/*.ts, packages/cli/**/*.ts"
alwaysApply: false
---

# JQuants API Optimization

## Stock Code vs Date Parameter

**Critical API Behaviors**:
- `code` parameter: Fetches ALL historical data for ONE stock (10+ years in 1 call)
- `date` parameter: Fetches ALL stocks for ONE date (~1800 stocks in 1 call)

**Optimal Strategy**:
| Scenario | Best Parameter | Example |
|----------|---------------|---------|
| Few stocks, many days | `code` | 50 stocks × 1 call = 50 API calls |
| Many stocks, few days | `date` | 1000 stocks ÷ 250 days = 250 API calls |

## Database Sync System

Uses **date-based fetching** for efficiency:
- **Initial sync**: 2 years of data (TOPIX + listed_info + ~500 trading days)
- **Daily update**: 2-3 API calls (TOPIX check + 1-2 new trading days)
- **Database**: `$HOME/.local/share/trading25/market.db` (XDG-compliant)

**Stock Split Detection**: Automatically detects `adjustment_factor != 1.0` and refetches historical data.

## Dataset System

Uses **stock-based fetching** for sampling/screening:
- Omitting `from`/`to` returns **all available historical data**
- One API call per stock gets complete history
- Optimal for small datasets (<250 stocks)

## Rate Limiting (Updated: 2026-01-15)

### JQuants API Call Limits by Plan

| Plan | Limit | Requests/sec | Interval (with 10% margin) |
|------|-------|--------------|---------------------------|
| Free | 5 req/min | 0.083 | ~13,200ms |
| Light | 60 req/min | 1.0 | ~1,100ms |
| Standard | 120 req/min | 2.0 | ~550ms |
| Premium | 500 req/min | 8.33 | ~132ms |

**Environment Variable**: `JQUANTS_PLAN` (required)
- Values: `free`, `light`, `standard`, `premium`
- Error thrown if not set

### Implementation

Rate limiting is built into `BaseJQuantsClient` using a promise chain approach:

```typescript
// Global rate limiter state (shared across all client instances)
const globalRateLimiter = {
  lastRequestTime: 0,
  requestQueue: Promise.resolve(),
};

// Before each request, wait for rate limit
await this.waitForRateLimit();
```

**Key features**:
- Promise chain serializes concurrent requests
- 10% safety margin on intervals
- No external dependencies
- Automatic plan detection from `JQUANTS_PLAN` env var

### BatchExecutor for Retry Logic

For batch operations with retry:

```typescript
import { BatchExecutor } from '@trading25/shared/clients/base/BatchExecutor';

const executor = new BatchExecutor({
  maxRetries: 3,
  retryDelayMs: 1000,
  maxRetryDelayMs: 10000,
});

// Single operation with retry
const result = await executor.execute(() => client.getListedInfo());

// Batch operations with concurrency control
const results = await executor.executeAll(operations, {
  concurrency: 2,
  signal: abortController.signal,
  onProgress: (completed, total) => console.log(`${completed}/${total}`),
});
```

**Per-Stock Timeout**: Each stock fetch has a 30-second timeout to prevent indefinite hangs

```typescript
import { IndicesFetcher } from '@trading25/shared/dataset';

const fetcher = new IndicesFetcher(config);
const sectorData = await fetcher.fetchSectorIndices(
  dateRange,
  progress => console.log(`${progress.processedIndices}/${progress.totalIndices}`),
  ['0040', '0041']  // Optional: specific sectors
);
```

## 33 Sector Indices

All JQuants sector indices (0040-0060):
- 水産・農林業, 鉱業, 建設業, 食料品, 繊維製品...
- Complete fetching with `IndicesFetcher`

## Style Indices (6 indices)

| Code | Name |
|------|------|
| 8100 | TOPIX バリュー |
| 8200 | TOPIX グロース |
| 812C | TOPIX500 バリュー |
| 822C | TOPIX500 グロース |
| 812D | TOPIXSmall バリュー |
| 822D | TOPIXSmall グロース |

## Financial Statements

```typescript
import { StatementsClient, StatementsFetcher } from '@trading25/shared/dataset';

const client = new StatementsClient(config);
const statements = await client.getStatements({ code: '7203' });

const fetcher = new StatementsFetcher(config);
const results = await fetcher.fetchStatementsForStocks(
  ['7203', '8411', '9984'],
  { dateRange: { from: '2020-01-01', to: '2024-12-31' } }
);
```
