---
name: api-endpoints
description: Trading25 API endpoint reference with two-layer architecture details.
globs: "packages/api/**/*.ts, packages/cli/**/*.ts, packages/web/**/*.ts"
alwaysApply: false
---

# API Endpoints Reference

Scalar docs: `http://localhost:3001/doc`

## Health

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Service health check |

## Layer 1: JQuants Proxy (`/api/jquants/*`)

Raw JQuants API data for debugging and development.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/jquants/daily-quotes` | Raw daily quotes (code, from, to, date) |
| `GET /api/jquants/listed-info` | Raw stock listings (code, date) |
| `GET /api/jquants/indices` | Raw sector indices (code, from, to, date) |
| `GET /api/jquants/topix` | Raw TOPIX data |
| `GET /api/jquants/stocks/{symbol}/margin-interest` | Raw margin data (from, to, date) |
| `GET /api/jquants/statements` | Raw financial statements EPS fields (code) |
| `GET /api/jquants/auth/status` | Check auth status |

## Layer 2: Chart & Analytics (`/api/chart/*`, `/api/analytics/*`)

Optimized, chart-ready data for production.

### Chart Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /api/chart/stocks/search` | Stock search |
| `GET /api/chart/stocks/{symbol}` | OHLCV data (timeframe: daily/weekly/monthly) |
| `GET /api/chart/indices/topix` | TOPIX chart data (1-hour cache) |
| `GET /api/chart/indices` | List available indices |
| `GET /api/chart/indices/{code}` | Index chart data |

### Analytics Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /api/analytics/roe` | ROE calculation (code, date, sortBy, limit) |
| `GET /api/analytics/ranking` | Market rankings (date, limit, markets, lookbackDays) |
| `GET /api/analytics/screening` | Stock screening (markets, enableRangeBreakFast/Slow) |
| `GET /api/analytics/stocks/{symbol}/margin-ratio` | Margin volume ratio |
| `GET /api/analytics/stocks/{symbol}/margin-pressure` | Margin pressure indicators |
| `GET /api/analytics/fundamentals/{symbol}` | Fundamental metrics time-series |
| `GET /api/analytics/factor-regression/{symbol}` | Two-stage factor regression |
| `GET /api/analytics/portfolio-factor-regression/{portfolioId}` | Portfolio factor regression |
| `GET /api/analytics/sector-stocks` | Sector stocks with trading data (sector33, sector17, sortBy) |

### Database Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /api/db/validate` | Validate market database |
| `GET /api/db/stats` | Database statistics |
| `POST /api/db/sync` | Trigger market sync (async job) |
| `GET /api/db/sync/jobs/{jobId}` | Get sync job status |
| `DELETE /api/db/sync/jobs/{jobId}` | Cancel sync job |
| `POST /api/db/stocks/refresh` | Refresh specific stocks |

### Dataset Endpoints (Job-based async)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/dataset` | List all datasets (lightweight scan) |
| `POST /api/dataset` | Start dataset creation job |
| `POST /api/dataset/resume` | Resume incomplete dataset |
| `DELETE /api/dataset/{name}` | Delete dataset file |
| `GET /api/dataset/jobs/{jobId}` | Get job status |
| `DELETE /api/dataset/jobs/{jobId}` | Cancel job |
| `GET /api/dataset/{name}/info` | Dataset information |
| `GET /api/dataset/{name}/sample` | Sample stocks |
| `GET /api/dataset/{name}/search` | Search stocks |
| `GET /api/dataset/{name}/stocks` | List stocks in dataset |
| `GET /api/dataset/{name}/stocks/{code}/ohlcv` | Stock OHLCV data |
| `GET /api/dataset/{name}/stocks/ohlcv/batch` | Batch OHLCV (codes, timeframe) max 100 |
| `GET /api/dataset/{name}/topix` | TOPIX data |
| `GET /api/dataset/{name}/indices` | Index list |
| `GET /api/dataset/{name}/indices/{code}` | Index data |
| `GET /api/dataset/{name}/margin` | Margin data list |
| `GET /api/dataset/{name}/margin/{code}` | Stock margin data |
| `GET /api/dataset/{name}/margin/batch` | Batch margin data (codes) max 100 |
| `GET /api/dataset/{name}/statements/{code}` | Financial statements (period_type, actual_only) |
| `GET /api/dataset/{name}/statements/batch` | Batch statements (codes, period_type, actual_only) max 100 |
| `GET /api/dataset/{name}/sectors` | List sectors |
| `GET /api/dataset/{name}/sectors/mapping` | Sector-Index mapping |
| `GET /api/dataset/{name}/sectors/stock-mapping` | Stock-Sector mapping |
| `GET /api/dataset/{name}/sectors/{sectorName}/stocks` | Stocks in sector |

### Market Data Endpoints (Python API clients)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/market/stocks` | All stocks with OHLCV (market, history_days) |
| `GET /api/market/stocks/{code}/ohlcv` | Stock OHLCV data |
| `GET /api/market/topix` | TOPIX data |

## Portfolio Management (`/api/portfolio/*`)

**ID-based** (programmatic):

| Endpoint | Purpose |
|----------|---------|
| `GET /api/portfolio` | List portfolios |
| `POST /api/portfolio` | Create portfolio |
| `GET /api/portfolio/{id}` | Get portfolio |
| `PUT /api/portfolio/{id}` | Update portfolio |
| `DELETE /api/portfolio/{id}` | Delete portfolio |
| `POST /api/portfolio/{id}/items` | Add stock |
| `PUT /api/portfolio/{id}/items/{itemId}` | Update item |
| `DELETE /api/portfolio/{id}/items/{itemId}` | Remove item |
| `GET /api/portfolio/{id}/performance` | Performance with benchmark (lookbackDays, benchmarkCode) |
| `GET /api/analytics/portfolio-factor-regression/{portfolioId}` | Portfolio factor regression |

**Name+Code-based** (CLI):

| Endpoint | Purpose |
|----------|---------|
| `PUT /api/portfolio/{portfolioName}/stocks/{code}` | Add/update stock by name+code |
| `DELETE /api/portfolio/{portfolioName}/stocks/{code}` | Remove stock by name+code |
| `GET /api/portfolio/{name}/codes` | Get stock codes (Python API) |

## Watchlist Management (`/api/watchlist/*`)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/watchlist` | List watchlists |
| `POST /api/watchlist` | Create watchlist |
| `GET /api/watchlist/{id}` | Get watchlist |
| `PUT /api/watchlist/{id}` | Update watchlist |
| `DELETE /api/watchlist/{id}` | Delete watchlist |
| `POST /api/watchlist/{id}/items` | Add stock to watchlist |
| `DELETE /api/watchlist/{id}/items/{itemId}` | Remove stock from watchlist |
| `GET /api/watchlist/{id}/prices` | Get watchlist stock prices |

## Error Response Format

```json
{
  "status": "error",
  "error": "Bad Request | Not Found | Unprocessable Entity | Internal Server Error",
  "message": "Human-readable description",
  "details": [{ "field": "symbol", "message": "Must be 4 characters" }],
  "timestamp": "2024-01-01T12:00:00.000Z",
  "correlationId": "uuid"
}
```

**Status Codes**: 200, 400, 404, 409 (Conflict), 422, 500

## Layer Selection Guide

| Scenario | Layer 1 (Proxy) | Layer 2 (Chart/Analytics) |
|----------|-----------------|---------------------------|
| Development/Debug | Use | - |
| Production Web UI | - | Use |
| CLI Tools | Debug only | Preferred |
| Data Verification | Use | - |
| Performance Critical | - | Use |

## Zod/OpenAPI Pattern

```typescript
import { z } from '@hono/zod-openapi';  // Always from @hono/zod-openapi
import { createErrorResponse } from '../utils/error-responses';
import { getCorrelationId } from '../../middleware/correlation';

const MySchema = z.object({
  field: z.string(),
}).openapi('MySchema');

// Error handling
return c.json(createErrorResponse({
  error: 'Not Found',
  message: 'Resource not found',
  correlationId: getCorrelationId(c),
}), 404);
```
