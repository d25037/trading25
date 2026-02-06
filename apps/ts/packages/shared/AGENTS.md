# Shared Package - Core Business Logic

JQuants API client, SQLite dataset management, technical analysis, and fundamental metrics.

## Architecture

- **Core**: JQuants API v2 client with API key authentication
- **Database**: SQLite with Drizzle ORM (see `drizzle-orm` skill)
- **Logger**: Node.js specific (separate from frontend)
- **Submodules**: `/dataset`, `/ta`, `/core`, `/fundamental-analysis`, `/factor-regression`, `/screening`

## Modules

| Module | Purpose |
|--------|---------|
| `@trading25/shared/dataset` | Dataset builder, reader, fetchers |
| `@trading25/shared/ta` | Utilities only (Timeframe変換、Relative OHLC、インジケータ計算は全てapps/bt/ API移行完了) |
| `@trading25/shared/fundamental-analysis` | ROE, ROA, PER, PBR, EPS, BPS calculations |
| `@trading25/shared/factor-regression` | Two-stage factor regression for risk decomposition |
| `@trading25/shared/screening` | Range Break detection algorithms |
| `@trading25/shared/portfolio` | Portfolio CRUD operations |
| `@trading25/shared/watchlist` | Watchlist management (lightweight stock monitoring) |
| `@trading25/shared/market-sync` | Market data sync and rankings |
| `@trading25/shared/db` | Drizzle ORM schemas and operations |
| `@trading25/shared/clients/backtest` | Backtest API client + Lab types for trading25-bt backend |

## API Authentication

JQuants API v2 uses API key authentication via `x-api-key` header.
Set `JQUANTS_API_KEY` environment variable.

## Technical Analysis

全TA機能はapps/bt/ APIに移行完了（Phase 4.3）:
- **インジケータ計算**: apps/bt/ API (`POST /api/indicators/compute`)
- **Timeframe変換**: apps/bt/ API (`POST /api/ohlcv/resample`)
- **Relative OHLC**: apps/bt/ API (`POST /api/ohlcv/resample` with `benchmark_code`)

apps/ts/shared/ta/ には以下のユーティリティのみ残存:
- **Utilities** (`cleanNaNValues`) — 汎用ユーティリティ

```typescript
import { cleanNaNValues } from '@trading25/shared/ta';
```

## Fundamentals Analysis

Fundamentals計算もapps/bt/ APIに移行完了（Single Source of Truth原則）:
- **Fundamentals計算**: apps/bt/ API (`POST /api/fundamentals/compute`)

```typescript
import { BacktestClient } from '@trading25/shared/clients/backtest';
const client = new BacktestClient();
const data = await client.computeFundamentals({ symbol: '7203' });
```

## Test Utilities

```typescript
// Use type guards instead of non-null assertions
import { getFirstElementOrFail } from '@trading25/shared/test-utils';
const first = getFirstElementOrFail(array, 'Expected element');
```

## Skills Reference

- **Dataset details**: `dataset-management` skill
- **JQuants API optimization**: `jquants-api-optimization` skill
- **Financial analysis**: `financial-analysis` skill
- **Portfolio operations**: `portfolio-management` skill
- **Database schema**: `drizzle-orm` skill

## Development

```bash
bun run build        # TypeScript compilation
bun test             # Run tests
bun run typecheck    # Type validation
```
