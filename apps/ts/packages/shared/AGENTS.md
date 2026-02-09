# Shared Package - Compatibility Facade

Phase 4A 以降、`@trading25/shared` は主に互換 re-export と補助ユーティリティを提供する薄いファサード。

## Architecture

- **Core**: JQuants API v2 client with API key authentication
- **Database**: SQLite with Drizzle ORM (see `drizzle-orm` skill)
- **Logger**: Node.js specific (separate from frontend)
- **Submodules**: `/dataset`, `/ta`, `/fundamental-analysis`, `/portfolio`, `/watchlist`, `/db`, `/clients/backtest`

## Modules

| Module | Purpose |
|--------|---------|
| `@trading25/shared/dataset` | **互換 re-export** → `@trading25/dataset-db-ts` |
| `@trading25/shared/ta` | Utilities only (Timeframe変換、Relative OHLC、インジケータ計算は全てapps/bt/ API移行完了) |
| `@trading25/shared/fundamental-analysis` | ROE, ROA, PER, PBR, EPS, BPS calculations |
| `@trading25/shared/portfolio` | **互換 re-export** → `@trading25/portfolio-db-ts/portfolio` |
| `@trading25/shared/watchlist` | **互換 re-export** → `@trading25/portfolio-db-ts/watchlist` |
| `@trading25/shared/db` | **互換 re-export** → `@trading25/market-db-ts/db` |
| `@trading25/shared/clients/backtest` | **互換 re-export** → `@trading25/clients-ts/backtest` |

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
