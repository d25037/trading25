# Trading25

Trading25 TypeScript monorepo for financial data analysis with strict TypeScript/Biome compliance.

## Architecture

- **Web** (`packages/web/`): React 19 + Vite + Tailwind CSS v4 + lightweight-charts
- **API** (`packages/api/`): OpenAPI 3.1 Hono server with Scalar docs
- **Shared** (`packages/shared/`): JQuants API, SQLite + Drizzle ORM, TA/FA modules
- **CLI** (`packages/cli/`): Gunshi CLI for dataset/portfolio/analysis

## Features

### Market Data & Storage
- **SQLite Database**: XDG-compliant storage with Drizzle ORM
- **JQuants API v2**: API key authentication, rate-limited fetching
- **Market Data Sync**: Automated data collection with progress tracking
- **Dataset Builder**: Create filtered datasets with preset configurations

### Technical Analysis
- **Indicators**: SMA, EMA, MACD, PPO, RSI, ATR, Bollinger Bands
- **Timeframe Conversion**: Daily to weekly/monthly OHLC aggregation

### Fundamental Analysis
- **Metrics**: ROE, ROA, PER, PBR, EPS, BPS calculations
- **Factor Regression**: Multi-factor analysis for stocks and portfolios
- **Screening**: Range Break detection algorithms
- **Rankings**: Market-wide stock rankings

### Portfolio Management
- **CRUD Operations**: Create, update, delete portfolios
- **Stock Holdings**: Add/remove stocks with quantity and price tracking
- **Portfolio Analysis**: Factor regression for entire portfolios

### Code Quality
- **Strict TypeScript**: No any types, strict null checks
- **Biome Compliance**: Consistent linting/formatting
- **Type-Safe Testing**: Array helpers, no non-null assertions

## Quick Start

```bash
# Install dependencies
bun install

# Development (concurrent web + api)
bun dev

# Build all packages
bun build

# Run tests
bun test

# Code quality
bun lint && bun check:fix

# TypeScript validation
bun typecheck:all
```

## Individual Package Development

```bash
# Web development (Vite, port 5173)
bun dev:web

# API development (Hono, port 3001)
bun dev:api

# CLI development
bun dev:cli
```

## CLI Usage

```bash
# Database operations
bun cli db sync              # Sync market data from JQuants
bun cli db validate          # Validate data integrity
bun cli db refresh           # Refresh data

# Dataset management (requires API server)
bun cli dataset create prime.db --preset primeMarket
bun cli dataset info prime.db
bun cli dataset validate prime.db

# Analysis
bun cli analysis roe 7203                    # ROE analysis for stock
bun cli analysis ranking --limit 20          # Top stocks ranking
bun cli analysis screening                   # Range break screening
bun cli analysis factor-regression 7203      # Factor analysis
bun cli analysis portfolio-factor-regression 1  # Portfolio factor analysis

# Portfolio
bun cli portfolio create "My Portfolio"
bun cli portfolio list
bun cli portfolio show "My Portfolio"
bun cli portfolio add-stock "My Portfolio" 7203 --quantity 100 --price 2500
bun cli portfolio remove-stock "My Portfolio" 7203
```

## API Endpoints

| Layer | Path | Purpose |
|-------|------|---------|
| JQuants Proxy | `/api/jquants/*` | Raw API data for debugging |
| Chart | `/api/chart/stocks/{symbol}` | Chart-ready OHLCV data |
| Analytics | `/api/analytics/fundamentals/{symbol}` | Fundamental metrics |
| Analytics | `/api/analytics/factor-regression/{symbol}` | Factor regression |
| Database | `/api/db/sync` | Market data sync |
| Dataset | `/api/dataset` | Dataset creation |
| Portfolio | `/api/portfolio` | Portfolio CRUD |

API documentation available at `http://localhost:3001/doc` (Scalar UI).

## Technology Stack

- **Runtime**: Bun + TypeScript (strict mode)
- **Web**: React 19 + Vite 7 + Tailwind CSS v4 + TanStack Query + Zustand
- **API**: Hono + @hono/zod-openapi + Zod v4 + Scalar
- **Data**: SQLite (bun:sqlite) + Drizzle ORM + JQuants API v2
- **CLI**: Gunshi + Chalk + Ora
- **Quality**: Biome 2.1 (linting/formatting)
- **Testing**: Bun test (backend) + Vitest (web)

## Environment Variables

Bun automatically loads `.env` from project root.

```
JQUANTS_API_KEY         # JQuants API key (v2 API)
JQUANTS_PLAN            # Required: free, light, standard, premium
LOG_LEVEL               # debug, info, warn, error
NODE_ENV                # development, production
```

## XDG-Compliant Data Paths

- **Market DB**: `$HOME/.local/share/trading25/market.db`
- **Datasets**: `$HOME/.local/share/trading25/datasets/`
- **Portfolio**: `$HOME/.local/share/trading25/portfolio.db`

Customize with `XDG_DATA_HOME` environment variable.

## Code Usage Examples

### Technical Analysis

```typescript
// TA計算は apps/bt/ API (POST /api/indicators/compute) に移行済み
// apps/ts/shared/ta にはtimeframe変換とrelativeMode用のインジケータが残存
import { sma, ema, macd, atr, bollingerBands, dailyToWeekly } from '@trading25/shared/ta';

const sma20 = sma(prices, 20);
const atr14 = atr(ohlcData, 14);
const bb = bollingerBands(prices, 20, 2.0); // { upper, middle, lower }
const weekly = dailyToWeekly(dailyOHLC);
```

### Type-Safe Patterns

```typescript
// Use type guards instead of non-null assertions
import { getFirstElementOrFail } from '@trading25/shared/test-utils';
const first = getFirstElementOrFail(array, 'Expected element');
```

For detailed development guidelines, see [CLAUDE.md](./CLAUDE.md).
