# API Package - OpenAPI-Compliant Hono Server

OpenAPI 3.1 compliant API server with Scalar documentation and Zod validation.

## Architecture

- **Server**: Hono + @hono/zod-openapi + Bun.serve
- **Documentation**: Scalar UI at `http://localhost:3001/doc`
- **Validation**: Zod schemas with runtime validation
- **Middleware**: Request logging, correlation IDs, unified error handling
- **Port**: 3001 (dev and prod)

## Two-Layer Architecture

| Layer | Path | Purpose |
|-------|------|---------|
| 1: JQuants Proxy | `/api/jquants/*` | Raw API data for debugging |
| 2: Chart/Analytics | `/api/chart/*`, `/api/analytics/*` | Optimized production data |

**For detailed endpoints, see the `api-endpoints` skill.**

## Key Endpoints

- `GET /api/chart/stocks/{symbol}` - Chart-ready OHLCV
- `GET /api/analytics/fundamentals/{symbol}` - Fundamental metrics (proxies to apps/bt/ API)
- `GET /api/analytics/factor-regression/{symbol}` - Factor regression
- `GET /api/analytics/portfolio-factor-regression/{portfolioId}` - Portfolio factor regression
- `POST /api/db/sync` - Market data sync
- `POST /api/dataset` - Dataset creation job
- `GET /api/dataset/{name}/statements/{code}` - Financial statements data
- `GET/POST /api/portfolio` - Portfolio CRUD
- `GET/POST /api/watchlist` - Watchlist CRUD + items + prices
- `GET /api/market/stocks/{code}` - Stock info for apps/bt/ (Single Source of Truth)
- `GET /api/market/stocks/{code}/ohlcv` - Stock OHLCV for apps/bt/
- `GET /api/market/topix` - TOPIX data for apps/bt/
- `GET /api/jquants/statements/raw` - Raw statements for apps/bt/ fundamentals

## Structure

```
src/
├── index.ts              # Main app with route mounting
├── schemas/              # Zod schemas for validation
├── routes/               # Two-layer route modules
│   ├── chart/            # Layer 2: Chart endpoints
│   ├── jquants/          # Layer 1: Proxy endpoints
│   ├── analytics/        # Layer 2: Analytics (proxies to apps/bt/ API)
│   ├── market/           # Market data for apps/bt/ (Single Source of Truth)
│   ├── db/               # Database operations
│   ├── dataset/          # Dataset operations
│   ├── portfolio/        # Portfolio CRUD
│   └── watchlist/        # Watchlist CRUD, items, prices
├── services/             # Business logic
├── middleware/           # Logging + error handling
└── utils/                # Shared utilities (validation, error handling, service lifecycle)
```

## Development

```bash
bun run dev                # Watch mode (port 3001)
bun run build              # Build + OpenAPI spec
bun run generate:openapi   # Generate OpenAPI files
bun run test               # Run tests
```

## Production

```bash
bun run start              # Serves API + static web files
```

In production, serves static files from `../dist/client/` for unified deployment.

## Environment Variables

- `JQUANTS_API_KEY` - JQuants API v2 authentication key
- `NODE_ENV` - development/production
- `PORT` - Server port (default: 3001)
