# OpenAPI Paths

Generated from `apps/ts/packages/shared/openapi/bt-openapi.json`. Do not edit manually.

Total paths: **118**

## /api/analytics

| Path | Methods |
|---|---|
| `/api/analytics/factor-regression/{symbol}` | `GET` |
| `/api/analytics/fundamental-ranking` | `GET` |
| `/api/analytics/fundamentals/{symbol}` | `GET` |
| `/api/analytics/portfolio-factor-regression/{portfolioId}` | `GET` |
| `/api/analytics/ranking` | `GET` |
| `/api/analytics/roe` | `GET` |
| `/api/analytics/screening` | `GET` |
| `/api/analytics/screening/jobs` | `POST` |
| `/api/analytics/screening/jobs/{job_id}` | `GET` |
| `/api/analytics/screening/jobs/{job_id}/cancel` | `POST` |
| `/api/analytics/screening/result/{job_id}` | `GET` |
| `/api/analytics/sector-stocks` | `GET` |
| `/api/analytics/stocks/{symbol}/margin-pressure` | `GET` |
| `/api/analytics/stocks/{symbol}/margin-ratio` | `GET` |

## /api/backtest

| Path | Methods |
|---|---|
| `/api/backtest/attribution-files` | `GET` |
| `/api/backtest/attribution-files/content` | `GET` |
| `/api/backtest/attribution/jobs/{job_id}` | `GET` |
| `/api/backtest/attribution/jobs/{job_id}/cancel` | `POST` |
| `/api/backtest/attribution/jobs/{job_id}/stream` | `GET` |
| `/api/backtest/attribution/result/{job_id}` | `GET` |
| `/api/backtest/attribution/run` | `POST` |
| `/api/backtest/html-files` | `GET` |
| `/api/backtest/html-files/{strategy}/{filename}` | `GET, DELETE` |
| `/api/backtest/html-files/{strategy}/{filename}/rename` | `POST` |
| `/api/backtest/jobs` | `GET` |
| `/api/backtest/jobs/{job_id}` | `GET` |
| `/api/backtest/jobs/{job_id}/cancel` | `POST` |
| `/api/backtest/jobs/{job_id}/stream` | `GET` |
| `/api/backtest/result/{job_id}` | `GET` |
| `/api/backtest/run` | `POST` |

## /api/chart

| Path | Methods |
|---|---|
| `/api/chart/indices` | `GET` |
| `/api/chart/indices/topix` | `GET` |
| `/api/chart/indices/{code}` | `GET` |
| `/api/chart/stocks/search` | `GET` |
| `/api/chart/stocks/{symbol}` | `GET` |

## /api/config

| Path | Methods |
|---|---|
| `/api/config/default` | `GET, PUT` |

## /api/dataset

| Path | Methods |
|---|---|
| `/api/dataset` | `GET, POST` |
| `/api/dataset/jobs/{jobId}` | `GET, DELETE` |
| `/api/dataset/resume` | `POST` |
| `/api/dataset/{name}` | `DELETE` |
| `/api/dataset/{name}/indices` | `GET` |
| `/api/dataset/{name}/indices/{code}` | `GET` |
| `/api/dataset/{name}/info` | `GET` |
| `/api/dataset/{name}/margin` | `GET` |
| `/api/dataset/{name}/margin/batch` | `GET` |
| `/api/dataset/{name}/margin/{code}` | `GET` |
| `/api/dataset/{name}/sample` | `GET` |
| `/api/dataset/{name}/search` | `GET` |
| `/api/dataset/{name}/sectors` | `GET` |
| `/api/dataset/{name}/sectors/mapping` | `GET` |
| `/api/dataset/{name}/sectors/stock-mapping` | `GET` |
| `/api/dataset/{name}/sectors/{sectorName}/stocks` | `GET` |
| `/api/dataset/{name}/statements/batch` | `GET` |
| `/api/dataset/{name}/statements/{code}` | `GET` |
| `/api/dataset/{name}/stocks` | `GET` |
| `/api/dataset/{name}/stocks/ohlcv/batch` | `GET` |
| `/api/dataset/{name}/stocks/{code}/ohlcv` | `GET` |
| `/api/dataset/{name}/topix` | `GET` |

## /api/db

| Path | Methods |
|---|---|
| `/api/db/stats` | `GET` |
| `/api/db/stocks/refresh` | `POST` |
| `/api/db/sync` | `POST` |
| `/api/db/sync/jobs/{jobId}` | `GET, DELETE` |
| `/api/db/validate` | `GET` |

## /api/fundamentals

| Path | Methods |
|---|---|
| `/api/fundamentals/compute` | `POST` |

## /api/health

| Path | Methods |
|---|---|
| `/api/health` | `GET` |

## /api/indicators

| Path | Methods |
|---|---|
| `/api/indicators/compute` | `POST` |
| `/api/indicators/margin` | `POST` |

## /api/jquants

| Path | Methods |
|---|---|
| `/api/jquants/auth/status` | `GET` |
| `/api/jquants/daily-quotes` | `GET` |
| `/api/jquants/indices` | `GET` |
| `/api/jquants/listed-info` | `GET` |
| `/api/jquants/statements` | `GET` |
| `/api/jquants/statements/raw` | `GET` |
| `/api/jquants/stocks/{symbol}/margin-interest` | `GET` |
| `/api/jquants/topix` | `GET` |

## /api/lab

| Path | Methods |
|---|---|
| `/api/lab/evolve` | `POST` |
| `/api/lab/generate` | `POST` |
| `/api/lab/improve` | `POST` |
| `/api/lab/jobs` | `GET` |
| `/api/lab/jobs/{job_id}` | `GET` |
| `/api/lab/jobs/{job_id}/cancel` | `POST` |
| `/api/lab/jobs/{job_id}/stream` | `GET` |
| `/api/lab/optimize` | `POST` |

## /api/market

| Path | Methods |
|---|---|
| `/api/market/stocks` | `GET` |
| `/api/market/stocks/{code}` | `GET` |
| `/api/market/stocks/{code}/ohlcv` | `GET` |
| `/api/market/topix` | `GET` |

## /api/ohlcv

| Path | Methods |
|---|---|
| `/api/ohlcv/resample` | `POST` |

## /api/optimize

| Path | Methods |
|---|---|
| `/api/optimize/grid-configs` | `GET` |
| `/api/optimize/grid-configs/{strategy}` | `GET, PUT, DELETE` |
| `/api/optimize/html-files` | `GET` |
| `/api/optimize/html-files/{strategy}/{filename}` | `GET, DELETE` |
| `/api/optimize/html-files/{strategy}/{filename}/rename` | `POST` |
| `/api/optimize/jobs/{job_id}` | `GET` |
| `/api/optimize/jobs/{job_id}/stream` | `GET` |
| `/api/optimize/run` | `POST` |

## /api/portfolio

| Path | Methods |
|---|---|
| `/api/portfolio` | `GET, POST` |
| `/api/portfolio/{id}` | `GET, PUT, DELETE` |
| `/api/portfolio/{id}/items` | `POST` |
| `/api/portfolio/{id}/items/{itemId}` | `PUT, DELETE` |
| `/api/portfolio/{id}/performance` | `GET` |
| `/api/portfolio/{portfolioName}/codes` | `GET` |
| `/api/portfolio/{portfolioName}/stocks/{code}` | `PUT, DELETE` |

## /api/signals

| Path | Methods |
|---|---|
| `/api/signals/compute` | `POST` |
| `/api/signals/reference` | `GET` |
| `/api/signals/schema` | `GET` |

## /api/strategies

| Path | Methods |
|---|---|
| `/api/strategies` | `GET` |
| `/api/strategies/{strategy_name}` | `GET, PUT, DELETE` |
| `/api/strategies/{strategy_name}/duplicate` | `POST` |
| `/api/strategies/{strategy_name}/move` | `POST` |
| `/api/strategies/{strategy_name}/rename` | `POST` |
| `/api/strategies/{strategy_name}/validate` | `POST` |

## /api/watchlist

| Path | Methods |
|---|---|
| `/api/watchlist` | `GET, POST` |
| `/api/watchlist/{id}` | `GET, PUT, DELETE` |
| `/api/watchlist/{id}/items` | `POST` |
| `/api/watchlist/{id}/items/{itemId}` | `DELETE` |
| `/api/watchlist/{id}/prices` | `GET` |

## /health

| Path | Methods |
|---|---|
| `/health` | `GET` |
