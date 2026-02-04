import type { OpenAPIHono } from '@hono/zod-openapi';
import factorRegressionApp from './routes/analytics/factor-regression';
import fundamentalsApp from './routes/analytics/fundamentals';
import marginPressureApp from './routes/analytics/margin-pressure';
import marginRatioApp from './routes/analytics/margin-ratio';
import portfolioFactorRegressionApp from './routes/analytics/portfolio-factor-regression';
import rankingApp from './routes/analytics/ranking';
import roeApp from './routes/analytics/roe';
import screeningApp from './routes/analytics/screening';
import sectorStocksApp from './routes/analytics/sector-stocks';
import chartIndicesApp from './routes/chart/indices';
import chartStocksApp from './routes/chart/stocks';
import chartTopixApp from './routes/chart/topix';
import datasetApp from './routes/dataset';
import datasetDataApp from './routes/dataset/data';
import dbRefreshApp from './routes/db/refresh';
import dbStatsApp from './routes/db/stats';
import dbSyncApp from './routes/db/sync';
import dbValidateApp from './routes/db/validate';
import healthApp from './routes/health';
import authApp from './routes/jquants/auth';
import dailyQuotesApp from './routes/jquants/daily-quotes';
import indicesApp from './routes/jquants/indices';
import listedInfoApp from './routes/jquants/listed-info';
import marginInterestApp from './routes/jquants/margin-interest';
import statementsApp from './routes/jquants/statements';
import topixRawApp from './routes/jquants/topix-raw';
import marketDataApp from './routes/market/data';
import portfolioApp from './routes/portfolio';
import portfolioPerformanceApp from './routes/portfolio/performance';
import watchlistApp from './routes/watchlist';

/**
 * Mount all API routes onto the given Hono app.
 * Used by both the main server (index.ts) and OpenAPI spec generation.
 */
export function mountAllRoutes(app: OpenAPIHono): void {
  app.route('/', healthApp);

  // JQuants proxy endpoints
  app.route('/', authApp);
  app.route('/', dailyQuotesApp);
  app.route('/', indicesApp);
  app.route('/', listedInfoApp);
  app.route('/', marginInterestApp);
  app.route('/', statementsApp);
  app.route('/', topixRawApp);

  // Chart data services
  app.route('/', chartIndicesApp);
  app.route('/', chartStocksApp);
  app.route('/', chartTopixApp);

  // Analytics services
  app.route('/', factorRegressionApp);
  app.route('/', fundamentalsApp);
  app.route('/', marginPressureApp);
  app.route('/', marginRatioApp);
  app.route('/', portfolioFactorRegressionApp);
  app.route('/', rankingApp);
  app.route('/', roeApp);
  app.route('/', screeningApp);
  app.route('/', sectorStocksApp);

  // Portfolio management
  app.route('/', portfolioApp);
  app.route('/', portfolioPerformanceApp);

  // Watchlist management
  app.route('/', watchlistApp);

  // Database management
  app.route('/', dbRefreshApp);
  app.route('/', dbStatsApp);
  app.route('/', dbSyncApp);
  app.route('/', dbValidateApp);

  // Dataset management
  app.route('/', datasetApp);
  app.route('/', datasetDataApp);

  // Market data (for Python API clients)
  app.route('/', marketDataApp);
}
