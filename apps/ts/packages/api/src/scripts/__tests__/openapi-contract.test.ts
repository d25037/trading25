import { beforeAll, describe, expect, test } from 'bun:test';

interface OpenAPISpec {
  paths?: Record<string, unknown>;
  components?: { schemas?: Record<string, unknown> };
}

async function generateFreshSpec(): Promise<OpenAPISpec> {
  const { OpenAPIHono } = await import('@hono/zod-openapi');
  const { mountAllRoutes } = await import('../../app-routes');
  const { openapiConfig } = await import('../../openapi/config');
  const app = new OpenAPIHono();
  mountAllRoutes(app);
  return app.getOpenAPIDocument(openapiConfig) as unknown as OpenAPISpec;
}

describe('OpenAPI Contract Tests', () => {
  let spec: OpenAPISpec;

  beforeAll(async () => {
    spec = await generateFreshSpec();
  });

  describe('Schema completeness', () => {
    const expectedSchemaNames = [
      // Ranking
      'RankingItem',
      'Rankings',
      'MarketRankingResponse',
      // Screening
      'RangeBreakDetails',
      'FuturePricePoint',
      'FutureReturns',
      'ScreeningResultItem',
      'ScreeningSummary',
      'MarketScreeningResponse',
      // Sync
      'JobProgress',
      'SyncJobResult',
      'CreateSyncJobResponse',
      'SyncJobResponse',
      'CancelJobResponse',
      // Dataset
      'DatasetListItem',
      'DatasetListResponse',
      'DatasetDeleteResponse',
      'DatasetJobProgress',
      'DatasetJobResponse',
      'DatasetInfoResponse',
      'CancelDatasetJobResponse',
      // Validation
      'AdjustmentEvent',
      'IntegrityIssue',
      'MarketValidationResponse',
      // Health & Error
      'HealthResponse',
      'ErrorResponse',
      // Fundamentals
      'FundamentalsResponse',
      // Factor Regression
      'FactorRegressionResponse',
      // ROE
      'ROEResponse',
      // Portfolio
      'PortfolioResponse',
      'PortfolioPerformanceResponse',
      // Watchlist
      'WatchlistResponse',
      'WatchlistPricesResponse',
    ];

    for (const name of expectedSchemaNames) {
      test(`schema "${name}" exists in components.schemas`, () => {
        const schemas = spec.components?.schemas;
        expect(schemas).toBeDefined();
        expect(schemas).toHaveProperty(name);
      });
    }
  });

  describe('Path completeness', () => {
    const expectedPaths = [
      '/health',
      // JQuants proxy
      '/api/jquants/auth/status',
      '/api/jquants/daily-quotes',
      '/api/jquants/indices',
      '/api/jquants/listed-info',
      '/api/jquants/topix',
      // Chart
      '/api/chart/stocks/{symbol}',
      '/api/chart/indices',
      '/api/chart/indices/topix',
      // Analytics
      '/api/analytics/ranking',
      '/api/analytics/screening',
      '/api/analytics/roe',
      '/api/analytics/fundamentals/{symbol}',
      '/api/analytics/factor-regression/{symbol}',
      '/api/analytics/portfolio-factor-regression/{portfolioId}',
      '/api/analytics/sector-stocks',
      '/api/analytics/stocks/{symbol}/margin-pressure',
      '/api/analytics/stocks/{symbol}/margin-ratio',
      // Database
      '/api/db/sync',
      '/api/db/validate',
      '/api/db/stats',
      // Dataset
      '/api/dataset',
      '/api/dataset/{name}',
      '/api/dataset/{name}/info',
      // Portfolio
      '/api/portfolio',
      '/api/portfolio/{id}',
      '/api/portfolio/{id}/performance',
      // Watchlist
      '/api/watchlist',
      '/api/watchlist/{id}',
      '/api/watchlist/{id}/prices',
      // Market data
      '/api/market/stocks',
      '/api/market/topix',
    ];

    for (const path of expectedPaths) {
      test(`path "${path}" exists in spec`, () => {
        const paths = spec.paths;
        expect(paths).toBeDefined();
        expect(paths).toHaveProperty(path);
      });
    }
  });
});
