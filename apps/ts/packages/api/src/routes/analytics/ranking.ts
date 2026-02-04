import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { MarketRankingQuerySchema, MarketRankingResponseSchema } from '../../schemas/market-ranking';
import { MarketRankingService } from '../../services/market/market-ranking-service';
import { createManagedService, createOpenAPIApp, handleRouteError } from '../../utils';

const getMarketRankingService = createManagedService('MarketRankingService', {
  factory: () => new MarketRankingService(),
});

const marketRankingApp = createOpenAPIApp();

/**
 * Get market ranking route
 */
const getMarketRankingRoute = createRoute({
  method: 'get',
  path: '/api/analytics/ranking',
  tags: ['Analytics'],
  summary: 'Get market rankings',
  description:
    'Get market rankings including top stocks by trading value, price gainers, and price losers. Supports single-day or multi-day average lookback.',
  request: {
    query: MarketRankingQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: MarketRankingResponseSchema,
        },
      },
      description: 'Market rankings with trading value, gainers, and losers',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    422: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Database not initialized or no trading data available',
    },
    500: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Internal server error',
    },
  },
});

/**
 * Get market ranking handler
 */
marketRankingApp.openapi(getMarketRankingRoute, async (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);
  const marketRankingService = getMarketRankingService();

  try {
    const rankingData = await marketRankingService.getRankings({
      date: query.date,
      limit: query.limit,
      markets: query.markets,
      lookbackDays: query.lookbackDays,
      periodDays: query.periodDays,
    });

    return c.json(rankingData, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'get market rankings',
      checkDatabaseErrors: true,
      databaseNotReadyMessage: 'Market database not initialized. Please run "bun cli db sync" first.',
      logContext: { date: query.date, markets: query.markets },
      allowedStatusCodes: [400, 422, 500] as const,
    });
  }
});

export default marketRankingApp;
