import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { MarketScreeningQuerySchema, MarketScreeningResponseSchema } from '../../schemas/market-screening';
import { MarketScreeningService } from '../../services/market/market-screening-service';
import { createManagedService, createOpenAPIApp, handleRouteError } from '../../utils';

const getMarketScreeningService = createManagedService('MarketScreeningService', {
  factory: () => new MarketScreeningService(),
});

const marketScreeningApp = createOpenAPIApp();

/**
 * Get market screening route
 */
const getMarketScreeningRoute = createRoute({
  method: 'get',
  path: '/api/analytics/screening',
  tags: ['Analytics'],
  summary: 'Run stock screening',
  description:
    'Run stock screening analysis on market-wide data. Supports Range Break Fast and Range Break Slow strategies with volume validation.',
  request: {
    query: MarketScreeningQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: MarketScreeningResponseSchema,
        },
      },
      description: 'Screening results with summary statistics',
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
      description: 'Database not initialized or no data available',
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
 * Get market screening handler
 */
marketScreeningApp.openapi(getMarketScreeningRoute, async (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);
  const marketScreeningService = getMarketScreeningService();

  try {
    const screeningData = await marketScreeningService.runScreening({
      markets: query.markets,
      rangeBreakFast: query.rangeBreakFast,
      rangeBreakSlow: query.rangeBreakSlow,
      recentDays: query.recentDays,
      referenceDate: query.date,
      minBreakPercentage: query.minBreakPercentage,
      minVolumeRatio: query.minVolumeRatio,
      sortBy: query.sortBy,
      order: query.order,
      limit: query.limit,
    });

    return c.json(screeningData, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'run market screening',
      checkDatabaseErrors: true,
      databaseNotReadyMessage: 'Market database not initialized. Please run "bun cli db sync" first.',
      logContext: { markets: query.markets },
      allowedStatusCodes: [400, 422, 500] as const,
    });
  }
});

export default marketScreeningApp;
