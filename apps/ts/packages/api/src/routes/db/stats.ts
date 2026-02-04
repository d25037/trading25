import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { MarketStatsResponseSchema } from '../../schemas/market-stats';
import { MarketStatsService } from '../../services/market/market-stats-service';
import { createErrorResponse, createManagedService, createOpenAPIApp, detectDatabaseError } from '../../utils';

const getMarketStatsService = createManagedService('MarketStatsService', {
  factory: () => new MarketStatsService(),
});

const marketStatsApp = createOpenAPIApp();

/**
 * Get database stats route
 */
const getMarketStatsRoute = createRoute({
  method: 'get',
  path: '/api/db/stats',
  tags: ['Database'],
  summary: 'Get market database statistics',
  description:
    'Get statistics about the market database including TOPIX, stocks, stock data, and indices counts and date ranges.',
  responses: {
    200: {
      content: {
        'application/json': {
          schema: MarketStatsResponseSchema,
        },
      },
      description: 'Market database statistics',
    },
    422: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Database not found or cannot be opened',
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
 * Get market stats handler
 */
marketStatsApp.openapi(getMarketStatsRoute, async (c) => {
  const correlationId = getCorrelationId(c);
  const marketStatsService = getMarketStatsService();

  try {
    const stats = await marketStatsService.getStats();
    return c.json(stats, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';

    // Check for database-related errors
    const dbError = detectDatabaseError(errorMessage);
    if (dbError.isDatabaseError) {
      logger.warn('Market database not found or corrupted', {
        correlationId,
        error: errorMessage,
      });
      return c.json(
        createErrorResponse({
          error: 'Unprocessable Entity',
          message: 'Market database not found. Please run "bun cli db sync --init" first.',
          correlationId,
        }),
        422
      );
    }

    logger.error('Failed to get market database stats', {
      correlationId,
      error: errorMessage,
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: errorMessage,
        correlationId,
      }),
      500
    );
  }
});

export default marketStatsApp;
