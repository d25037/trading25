import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { MarketRefreshRequestSchema, MarketRefreshResponseSchema } from '../../schemas/market-refresh';
import { MarketRefreshService } from '../../services/market/market-refresh-service';
import { createErrorResponse, createManagedService, createOpenAPIApp, detectDatabaseError } from '../../utils';

const getMarketRefreshService = createManagedService('MarketRefreshService', {
  factory: () => new MarketRefreshService(),
});

const marketRefreshApp = createOpenAPIApp();

/**
 * POST /api/db/stocks/refresh
 */
const refreshStocksRoute = createRoute({
  method: 'post',
  path: '/api/db/stocks/refresh',
  tags: ['Database'],
  summary: 'Refresh historical data for specific stocks',
  description:
    'Refetch complete historical data for specified stocks to update adjusted prices after stock splits or mergers. Data is filtered to TOPIX date range for consistency.',
  request: {
    body: {
      content: {
        'application/json': {
          schema: MarketRefreshRequestSchema,
        },
      },
    },
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: MarketRefreshResponseSchema,
        },
      },
      description: 'Refresh completed successfully',
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
      description: 'Database not initialized or no TOPIX data available',
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
 * Refresh stocks handler
 */
marketRefreshApp.openapi(refreshStocksRoute, async (c) => {
  const { codes } = c.req.valid('json');
  const correlationId = getCorrelationId(c);
  const marketRefreshService = getMarketRefreshService();

  logger.info('Stock refresh requested', {
    correlationId,
    codes,
    count: codes.length,
  });

  try {
    const result = await marketRefreshService.refreshStocks(codes);
    return c.json(result, 200);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';

    // Check for database-related errors
    const dbError = detectDatabaseError(errorMessage);
    if (dbError.isDatabaseError) {
      logger.warn('Market database not ready for refresh', {
        correlationId,
        error: errorMessage,
      });
      return c.json(
        createErrorResponse({
          error: 'Unprocessable Entity',
          message: 'Market database not initialized. Please run "bun cli market sync" first.',
          correlationId,
        }),
        422
      );
    }

    logger.error('Failed to refresh stocks', {
      correlationId,
      codes,
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

export default marketRefreshApp;
