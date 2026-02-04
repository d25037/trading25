import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { SectorStocksQuerySchema, SectorStocksResponseSchema } from '../../schemas/sector-stocks';
import { SectorStocksService } from '../../services/market/sector-stocks-service';
import { createManagedService, createOpenAPIApp, handleRouteError } from '../../utils';

const getSectorStocksService = createManagedService('SectorStocksService', {
  factory: () => new SectorStocksService(),
});

const sectorStocksApp = createOpenAPIApp();

/**
 * Get sector stocks route
 */
const getSectorStocksRoute = createRoute({
  method: 'get',
  path: '/api/analytics/sector-stocks',
  tags: ['Analytics'],
  summary: 'Get stocks by sector',
  description:
    'Get stocks filtered by sector (sector33 or sector17) with trading data, sorted by trading value or price change percentage.',
  request: {
    query: SectorStocksQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: SectorStocksResponseSchema,
        },
      },
      description: 'Sector stocks with trading data',
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
 * Get sector stocks handler
 */
sectorStocksApp.openapi(getSectorStocksRoute, async (c) => {
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);
  const sectorStocksService = getSectorStocksService();

  try {
    const result = await sectorStocksService.getStocks({
      sector33Name: query.sector33Name,
      sector17Name: query.sector17Name,
      markets: query.markets,
      lookbackDays: query.lookbackDays,
      sortBy: query.sortBy,
      sortOrder: query.sortOrder,
      limit: query.limit,
    });

    return c.json(result, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'get sector stocks',
      checkDatabaseErrors: true,
      databaseNotReadyMessage: 'Market database not initialized. Please run "bun cli db sync" first.',
      logContext: { sector33Name: query.sector33Name, sector17Name: query.sector17Name },
      allowedStatusCodes: [400, 422, 500] as const,
    });
  }
});

export default sectorStocksApp;
