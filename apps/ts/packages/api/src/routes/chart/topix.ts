import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiTopixDataResponseSchema, TopixQuerySchema } from '../../schemas/topix';
import { TopixDataService } from '../../services/topix-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const topixDataService = new TopixDataService();

const topixApp = createOpenAPIApp();

/**
 * Get TOPIX index data route
 */
const getTopixDataRoute = createRoute({
  method: 'get',
  path: '/api/chart/indices/topix',
  tags: ['Chart'],
  summary: 'Get TOPIX index data (cached)',
  description: 'Fetch historical TOPIX index data optimized for chart display. Cached for 1 hour for performance.',
  request: {
    query: TopixQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiTopixDataResponseSchema,
        },
      },
      description: 'TOPIX data retrieved successfully',
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
      description: 'Unprocessable request (e.g., invalid date range)',
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
 * Get TOPIX index data handler
 */
topixApp.openapi(getTopixDataRoute, async (c) => {
  const { from, to, date } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    // Validate date range logic
    if (from && to && new Date(from) > new Date(to)) {
      return c.json(
        createErrorResponse({
          error: 'Unprocessable Entity',
          message: 'Invalid date range: "from" date must be before or equal to "to" date',
          correlationId,
        }),
        422
      );
    }

    const params = from || to || date ? { from, to, date } : undefined;
    const jquantsResponse = await topixDataService.getTOPIXData(params);

    // Transform JQuants response to API response format
    const apiResponse = {
      topix: jquantsResponse.data.map((item) => ({
        date: item.Date,
        open: item.O,
        high: item.H,
        low: item.L,
        close: item.C,
        volume: 0, // TOPIX data doesn't include volume
      })),
      lastUpdated: new Date().toISOString(),
    };

    return c.json(apiResponse, 200);
  } catch (error) {
    logger.error('Failed to fetch TOPIX data', {
      correlationId,
      params: { from, to, date },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch TOPIX data',
        correlationId,
      }),
      500
    );
  }
});

export default topixApp;
