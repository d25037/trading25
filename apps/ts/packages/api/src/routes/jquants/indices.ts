import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiIndicesResponseSchema, IndicesQuerySchema } from '../../schemas/indices';
import { IndicesDataService } from '../../services/indices-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const indicesDataService = new IndicesDataService();

const indicesApp = createOpenAPIApp();

/**
 * Get indices route
 */
const getIndicesRoute = createRoute({
  method: 'get',
  path: '/api/jquants/indices',
  tags: ['JQuants Proxy'],
  summary: 'Get index data',
  description: 'Fetch historical index data with optional filtering by code or date range',
  request: {
    query: IndicesQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiIndicesResponseSchema,
        },
      },
      description: 'Indices data retrieved successfully',
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
 * Get indices handler
 */
indicesApp.openapi(getIndicesRoute, async (c) => {
  const { code, from, to, date } = c.req.valid('query');
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

    const params = code || from || to || date ? { code, from, to, date } : undefined;
    const jquantsResponse = await indicesDataService.getIndices(params);

    // Transform JQuants response to API response format
    // Filter out items with null values for OHLC data
    const apiResponse = {
      indices: jquantsResponse.data
        .filter((item) => item.O !== null && item.H !== null && item.L !== null && item.C !== null)
        .map((item) => ({
          date: item.Date,
          code: item.Code,
          open: item.O as number,
          high: item.H as number,
          low: item.L as number,
          close: item.C as number,
        })),
      lastUpdated: new Date().toISOString(),
    };

    return c.json(apiResponse, 200);
  } catch (error) {
    logger.error('Failed to fetch indices', {
      correlationId,
      params: { code, from, to, date },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch indices',
        correlationId,
      }),
      500
    );
  }
});

export default indicesApp;
