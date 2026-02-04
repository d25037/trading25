import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { z } from 'zod';
import { ErrorResponseSchema } from '../../schemas/common';
import { TopixRawDataService } from '../../services/topix-raw-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const topixRawDataService = new TopixRawDataService();

const topixRawApp = createOpenAPIApp();

/**
 * TOPIX query schema (same as chart version but no caching)
 */
const TopixRawQuerySchema = z
  .object({
    from: z.string().optional().openapi({
      example: '2024-01-01',
      description: 'Start date (YYYY-MM-DD)',
    }),
    to: z.string().optional().openapi({
      example: '2024-12-31',
      description: 'End date (YYYY-MM-DD)',
    }),
    date: z.string().optional().openapi({
      example: '2024-12-01',
      description: 'Specific date (YYYY-MM-DD)',
    }),
  })
  .openapi('TopixRawQuery');

const ApiTopixRawResponseSchema = z
  .object({
    topix: z.array(
      z.object({
        Date: z.string(),
        Open: z.number().nullable(),
        High: z.number().nullable(),
        Low: z.number().nullable(),
        Close: z.number().nullable(),
      })
    ),
  })
  .openapi('TopixRawResponse');

/**
 * Get TOPIX index data route (raw, no caching)
 */
const getTopixRawDataRoute = createRoute({
  method: 'get',
  path: '/api/jquants/topix',
  tags: ['JQuants Proxy'],
  summary: 'Get TOPIX index data (raw)',
  description: 'Fetch raw TOPIX index data from JQuants API without caching',
  request: {
    query: TopixRawQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiTopixRawResponseSchema,
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
 * Get TOPIX data handler
 */
topixRawApp.openapi(getTopixRawDataRoute, async (c) => {
  const { from, to, date } = c.req.valid('query');
  const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

  try {
    const params: { from?: string; to?: string; date?: string } = {};
    if (from) params.from = from;
    if (to) params.to = to;
    if (date) params.date = date;

    const jquantsResponse = await topixRawDataService.getTOPIX(Object.keys(params).length > 0 ? params : undefined);

    // Transform JQuants v2 response to API response format
    const apiResponse = {
      topix: jquantsResponse.data.map((item) => ({
        Date: item.Date,
        Open: item.O,
        High: item.H,
        Low: item.L,
        Close: item.C,
      })),
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

export default topixRawApp;
