import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiMarginInterestResponseSchema, MarginInterestQuerySchema } from '../../schemas/margin-interest';
import { StockSymbolParamSchema } from '../../schemas/stock';
import { MarginInterestDataService } from '../../services/margin-interest-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const marginInterestDataService = new MarginInterestDataService();

const marginInterestApp = createOpenAPIApp();

/**
 * Get margin interest route
 */
const getMarginInterestRoute = createRoute({
  method: 'get',
  path: '/api/jquants/stocks/{symbol}/margin-interest',
  tags: ['JQuants Proxy'],
  summary: 'Get weekly margin interest data',
  description: 'Fetch weekly margin interest data for a specific stock with optional date range filtering',
  request: {
    params: StockSymbolParamSchema,
    query: MarginInterestQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiMarginInterestResponseSchema,
        },
      },
      description: 'Margin interest data retrieved successfully',
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
 * Get margin interest handler
 */
marginInterestApp.openapi(getMarginInterestRoute, async (c) => {
  const { symbol } = c.req.valid('param');
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

    const params = { code: symbol, from, to, date };
    const jquantsResponse = await marginInterestDataService.getMarginInterest(params);

    // Transform JQuants response to API response format
    const apiResponse = {
      symbol,
      marginInterest: jquantsResponse.data.map((item) => ({
        date: item.Date,
        code: item.Code,
        shortMarginTradeVolume: item.ShrtVol,
        longMarginTradeVolume: item.LongVol,
      })),
      lastUpdated: new Date().toISOString(),
    };

    return c.json(apiResponse, 200);
  } catch (error) {
    logger.error('Failed to fetch margin interest', {
      correlationId,
      symbol,
      params: { from, to, date },
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch margin interest',
        correlationId,
      }),
      500
    );
  }
});

export default marginInterestApp;
