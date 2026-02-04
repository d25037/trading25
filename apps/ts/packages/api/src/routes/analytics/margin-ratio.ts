import { createRoute } from '@hono/zod-openapi';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiMarginVolumeRatioResponseSchema, StockSymbolParamSchema } from '../../schemas/stock';
import { StockDataService } from '../../services/stock-data';
import { createErrorResponse } from '../../utils/error-responses';
import { createOpenAPIApp } from '../../utils/validation-hook';

const stockDataService = new StockDataService();

const marginRatioApp = createOpenAPIApp();

/**
 * Get margin volume ratio route
 */
const getMarginRatioRoute = createRoute({
  method: 'get',
  path: '/api/analytics/stocks/{symbol}/margin-ratio',
  tags: ['Analytics'],
  summary: 'Get margin volume ratio',
  description: 'Fetch calculated margin volume ratio data for trading analysis',
  request: {
    params: StockSymbolParamSchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiMarginVolumeRatioResponseSchema,
        },
      },
      description: 'Margin ratio data retrieved successfully',
    },
    400: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Invalid request parameters',
    },
    404: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Stock symbol not found',
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
 * Get margin volume ratio handler
 */
marginRatioApp.openapi(getMarginRatioRoute, async (c) => {
  const { symbol } = c.req.valid('param');
  const correlationId = getCorrelationId(c);

  try {
    const marginRatioData = await stockDataService.getMarginVolumeRatio(symbol);

    // Check if margin ratio data is empty (symbol not found)
    if (!marginRatioData || (marginRatioData.longRatio.length === 0 && marginRatioData.shortRatio.length === 0)) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Margin ratio data for stock symbol '${symbol}' not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(marginRatioData, 200);
  } catch (error) {
    logger.error('Failed to fetch margin volume ratio data', {
      correlationId,
      symbol,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to fetch margin volume ratio data',
        correlationId,
      }),
      500
    );
  }
});

export default marginRatioApp;
