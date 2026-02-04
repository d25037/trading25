import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import { ApiMarginPressureIndicatorsResponseSchema, MarginPressureQuerySchema } from '../../schemas/margin-pressure';
import { StockSymbolParamSchema } from '../../schemas/stock';
import { StockDataService } from '../../services/stock-data';
import { createErrorResponse, createOpenAPIApp, handleRouteError } from '../../utils';

const stockDataService = new StockDataService();

const marginPressureApp = createOpenAPIApp();

/**
 * Get margin pressure indicators route
 */
const getMarginPressureRoute = createRoute({
  method: 'get',
  path: '/api/analytics/stocks/{symbol}/margin-pressure',
  tags: ['Analytics'],
  summary: 'Get margin pressure indicators',
  description: 'Fetch margin long pressure, flow pressure, and turnover days indicators for trading analysis',
  request: {
    params: StockSymbolParamSchema,
    query: MarginPressureQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: ApiMarginPressureIndicatorsResponseSchema,
        },
      },
      description: 'Margin pressure indicators retrieved successfully',
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
 * Get margin pressure indicators handler
 */
marginPressureApp.openapi(getMarginPressureRoute, async (c) => {
  const { symbol } = c.req.valid('param');
  const { period } = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const indicatorsData = await stockDataService.getMarginPressureIndicators(symbol, period);

    // Check if data is empty (symbol not found)
    if (
      !indicatorsData ||
      (indicatorsData.longPressure.length === 0 &&
        indicatorsData.flowPressure.length === 0 &&
        indicatorsData.turnoverDays.length === 0)
    ) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `Margin pressure data for stock symbol '${symbol}' not found`,
          correlationId,
        }),
        404
      );
    }

    return c.json(indicatorsData, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'fetch margin pressure indicators',
      logContext: { symbol, period },
      allowedStatusCodes: [400, 404, 500] as const,
    });
  }
});

export default marginPressureApp;
