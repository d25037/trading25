import { BacktestApiError, BacktestClient } from '@trading25/shared/clients/backtest';
import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  FundamentalsPathParamsSchema,
  FundamentalsQuerySchema,
  FundamentalsResponseSchema,
} from '../../schemas/fundamentals';
import { createErrorResponse, createOpenAPIApp, handleRouteError } from '../../utils';

const fundamentalsApp = createOpenAPIApp();

/**
 * Get fundamentals analysis route
 */
const getFundamentalsRoute = createRoute({
  method: 'get',
  path: '/api/analytics/fundamentals/{symbol}',
  tags: ['Analytics'],
  summary: 'Get fundamental analysis metrics for a stock',
  description:
    'Fetch financial statements from JQuants API and calculate fundamental metrics (ROE, PER, PBR, EPS, BPS, ROA, Operating Margin, Net Margin) for a specific stock. PER and PBR are calculated using stock prices at statement disclosure dates.',
  request: {
    params: FundamentalsPathParamsSchema,
    query: FundamentalsQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: FundamentalsResponseSchema,
        },
      },
      description: 'Fundamental analysis results with time series data',
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
      description: 'Stock not found',
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
 * Get fundamentals analysis handler
 *
 * Proxies to apps/bt/ API for fundamental calculations (Single Source of Truth).
 */
fundamentalsApp.openapi(getFundamentalsRoute, async (c) => {
  const { symbol } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const btClient = new BacktestClient();
    const fundamentalsData = await btClient.computeFundamentals({
      symbol,
      from_date: query.from,
      to_date: query.to,
      period_type: query.periodType,
      prefer_consolidated: query.preferConsolidated,
    });

    // Check if any data was found
    if (fundamentalsData.data.length === 0) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: `No financial statements found for stock ${symbol}`,
          correlationId,
        }),
        404
      );
    }

    return c.json(fundamentalsData, 200);
  } catch (error) {
    // Handle apps/bt/ API errors - preserve status codes
    if (error instanceof BacktestApiError) {
      switch (error.status) {
        case 404:
          return c.json(
            createErrorResponse({
              error: 'Not Found',
              message: `Stock ${symbol} not found`,
              correlationId,
            }),
            404
          );
        case 400:
          return c.json(
            createErrorResponse({
              error: 'Bad Request',
              message: error.message,
              correlationId,
            }),
            400
          );
      }
    }

    return handleRouteError(c, error, correlationId, {
      operationName: 'get fundamentals',
      logContext: { symbol },
      allowedStatusCodes: [400, 404, 500] as const,
    });
  }
});

export default fundamentalsApp;
