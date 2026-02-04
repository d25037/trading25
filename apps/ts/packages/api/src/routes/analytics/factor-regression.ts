import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  FactorRegressionPathParamsSchema,
  FactorRegressionQuerySchema,
  FactorRegressionResponseSchema,
} from '../../schemas/factor-regression';
import { FactorRegressionService } from '../../services/factor-regression-service';
import { createManagedService, createOpenAPIApp, handleRouteError } from '../../utils';

const getFactorRegressionService = createManagedService('FactorRegressionService', {
  factory: () => new FactorRegressionService(),
});

const factorRegressionApp = createOpenAPIApp();

/**
 * Get factor regression analysis route
 */
const getFactorRegressionRoute = createRoute({
  method: 'get',
  path: '/api/analytics/factor-regression/{symbol}',
  tags: ['Analytics'],
  summary: 'Analyze stock risk factors via OLS regression',
  description: `Perform two-stage factor regression analysis for risk decomposition:

**Stage 1: Market Regression**
\`r_stock = α + βm × r_TOPIX + residual\`
- Returns market beta (βm) and R² (market exposure)

**Stage 2: Residual Factor Matching**
\`residual ~ r_index\` for each index category
- TOPIX-17 Sectors: Top 3 matching indices
- 33 Sectors: Top 3 matching indices
- TOPIX Size + Market + Style: Top 3 matching indices

This analysis helps identify which risk factors a stock is most exposed to beyond market risk.`,
  request: {
    params: FactorRegressionPathParamsSchema,
    query: FactorRegressionQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: FactorRegressionResponseSchema,
        },
      },
      description: 'Factor regression analysis results',
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
    422: {
      content: {
        'application/json': {
          schema: ErrorResponseSchema,
        },
      },
      description: 'Insufficient data for analysis',
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
 * Get factor regression analysis handler
 */
factorRegressionApp.openapi(getFactorRegressionRoute, async (c) => {
  const { symbol } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const result = await getFactorRegressionService().analyzeStock({
      symbol,
      lookbackDays: query.lookbackDays,
    });

    return c.json(result, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'perform factor regression',
      logContext: { symbol, lookbackDays: query.lookbackDays },
      errorMappings: [
        { pattern: 'not found', errorType: 'Not Found', statusCode: 404 },
        { pattern: 'Insufficient data', errorType: 'Unprocessable Entity', statusCode: 422 },
      ],
      allowedStatusCodes: [400, 404, 422, 500] as const,
    });
  }
});

export default factorRegressionApp;
