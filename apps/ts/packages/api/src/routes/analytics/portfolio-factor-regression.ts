import { createRoute } from '@hono/zod-openapi';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  PortfolioFactorRegressionPathParamsSchema,
  PortfolioFactorRegressionQuerySchema,
  PortfolioFactorRegressionResponseSchema,
} from '../../schemas/portfolio-factor-regression';
import { PortfolioFactorRegressionService } from '../../services/portfolio-factor-regression-service';
import { createManagedService, createOpenAPIApp, handleRouteError } from '../../utils';

const getPortfolioFactorRegressionService = createManagedService('PortfolioFactorRegressionService', {
  factory: () => new PortfolioFactorRegressionService(),
});

const portfolioFactorRegressionApp = createOpenAPIApp();

/**
 * Get portfolio factor regression analysis route
 */
const getPortfolioFactorRegressionRoute = createRoute({
  method: 'get',
  path: '/api/analytics/portfolio-factor-regression/{portfolioId}',
  tags: ['Analytics'],
  summary: 'Analyze portfolio risk factors via OLS regression',
  description: `Perform two-stage factor regression analysis on a portfolio for risk decomposition.

**Weight Calculation**
Portfolio weights are calculated using current market values:
\`weight_i = (quantity_i × latestPrice_i) / Σ(quantity_j × latestPrice_j)\`

**Stage 1: Market Regression**
\`r_portfolio = α + βm × r_TOPIX + residual\`
- Returns market beta (βm) and R² (market exposure)

**Stage 2: Residual Factor Matching**
\`residual ~ r_index\` for each index category
- TOPIX-17 Sectors: Top 3 matching indices
- 33 Sectors: Top 3 matching indices
- TOPIX Size + Market + Style: Top 3 matching indices

This analysis helps identify which risk factors a portfolio is most exposed to beyond market risk.`,
  request: {
    params: PortfolioFactorRegressionPathParamsSchema,
    query: PortfolioFactorRegressionQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: PortfolioFactorRegressionResponseSchema,
        },
      },
      description: 'Portfolio factor regression analysis results',
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
      description: 'Portfolio not found',
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
 * Get portfolio factor regression analysis handler
 */
portfolioFactorRegressionApp.openapi(getPortfolioFactorRegressionRoute, async (c) => {
  const { portfolioId } = c.req.valid('param');
  const query = c.req.valid('query');
  const correlationId = getCorrelationId(c);

  try {
    const result = await getPortfolioFactorRegressionService().analyzePortfolio({
      portfolioId,
      lookbackDays: query.lookbackDays,
    });

    return c.json(result, 200);
  } catch (error) {
    return handleRouteError(c, error, correlationId, {
      operationName: 'perform portfolio factor regression',
      logContext: { portfolioId, lookbackDays: query.lookbackDays },
      errorMappings: [
        { pattern: 'not found', errorType: 'Not Found', statusCode: 404 },
        { pattern: 'insufficient', errorType: 'Unprocessable Entity', statusCode: 422 },
        { pattern: 'no valid', errorType: 'Unprocessable Entity', statusCode: 422 },
        { pattern: 'zero', errorType: 'Unprocessable Entity', statusCode: 422 },
        { pattern: 'no stocks', errorType: 'Unprocessable Entity', statusCode: 422 },
      ],
      allowedStatusCodes: [400, 404, 422, 500] as const,
    });
  }
});

export default portfolioFactorRegressionApp;
