import { createRoute } from '@hono/zod-openapi';
import { FactorRegressionError } from '@trading25/shared/factor-regression';
import { PortfolioNotFoundError } from '@trading25/shared/portfolio';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  PortfolioFactorRegressionPathParamsSchema,
  PortfolioFactorRegressionQuerySchema,
  PortfolioFactorRegressionResponseSchema,
} from '../../schemas/portfolio-factor-regression';
import { PortfolioFactorRegressionService } from '../../services/portfolio-factor-regression-service';
import { createErrorResponse, createManagedService, createOpenAPIApp } from '../../utils';

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
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error('Failed to perform portfolio factor regression', {
      correlationId,
      portfolioId,
      lookbackDays: query.lookbackDays,
      error: errorMessage,
    });

    // Handle specific error cases
    if (error instanceof PortfolioNotFoundError) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: errorMessage,
          correlationId,
        }),
        404
      );
    }

    if (
      error instanceof FactorRegressionError &&
      (error.code === 'NO_VALID_STOCKS' ||
        error.code === 'INSUFFICIENT_PORTFOLIO_DATA' ||
        error.code === 'ZERO_PORTFOLIO_VALUE')
    ) {
      return c.json(
        createErrorResponse({
          error: 'Unprocessable Entity',
          message: errorMessage,
          correlationId,
        }),
        422
      );
    }

    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: errorMessage,
        correlationId,
      }),
      500
    );
  }
});

export default portfolioFactorRegressionApp;
