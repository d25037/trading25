/**
 * Portfolio Performance Route
 *
 * GET /api/portfolio/{id}/performance - Portfolio P&L and benchmark comparison
 */

import { createRoute } from '@hono/zod-openapi';
import { PortfolioNotFoundError } from '@trading25/shared/portfolio';
import { logger } from '@trading25/shared/utils/logger';
import { getCorrelationId } from '../../middleware/correlation';
import { ErrorResponseSchema } from '../../schemas/common';
import {
  PortfolioPerformancePathParamsSchema,
  PortfolioPerformanceQuerySchema,
  PortfolioPerformanceResponseSchema,
} from '../../schemas/portfolio-performance';
import { PortfolioPerformanceService } from '../../services/portfolio-performance-service';
import { PortfolioService } from '../../services/portfolio-service';
import { createErrorResponse, createOpenAPIApp, safeParseInt } from '../../utils';

// Singleton services
let portfolioServiceInstance: PortfolioService | null = null;
let performanceServiceInstance: PortfolioPerformanceService | null = null;

function getPortfolioService(): PortfolioService {
  if (!portfolioServiceInstance) {
    portfolioServiceInstance = new PortfolioService();
  }
  return portfolioServiceInstance;
}

function getPerformanceService(): PortfolioPerformanceService {
  if (!performanceServiceInstance) {
    performanceServiceInstance = new PortfolioPerformanceService(getPortfolioService());

    // Register cleanup handlers
    const cleanup = () => {
      if (performanceServiceInstance) {
        logger.info('Closing portfolio performance service...');
        performanceServiceInstance.close();
        performanceServiceInstance = null;
      }
    };

    process.on('SIGTERM', cleanup);
    process.on('SIGINT', cleanup);
  }
  return performanceServiceInstance;
}

const performanceApp = createOpenAPIApp();

/**
 * Get portfolio performance
 */
const getPerformanceRoute = createRoute({
  method: 'get',
  path: '/api/portfolio/{id}/performance',
  tags: ['Portfolio'],
  summary: 'Get portfolio performance',
  description: 'Retrieve portfolio P&L metrics, holdings performance, and benchmark comparison',
  request: {
    params: PortfolioPerformancePathParamsSchema,
    query: PortfolioPerformanceQuerySchema,
  },
  responses: {
    200: {
      content: {
        'application/json': {
          schema: PortfolioPerformanceResponseSchema,
        },
      },
      description: 'Portfolio performance retrieved successfully',
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

performanceApp.openapi(getPerformanceRoute, async (c) => {
  const correlationId = getCorrelationId(c);
  const { id } = c.req.valid('param');
  const { benchmarkCode, lookbackDays } = c.req.valid('query');
  const portfolioId = safeParseInt(id, 'portfolioId');

  try {
    const service = getPerformanceService();
    const result = await service.getPortfolioPerformance(portfolioId, benchmarkCode, lookbackDays);

    // Transform holdings to API response format
    const holdings = result.holdings.map((h) => ({
      code: h.code,
      companyName: h.companyName,
      quantity: h.quantity,
      purchasePrice: h.purchasePrice,
      currentPrice: h.currentPrice,
      cost: h.cost,
      marketValue: h.marketValue,
      pnl: h.pnl,
      returnRate: h.returnRate,
      weight: h.weight,
      purchaseDate: h.purchaseDate,
      account: h.account,
    }));

    return c.json(
      {
        portfolioId: result.portfolioId,
        portfolioName: result.portfolioName,
        portfolioDescription: result.portfolioDescription,
        summary: result.summary,
        holdings,
        timeSeries: result.timeSeries.map((t) => ({
          date: t.date,
          dailyReturn: t.dailyReturn,
          cumulativeReturn: t.cumulativeReturn,
        })),
        benchmark: result.benchmark,
        benchmarkTimeSeries: result.benchmarkTimeSeries,
        analysisDate: result.analysisDate,
        dateRange: result.dateRange,
        dataPoints: result.dataPoints,
        warnings: result.warnings,
      },
      200
    );
  } catch (error) {
    if (error instanceof PortfolioNotFoundError) {
      return c.json(
        createErrorResponse({
          error: 'Not Found',
          message: error.message,
          correlationId,
        }),
        404
      );
    }

    logger.error('Failed to get portfolio performance', {
      correlationId,
      portfolioId,
      error: error instanceof Error ? error.message : String(error),
    });
    return c.json(
      createErrorResponse({
        error: 'Internal Server Error',
        message: error instanceof Error ? error.message : 'Failed to get portfolio performance',
        correlationId,
      }),
      500
    );
  }
});

export default performanceApp;
