import { z } from '@hono/zod-openapi';
import { IndexMatchSchema } from './factor-regression';

/**
 * Portfolio factor regression path parameters schema
 */
export const PortfolioFactorRegressionPathParamsSchema = z
  .object({
    portfolioId: z.coerce.number().int().positive().openapi({
      description: 'Portfolio ID',
      example: 1,
    }),
  })
  .openapi('PortfolioFactorRegressionPathParams');

/**
 * Portfolio factor regression query parameters schema
 */
export const PortfolioFactorRegressionQuerySchema = z
  .object({
    lookbackDays: z.coerce.number().int().min(60).max(1000).default(252).openapi({
      description: 'Number of trading days for analysis (default: 252, min: 60, max: 1000)',
      example: 252,
    }),
  })
  .openapi('PortfolioFactorRegressionQuery');

/**
 * Portfolio weight schema
 */
export const PortfolioWeightSchema = z
  .object({
    code: z.string().openapi({
      description: 'Stock code (4-digit)',
      example: '7203',
    }),
    companyName: z.string().openapi({
      description: 'Company name',
      example: 'トヨタ自動車',
    }),
    weight: z.number().openapi({
      description: 'Weight (0-1)',
      example: 0.25,
    }),
    latestPrice: z.number().openapi({
      description: 'Latest stock price used for weight calculation',
      example: 2500,
    }),
    marketValue: z.number().openapi({
      description: 'Current market value (quantity × latestPrice)',
      example: 250000,
    }),
    quantity: z.number().openapi({
      description: 'Holding quantity',
      example: 100,
    }),
  })
  .openapi('PortfolioWeight');

/**
 * Excluded stock schema
 */
export const ExcludedStockSchema = z
  .object({
    code: z.string().openapi({
      description: 'Stock code (4-digit)',
      example: '9999',
    }),
    companyName: z.string().openapi({
      description: 'Company name',
      example: 'Example Corp',
    }),
    reason: z.string().openapi({
      description: 'Reason for exclusion',
      example: 'Insufficient data points: 25 < 30',
    }),
  })
  .openapi('ExcludedStock');

/**
 * Portfolio factor regression response schema
 */
export const PortfolioFactorRegressionResponseSchema = z
  .object({
    portfolioId: z.number().openapi({
      description: 'Portfolio ID',
      example: 1,
    }),
    portfolioName: z.string().openapi({
      description: 'Portfolio name',
      example: 'My Portfolio',
    }),

    // Weight information
    weights: z.array(PortfolioWeightSchema).openapi({
      description: 'Weight breakdown for each stock',
    }),
    totalValue: z.number().openapi({
      description: 'Total portfolio market value',
      example: 1000000,
    }),
    stockCount: z.number().openapi({
      description: 'Number of stocks in portfolio',
      example: 5,
    }),
    includedStockCount: z.number().openapi({
      description: 'Number of stocks included in analysis',
      example: 4,
    }),

    // Stage 1: Market regression
    marketBeta: z.number().openapi({
      description: 'Market beta (βm) from TOPIX regression',
      example: 1.05,
    }),
    marketRSquared: z.number().openapi({
      description: 'R-squared from TOPIX regression (market exposure)',
      example: 0.72,
    }),

    // Stage 2: Factor matches (top 3 from each category)
    sector17Matches: z.array(IndexMatchSchema).openapi({
      description: 'Top 3 matching TOPIX-17 sector indices',
    }),
    sector33Matches: z.array(IndexMatchSchema).openapi({
      description: 'Top 3 matching 33-sector indices',
    }),
    topixStyleMatches: z.array(IndexMatchSchema).openapi({
      description: 'Top 3 matching TOPIX size + Market + Style indices (excluding TOPIX 0000)',
    }),

    // Metadata
    analysisDate: z.string().openapi({
      description: 'Analysis date (YYYY-MM-DD)',
      example: '2024-12-20',
    }),
    dataPoints: z.number().openapi({
      description: 'Number of trading days used in analysis',
      example: 252,
    }),
    dateRange: z
      .object({
        from: z.string().openapi({
          description: 'Start date of analysis period (YYYY-MM-DD)',
          example: '2024-01-02',
        }),
        to: z.string().openapi({
          description: 'End date of analysis period (YYYY-MM-DD)',
          example: '2024-12-19',
        }),
      })
      .openapi('PortfolioDateRange'),

    // Excluded stocks
    excludedStocks: z.array(ExcludedStockSchema).openapi({
      description: 'Stocks excluded from analysis due to insufficient data',
    }),
  })
  .openapi('PortfolioFactorRegressionResponse', {
    description: 'Portfolio factor regression analysis result for risk decomposition',
  });

/**
 * Type exports for use in services
 */
export type PortfolioFactorRegressionPathParams = z.input<typeof PortfolioFactorRegressionPathParamsSchema>;
export type PortfolioFactorRegressionQuery = z.input<typeof PortfolioFactorRegressionQuerySchema>;
export type PortfolioWeight = z.infer<typeof PortfolioWeightSchema>;
export type ExcludedStock = z.infer<typeof ExcludedStockSchema>;
export type PortfolioFactorRegressionResponse = z.infer<typeof PortfolioFactorRegressionResponseSchema>;
