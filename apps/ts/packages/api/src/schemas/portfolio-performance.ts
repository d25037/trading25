import { z } from '@hono/zod-openapi';

/**
 * Portfolio performance path parameters schema
 */
export const PortfolioPerformancePathParamsSchema = z
  .object({
    id: z.string().regex(/^\d+$/).openapi({
      description: 'Portfolio ID',
      example: '1',
    }),
  })
  .openapi('PortfolioPerformancePathParams');

/**
 * Portfolio performance query parameters schema
 */
export const PortfolioPerformanceQuerySchema = z
  .object({
    benchmarkCode: z.string().length(4).default('0000').openapi({
      description: 'Benchmark index code (default: 0000 for TOPIX)',
      example: '0000',
    }),
    lookbackDays: z.coerce.number().int().min(30).max(1000).default(252).openapi({
      description: 'Number of trading days for time series analysis (default: 252)',
      example: 252,
    }),
  })
  .openapi('PortfolioPerformanceQuery');

/**
 * Portfolio summary schema
 */
export const PortfolioSummarySchema = z
  .object({
    totalCost: z.number().openapi({
      description: 'Total acquisition cost',
      example: 2500000,
    }),
    currentValue: z.number().openapi({
      description: 'Current market value',
      example: 2750000,
    }),
    totalPnL: z.number().openapi({
      description: 'Total unrealized P&L',
      example: 250000,
    }),
    returnRate: z.number().openapi({
      description: 'Return rate as decimal (e.g., 0.10 = 10%)',
      example: 0.1,
    }),
  })
  .openapi('PortfolioSummary');

/**
 * Holding performance schema
 */
export const HoldingPerformanceSchema = z
  .object({
    code: z.string().openapi({
      description: 'Stock code',
      example: '7203',
    }),
    companyName: z.string().openapi({
      description: 'Company name',
      example: 'トヨタ自動車',
    }),
    quantity: z.number().openapi({
      description: 'Holding quantity',
      example: 100,
    }),
    purchasePrice: z.number().openapi({
      description: 'Purchase price per share',
      example: 2500,
    }),
    currentPrice: z.number().openapi({
      description: 'Current price per share',
      example: 2750,
    }),
    cost: z.number().openapi({
      description: 'Total cost (purchasePrice x quantity)',
      example: 250000,
    }),
    marketValue: z.number().openapi({
      description: 'Current market value (currentPrice x quantity)',
      example: 275000,
    }),
    pnl: z.number().openapi({
      description: 'Unrealized P&L',
      example: 25000,
    }),
    returnRate: z.number().openapi({
      description: 'Return rate as decimal',
      example: 0.1,
    }),
    weight: z.number().openapi({
      description: 'Weight in portfolio (0-1)',
      example: 0.25,
    }),
    purchaseDate: z.string().openapi({
      description: 'Purchase date (YYYY-MM-DD)',
      example: '2024-01-15',
    }),
    account: z.string().optional().openapi({
      description: 'Account name',
      example: 'NISA',
    }),
  })
  .openapi('HoldingPerformance');

/**
 * Performance time series data point schema
 */
export const PerformanceDataPointSchema = z
  .object({
    date: z.string().openapi({
      description: 'Date (YYYY-MM-DD)',
      example: '2024-12-20',
    }),
    dailyReturn: z.number().openapi({
      description: 'Daily log return',
      example: 0.005,
    }),
    cumulativeReturn: z.number().openapi({
      description: 'Cumulative log return from start',
      example: 0.085,
    }),
  })
  .openapi('PerformanceDataPoint');

/**
 * Benchmark metrics schema
 */
export const BenchmarkMetricsSchema = z
  .object({
    code: z.string().openapi({
      description: 'Benchmark index code',
      example: '0000',
    }),
    name: z.string().openapi({
      description: 'Benchmark name',
      example: 'TOPIX',
    }),
    beta: z.number().openapi({
      description: 'Portfolio beta vs benchmark',
      example: 1.15,
    }),
    alpha: z.number().openapi({
      description: "Jensen's alpha (annualized)",
      example: 0.02,
    }),
    correlation: z.number().openapi({
      description: 'Correlation coefficient',
      example: 0.85,
    }),
    rSquared: z.number().openapi({
      description: 'R-squared from regression',
      example: 0.72,
    }),
    benchmarkReturn: z.number().openapi({
      description: 'Benchmark cumulative return',
      example: 0.06,
    }),
    relativeReturn: z.number().openapi({
      description: 'Relative return (portfolio - benchmark)',
      example: 0.025,
    }),
  })
  .openapi('BenchmarkMetrics');

/**
 * Benchmark time series data point schema
 */
export const BenchmarkDataPointSchema = z
  .object({
    date: z.string().openapi({
      description: 'Date (YYYY-MM-DD)',
      example: '2024-12-20',
    }),
    portfolioReturn: z.number().openapi({
      description: 'Portfolio cumulative return',
      example: 0.085,
    }),
    benchmarkReturn: z.number().openapi({
      description: 'Benchmark cumulative return',
      example: 0.06,
    }),
  })
  .openapi('BenchmarkDataPoint');

/**
 * Portfolio performance response schema
 */
export const PortfolioPerformanceResponseSchema = z
  .object({
    portfolioId: z.number().openapi({
      description: 'Portfolio ID',
      example: 1,
    }),
    portfolioName: z.string().openapi({
      description: 'Portfolio name',
      example: 'My Portfolio',
    }),
    portfolioDescription: z.string().optional().openapi({
      description: 'Portfolio description',
    }),

    summary: PortfolioSummarySchema.openapi({
      description: 'Portfolio summary metrics',
    }),

    holdings: z.array(HoldingPerformanceSchema).openapi({
      description: 'Per-holding performance metrics',
    }),

    timeSeries: z.array(PerformanceDataPointSchema).openapi({
      description: 'Portfolio performance time series',
    }),

    benchmark: BenchmarkMetricsSchema.nullable().openapi({
      description: 'Benchmark comparison metrics (null if insufficient data)',
    }),

    benchmarkTimeSeries: z.array(BenchmarkDataPointSchema).nullable().openapi({
      description: 'Portfolio vs benchmark time series (null if insufficient data)',
    }),

    analysisDate: z.string().openapi({
      description: 'Analysis date (YYYY-MM-DD)',
      example: '2024-12-20',
    }),

    dateRange: z
      .object({
        from: z.string().openapi({
          description: 'Start date of time series',
          example: '2024-01-02',
        }),
        to: z.string().openapi({
          description: 'End date of time series',
          example: '2024-12-19',
        }),
      })
      .nullable()
      .openapi({
        description: 'Date range of time series analysis',
      }),

    dataPoints: z.number().openapi({
      description: 'Number of data points in time series',
      example: 252,
    }),

    warnings: z.array(z.string()).openapi({
      description: 'Warnings or notes about the analysis',
    }),
  })
  .openapi('PortfolioPerformanceResponse', {
    description: 'Portfolio performance with P&L and benchmark comparison',
  });

/**
 * Type exports
 */
export type PortfolioPerformancePathParams = z.input<typeof PortfolioPerformancePathParamsSchema>;
export type PortfolioPerformanceQuery = z.input<typeof PortfolioPerformanceQuerySchema>;
export type PortfolioSummary = z.infer<typeof PortfolioSummarySchema>;
export type HoldingPerformance = z.infer<typeof HoldingPerformanceSchema>;
export type PerformanceDataPoint = z.infer<typeof PerformanceDataPointSchema>;
export type BenchmarkMetrics = z.infer<typeof BenchmarkMetricsSchema>;
export type BenchmarkDataPoint = z.infer<typeof BenchmarkDataPointSchema>;
export type PortfolioPerformanceResponse = z.infer<typeof PortfolioPerformanceResponseSchema>;
