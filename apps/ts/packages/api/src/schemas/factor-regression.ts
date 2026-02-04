import { z } from '@hono/zod-openapi';

/**
 * Factor regression path parameters schema
 */
export const FactorRegressionPathParamsSchema = z
  .object({
    symbol: z.string().length(4).openapi({
      description: 'Stock symbol (4-character code)',
      example: '7203',
    }),
  })
  .openapi('FactorRegressionPathParams');

/**
 * Factor regression query parameters schema
 */
export const FactorRegressionQuerySchema = z
  .object({
    lookbackDays: z.coerce.number().int().min(60).max(1000).default(252).openapi({
      description: 'Number of trading days for analysis (default: 252, min: 60, max: 1000)',
      example: 252,
    }),
  })
  .openapi('FactorRegressionQuery');

/**
 * Index match schema
 */
export const IndexMatchSchema = z
  .object({
    indexCode: z.string().openapi({
      description: 'Index code (e.g., "0040", "0085")',
      example: '0085',
    }),
    indexName: z.string().openapi({
      description: 'Index name',
      example: 'TOPIX-17 自動車・輸送機',
    }),
    category: z.string().openapi({
      description: 'Index category',
      example: 'sector17',
    }),
    rSquared: z.number().openapi({
      description: 'R-squared (coefficient of determination) from residual regression',
      example: 0.421,
    }),
    beta: z.number().openapi({
      description: 'Beta coefficient from residual regression',
      example: 0.85,
    }),
  })
  .openapi('IndexMatch');

/**
 * Factor regression response schema
 */
export const FactorRegressionResponseSchema = z
  .object({
    stockCode: z.string().openapi({
      description: 'Stock code',
      example: '7203',
    }),
    companyName: z.string().optional().openapi({
      description: 'Company name',
      example: 'トヨタ自動車',
    }),

    // Stage 1: Market regression
    marketBeta: z.number().openapi({
      description: 'Market beta (βm) from TOPIX regression',
      example: 1.15,
    }),
    marketRSquared: z.number().openapi({
      description: 'R-squared from TOPIX regression (market exposure)',
      example: 0.653,
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
      .openapi('DateRange'),
  })
  .openapi('FactorRegressionResponse', {
    description: 'Two-stage factor regression analysis result for risk decomposition',
  });

/**
 * Type exports for use in services
 */
export type FactorRegressionPathParams = z.input<typeof FactorRegressionPathParamsSchema>;
export type FactorRegressionQuery = z.input<typeof FactorRegressionQuerySchema>;
export type IndexMatch = z.infer<typeof IndexMatchSchema>;
export type FactorRegressionResponse = z.infer<typeof FactorRegressionResponseSchema>;
