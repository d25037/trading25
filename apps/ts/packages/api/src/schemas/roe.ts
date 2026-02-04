import { z } from '@hono/zod-openapi';

/**
 * ROE query parameters schema
 */
export const ROEQuerySchema = z
  .object({
    code: z.string().optional().openapi({
      description: 'Stock codes (comma-separated for multiple stocks, e.g., 7203,6758,9984)',
      example: '7203',
    }),
    date: z
      .string()
      .regex(/^\d{4}-?\d{2}-?\d{2}$/, 'Must be YYYYMMDD or YYYY-MM-DD format')
      .optional()
      .openapi({
        description: 'Specific date to filter statements (YYYYMMDD or YYYY-MM-DD)',
        example: '20241201',
      }),
    annualize: z
      .enum(['true', 'false'])
      .default('true')
      .transform((val) => val !== 'false')
      .openapi({
        description: 'Annualize quarterly data (default: true)',
        example: 'true',
      }),
    preferConsolidated: z
      .enum(['true', 'false'])
      .default('true')
      .transform((val) => val !== 'false')
      .openapi({
        description: 'Prefer consolidated over non-consolidated data (default: true)',
        example: 'true',
      }),
    minEquity: z
      .string()
      .default('1000')
      .transform((val) => Number.parseInt(val, 10))
      .openapi({
        description: 'Minimum equity threshold in millions of yen (default: 1000)',
        example: '1000',
      }),
    sortBy: z.enum(['roe', 'code', 'date']).default('roe').openapi({
      description: 'Sort results by field (default: roe)',
      example: 'roe',
    }),
    limit: z
      .string()
      .default('50')
      .transform((val) => Number.parseInt(val, 10))
      .openapi({
        description: 'Maximum number of results to return (default: 50)',
        example: '50',
      }),
  })
  .openapi('ROEQuery');

/**
 * ROE metadata schema
 */
export const ROEMetadataSchema = z
  .object({
    code: z.string().openapi({ description: 'Stock code', example: '7203' }),
    periodType: z.string().openapi({ description: 'Period type (FY, Q1, Q2, Q3)', example: 'FY' }),
    periodEnd: z.string().openapi({ description: 'Period end date', example: '2024-03-31' }),
    isConsolidated: z.boolean().openapi({ description: 'Whether data is consolidated', example: true }),
    accountingStandard: z.string().nullable().openapi({ description: 'Accounting standard', example: 'IFRS' }),
    isAnnualized: z
      .boolean()
      .optional()
      .openapi({ description: 'Whether quarterly data is annualized', example: false }),
  })
  .openapi('ROEMetadata', {
    description: 'Metadata for ROE calculation',
  });

/**
 * ROE result item schema
 */
export const ROEResultItemSchema = z
  .object({
    roe: z.number().openapi({ description: 'Return on Equity percentage', example: 15.5 }),
    netProfit: z.number().openapi({ description: 'Net profit in millions of yen', example: 250000 }),
    equity: z.number().openapi({ description: "Shareholders' equity in millions of yen", example: 1500000 }),
    metadata: ROEMetadataSchema,
  })
  .openapi('ROEResultItem', {
    description: 'ROE calculation result for a single company',
  });

/**
 * ROE summary statistics schema
 */
export const ROESummarySchema = z
  .object({
    averageROE: z.number().openapi({ description: 'Average ROE across all results', example: 12.5 }),
    maxROE: z.number().openapi({ description: 'Maximum ROE value', example: 25.0 }),
    minROE: z.number().openapi({ description: 'Minimum ROE value', example: 5.0 }),
    totalCompanies: z.number().int().openapi({ description: 'Number of companies in results', example: 50 }),
  })
  .openapi('ROESummary', {
    description: 'Summary statistics for ROE analysis',
  });

/**
 * ROE response schema
 */
export const ROEResponseSchema = z
  .object({
    results: z.array(ROEResultItemSchema),
    summary: ROESummarySchema,
    lastUpdated: z.string().datetime(),
  })
  .openapi('ROEResponse', {
    description: 'ROE analysis response with results and summary statistics',
  });

/**
 * Type exports for use in services
 */
export type ROEQuery = z.input<typeof ROEQuerySchema>;
export type ROEResultItem = z.infer<typeof ROEResultItemSchema>;
export type ROESummary = z.infer<typeof ROESummarySchema>;
export type ROEResponse = z.infer<typeof ROEResponseSchema>;
