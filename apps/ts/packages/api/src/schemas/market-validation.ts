import { z } from '@hono/zod-openapi';

/**
 * Adjustment event schema
 */
export const AdjustmentEventSchema = z
  .object({
    code: z.string().openapi({ description: 'Stock code', example: '7203' }),
    date: z.string().openapi({ description: 'Event date', example: '2025-01-15' }),
    adjustmentFactor: z.number().openapi({ description: 'Adjustment factor', example: 0.5 }),
    close: z.number().openapi({ description: 'Closing price', example: 2500 }),
    eventType: z.string().openapi({ description: 'Event interpretation', example: '1:2 stock split' }),
  })
  .openapi('AdjustmentEvent');

/**
 * Integrity issue schema
 */
export const IntegrityIssueSchema = z
  .object({
    code: z.string().openapi({ description: 'Stock code', example: '7203' }),
    count: z.number().int().openapi({ description: 'Number of records outside range', example: 5 }),
  })
  .openapi('IntegrityIssue');

/**
 * Market validation response schema
 */
export const MarketValidationResponseSchema = z
  .object({
    status: z
      .enum(['healthy', 'warning', 'error'])
      .openapi({ description: 'Overall health status', example: 'healthy' }),
    initialized: z.boolean().openapi({ description: 'Database initialized', example: true }),
    lastSync: z
      .string()
      .nullable()
      .openapi({ description: 'Last sync timestamp', example: '2025-01-15T10:00:00.000Z' }),
    lastStocksRefresh: z
      .string()
      .nullable()
      .openapi({ description: 'Last stocks refresh timestamp', example: '2025-01-15T10:00:00.000Z' }),
    topix: z
      .object({
        count: z.number().int().openapi({ description: 'Number of TOPIX data points', example: 252 }),
        dateRange: z
          .object({
            min: z.string().openapi({ description: 'Earliest date', example: '2024-01-01' }),
            max: z.string().openapi({ description: 'Latest date', example: '2025-01-15' }),
          })
          .nullable(),
      })
      .openapi('TopixInfo'),
    stocks: z
      .object({
        total: z.number().int().openapi({ description: 'Total stock count', example: 1800 }),
        byMarket: z
          .record(z.string(), z.number().int())
          .openapi({ description: 'Stock count by market', example: { prime: 1500, standard: 300 } }),
      })
      .openapi('StocksInfo'),
    stockData: z
      .object({
        count: z.number().int().openapi({ description: 'Number of unique dates', example: 252 }),
        dateRange: z
          .object({
            min: z.string().openapi({ description: 'Earliest date', example: '2024-01-01' }),
            max: z.string().openapi({ description: 'Latest date', example: '2025-01-15' }),
          })
          .nullable(),
        missingDates: z
          .array(z.string())
          .openapi({ description: 'List of missing dates (max 20)', example: ['2025-01-14'] }),
        missingDatesCount: z.number().int().openapi({ description: 'Total missing dates count', example: 1 }),
      })
      .openapi('StockDataInfo'),
    failedDates: z.array(z.string()).openapi({ description: 'Dates pending retry (max 10)', example: [] }),
    failedDatesCount: z.number().int().openapi({ description: 'Total failed dates count', example: 0 }),
    adjustmentEvents: z.array(AdjustmentEventSchema).openapi({ description: 'Stock split/merger events (max 20)' }),
    adjustmentEventsCount: z.number().int().openapi({ description: 'Total adjustment events count', example: 0 }),
    stocksNeedingRefresh: z
      .array(z.string())
      .openapi({ description: 'Stock codes needing refresh (max 20)', example: [] }),
    stocksNeedingRefreshCount: z.number().int().openapi({ description: 'Total stocks needing refresh', example: 0 }),
    integrityIssues: z
      .array(IntegrityIssueSchema)
      .openapi({ description: 'Stocks with data outside TOPIX range (max 10)' }),
    integrityIssuesCount: z.number().int().openapi({ description: 'Total integrity issues count', example: 0 }),
    recommendations: z.array(z.string()).openapi({ description: 'Action recommendations' }),
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('MarketValidationResponse', {
    description: 'Market database validation report',
  });

/**
 * Type exports
 */
export type AdjustmentEvent = z.infer<typeof AdjustmentEventSchema>;
export type IntegrityIssue = z.infer<typeof IntegrityIssueSchema>;
export type MarketValidationResponse = z.infer<typeof MarketValidationResponseSchema>;
