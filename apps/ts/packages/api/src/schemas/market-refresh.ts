import { z } from '@hono/zod-openapi';

/**
 * Market refresh request body schema
 */
export const MarketRefreshRequestSchema = z
  .object({
    codes: z
      .array(z.string().min(1).max(10))
      .min(1)
      .max(50)
      .openapi({
        description: 'Stock codes to refresh (1-50 codes)',
        example: ['7203', '6758', '9984'],
      }),
  })
  .openapi('MarketRefreshRequest');

/**
 * Stock refetch result schema
 */
export const StockRefetchResultSchema = z
  .object({
    code: z.string().openapi({ description: 'Stock code', example: '7203' }),
    success: z.boolean().openapi({ description: 'Whether refetch was successful', example: true }),
    recordsFetched: z.number().int().openapi({ description: 'Number of records fetched from API', example: 2500 }),
    recordsStored: z.number().int().openapi({ description: 'Number of records stored in database', example: 252 }),
    error: z.string().optional().openapi({ description: 'Error message if failed', example: 'API call failed' }),
  })
  .openapi('StockRefetchResult');

/**
 * Market refresh response schema
 */
export const MarketRefreshResponseSchema = z
  .object({
    totalStocks: z.number().int().openapi({ description: 'Total number of stocks processed', example: 3 }),
    successCount: z.number().int().openapi({ description: 'Number of successful refreshes', example: 3 }),
    failedCount: z.number().int().openapi({ description: 'Number of failed refreshes', example: 0 }),
    totalApiCalls: z.number().int().openapi({ description: 'Total API calls made', example: 3 }),
    totalRecordsStored: z.number().int().openapi({ description: 'Total records stored', example: 756 }),
    results: z.array(StockRefetchResultSchema).openapi({ description: 'Individual stock results' }),
    errors: z.array(z.string()).openapi({ description: 'List of error messages', example: [] }),
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('MarketRefreshResponse', {
    description: 'Result of refreshing stock historical data',
  });

/**
 * Type exports
 */
export type MarketRefreshRequest = z.infer<typeof MarketRefreshRequestSchema>;
export type StockRefetchResult = z.infer<typeof StockRefetchResultSchema>;
export type MarketRefreshResponse = z.infer<typeof MarketRefreshResponseSchema>;
