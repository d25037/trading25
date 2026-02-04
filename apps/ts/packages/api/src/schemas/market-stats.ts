import { z } from '@hono/zod-openapi';

/**
 * Date range schema
 */
const DateRangeSchema = z
  .object({
    min: z.string().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
    max: z.string().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
  })
  .nullable()
  .openapi('DateRange');

/**
 * Index stats by category schema
 */
const IndexStatsByCategorySchema = z.record(z.string(), z.number()).openapi({
  description: 'Number of indices by category',
  example: { topix: 1, sector33: 33, sector17: 17, market: 1 },
});

/**
 * Market stats response schema
 */
export const MarketStatsResponseSchema = z
  .object({
    initialized: z.boolean().openapi({ description: 'Whether the database is initialized', example: true }),
    lastSync: z.string().nullable().openapi({ description: 'Last sync timestamp', example: '2024-12-19T10:00:00Z' }),
    databaseSize: z.number().openapi({ description: 'Database file size in bytes', example: 104857600 }),
    topix: z
      .object({
        count: z.number().openapi({ description: 'Number of TOPIX records', example: 294 }),
        dateRange: DateRangeSchema,
      })
      .openapi('TopixStats'),
    stocks: z
      .object({
        total: z.number().openapi({ description: 'Total number of stocks', example: 1800 }),
        byMarket: z.record(z.string(), z.number()).openapi({
          description: 'Number of stocks by market',
          example: { prime: 1200, standard: 600 },
        }),
      })
      .openapi('StocksStats'),
    stockData: z
      .object({
        count: z.number().openapi({ description: 'Total stock data records', example: 933977 }),
        dateCount: z.number().openapi({ description: 'Number of unique dates', example: 294 }),
        dateRange: DateRangeSchema,
        averageStocksPerDay: z.number().openapi({ description: 'Average stocks per trading day', example: 3177 }),
      })
      .openapi('StockDataStats'),
    indices: z
      .object({
        masterCount: z.number().openapi({ description: 'Number of index definitions', example: 52 }),
        dataCount: z.number().openapi({ description: 'Total index data records', example: 15288 }),
        dateCount: z.number().openapi({ description: 'Number of unique dates', example: 294 }),
        dateRange: DateRangeSchema,
        byCategory: IndexStatsByCategorySchema,
      })
      .openapi('IndicesStats'),
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('MarketStatsResponse', {
    description: 'Market database statistics',
  });

export type MarketStatsResponse = z.infer<typeof MarketStatsResponseSchema>;
