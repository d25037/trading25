import { z } from '@hono/zod-openapi';

/**
 * Sector stocks query parameters schema
 */
export const SectorStocksQuerySchema = z
  .object({
    sector33Name: z.string().optional().openapi({
      description: 'Sector 33 name to filter by (e.g., "輸送用機器")',
      example: '輸送用機器',
    }),
    sector17Name: z.string().optional().openapi({
      description: 'Sector 17 name to filter by (e.g., "自動車・輸送機")',
      example: '自動車・輸送機',
    }),
    markets: z.string().default('prime,standard').openapi({
      description: 'Market filter (prime, standard, growth, or comma-separated)',
      example: 'prime,standard',
    }),
    lookbackDays: z
      .string()
      .default('5')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 100);
      })
      .openapi({
        description: 'Trading days to look back for price change calculations (1-100, default: 5)',
        example: '5',
      }),
    sortBy: z.enum(['tradingValue', 'changePercentage', 'code']).default('tradingValue').openapi({
      description: 'Sort field (tradingValue, changePercentage, code)',
      example: 'tradingValue',
    }),
    sortOrder: z.enum(['asc', 'desc']).default('desc').openapi({
      description: 'Sort order (asc, desc)',
      example: 'desc',
    }),
    limit: z
      .string()
      .default('100')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 500);
      })
      .openapi({
        description: 'Maximum number of results (1-500, default: 100)',
        example: '100',
      }),
  })
  .openapi('SectorStocksQuery');

/**
 * Sector stock item schema
 */
export const SectorStockItemSchema = z
  .object({
    rank: z.number().int().openapi({ description: 'Rank position', example: 1 }),
    code: z.string().openapi({ description: 'Stock code', example: '7203' }),
    companyName: z.string().openapi({ description: 'Company name', example: 'トヨタ自動車' }),
    marketCode: z.string().openapi({ description: 'Market code', example: 'prime' }),
    sector33Name: z.string().openapi({ description: 'Sector name', example: '輸送用機器' }),
    currentPrice: z.number().openapi({ description: 'Current stock price', example: 2500 }),
    volume: z.number().openapi({ description: 'Trading volume', example: 10000000 }),
    tradingValue: z
      .number()
      .optional()
      .openapi({ description: 'Trading value (price * volume)', example: 25000000000 }),
    basePrice: z.number().optional().openapi({ description: 'Base price for lookback calculation', example: 2400 }),
    changeAmount: z.number().optional().openapi({ description: 'Price change amount', example: 100 }),
    changePercentage: z.number().optional().openapi({ description: 'Price change percentage', example: 4.17 }),
    lookbackDays: z.number().optional().openapi({ description: 'Lookback days used', example: 5 }),
  })
  .openapi('SectorStockItem');

/**
 * Sector stocks response schema
 */
export const SectorStocksResponseSchema = z
  .object({
    sector33Name: z.string().optional().openapi({ description: 'Sector 33 name filter used' }),
    sector17Name: z.string().optional().openapi({ description: 'Sector 17 name filter used' }),
    markets: z.array(z.string()).openapi({ description: 'Markets included', example: ['prime', 'standard'] }),
    lookbackDays: z.number().int().openapi({ description: 'Lookback days used', example: 5 }),
    sortBy: z.string().openapi({ description: 'Sort field used', example: 'tradingValue' }),
    sortOrder: z.string().openapi({ description: 'Sort order used', example: 'desc' }),
    stocks: z.array(SectorStockItemSchema),
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('SectorStocksResponse');

/**
 * Type exports
 */
export type SectorStocksQuery = z.input<typeof SectorStocksQuerySchema>;
export type SectorStockItem = z.infer<typeof SectorStockItemSchema>;
export type SectorStocksResponse = z.infer<typeof SectorStocksResponseSchema>;
