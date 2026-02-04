import { z } from '@hono/zod-openapi';

/**
 * Market ranking query parameters schema
 */
export const MarketRankingQuerySchema = z
  .object({
    date: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format')
      .optional()
      .openapi({
        description: 'Target date (YYYY-MM-DD). Defaults to latest trading date.',
        example: '2025-01-15',
      }),
    limit: z
      .string()
      .default('20')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 100);
      })
      .openapi({
        description: 'Number of results per ranking (1-100, default: 20)',
        example: '20',
      }),
    markets: z.string().default('prime').openapi({
      description: 'Market filter (prime, standard, or prime,standard)',
      example: 'prime',
    }),
    lookbackDays: z
      .string()
      .default('1')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 100);
      })
      .openapi({
        description: 'Trading days to look back for average calculations (1-100, default: 1)',
        example: '1',
      }),
    periodDays: z
      .string()
      .default('250')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 250);
      })
      .openapi({
        description: 'Period days for high/low calculations (1-250, default: 250)',
        example: '250',
      }),
  })
  .openapi('MarketRankingQuery');

/**
 * Ranking item schema
 */
export const RankingItemSchema = z
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
    tradingValueAverage: z
      .number()
      .optional()
      .openapi({ description: 'Average trading value over lookback period', example: 24000000000 }),
    previousPrice: z.number().optional().openapi({ description: 'Previous day closing price', example: 2480 }),
    basePrice: z.number().optional().openapi({ description: 'Base price for lookback calculation', example: 2400 }),
    changeAmount: z.number().optional().openapi({ description: 'Price change amount', example: 20 }),
    changePercentage: z.number().optional().openapi({ description: 'Price change percentage', example: 0.81 }),
    lookbackDays: z.number().optional().openapi({ description: 'Lookback days used', example: 5 }),
  })
  .openapi('RankingItem', {
    description: 'Single ranking entry',
  });

/**
 * Rankings collection schema
 */
export const RankingsSchema = z
  .object({
    tradingValue: z.array(RankingItemSchema).openapi({ description: 'Top stocks by trading value' }),
    gainers: z.array(RankingItemSchema).openapi({ description: 'Top price gainers' }),
    losers: z.array(RankingItemSchema).openapi({ description: 'Top price losers' }),
    periodHigh: z.array(RankingItemSchema).openapi({ description: 'Stocks hitting N-day high' }),
    periodLow: z.array(RankingItemSchema).openapi({ description: 'Stocks hitting N-day low' }),
  })
  .openapi('Rankings', {
    description: 'Collection of market rankings',
  });

/**
 * Market ranking response schema
 */
export const MarketRankingResponseSchema = z
  .object({
    date: z.string().openapi({ description: 'Ranking date', example: '2025-01-15' }),
    markets: z.array(z.string()).openapi({ description: 'Markets included', example: ['prime'] }),
    lookbackDays: z.number().int().openapi({ description: 'Lookback days used', example: 1 }),
    periodDays: z.number().int().openapi({ description: 'Period days for high/low', example: 250 }),
    rankings: RankingsSchema,
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('MarketRankingResponse', {
    description: 'Market ranking response with trading value, gainers, losers, and period high/low',
  });

/**
 * Type exports for use in services
 */
export type MarketRankingQuery = z.input<typeof MarketRankingQuerySchema>;
export type RankingItem = z.infer<typeof RankingItemSchema>;
export type Rankings = z.infer<typeof RankingsSchema>;
export type MarketRankingResponse = z.infer<typeof MarketRankingResponseSchema>;
