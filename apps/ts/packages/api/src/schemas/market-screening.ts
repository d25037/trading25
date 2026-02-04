import { z } from '@hono/zod-openapi';

/**
 * Market screening query parameters schema
 */
export const MarketScreeningQuerySchema = z
  .object({
    markets: z.string().default('prime').openapi({
      description: 'Market filter (prime, standard, or prime,standard)',
      example: 'prime',
    }),
    rangeBreakFast: z
      .enum(['true', 'false'])
      .default('true')
      .transform((val) => val !== 'false')
      .openapi({
        description: 'Enable Range Break Fast screening (default: true)',
        example: 'true',
      }),
    rangeBreakSlow: z
      .enum(['true', 'false'])
      .default('true')
      .transform((val) => val !== 'false')
      .openapi({
        description: 'Enable Range Break Slow screening (default: true)',
        example: 'true',
      }),
    recentDays: z
      .string()
      .default('10')
      .transform((val) => {
        const num = Number.parseInt(val, 10);
        return Math.min(Math.max(num, 1), 90);
      })
      .openapi({
        description: 'Days to look back for recent signals (1-90, default: 10)',
        example: '10',
      }),
    date: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/)
      .optional()
      .openapi({
        description:
          'Reference date for historical screening (YYYY-MM-DD). When specified, screening runs as if this date is "today".',
        example: '2025-01-18',
      }),
    minBreakPercentage: z
      .string()
      .optional()
      .transform((val) => (val ? Number.parseFloat(val) : undefined))
      .openapi({
        description: 'Minimum break percentage filter',
        example: '5.0',
      }),
    minVolumeRatio: z
      .string()
      .optional()
      .transform((val) => (val ? Number.parseFloat(val) : undefined))
      .openapi({
        description: 'Minimum volume ratio filter',
        example: '1.5',
      }),
    sortBy: z.enum(['date', 'stockCode', 'volumeRatio', 'breakPercentage']).default('date').openapi({
      description: 'Sort results by field (default: date)',
      example: 'date',
    }),
    order: z.enum(['asc', 'desc']).default('desc').openapi({
      description: 'Sort order (default: desc)',
      example: 'desc',
    }),
    limit: z
      .string()
      .optional()
      .transform((val) => (val ? Number.parseInt(val, 10) : undefined))
      .openapi({
        description: 'Limit number of results',
        example: '50',
      }),
  })
  .openapi('MarketScreeningQuery');

/**
 * Range break details schema
 */
export const RangeBreakDetailsSchema = z
  .object({
    breakDate: z.string().openapi({ description: 'Date of the breakout', example: '2025-01-15' }),
    currentHigh: z.number().openapi({ description: 'Current high price', example: 2600 }),
    maxHighInLookback: z.number().openapi({ description: 'Maximum high in lookback period', example: 2500 }),
    breakPercentage: z.number().openapi({ description: 'Break percentage above lookback max', example: 4.0 }),
    volumeRatio: z.number().openapi({ description: 'Volume ratio (short/long MA)', example: 2.1 }),
    avgVolume20Days: z.number().openapi({ description: 'Average volume (short period)', example: 1500000 }),
    avgVolume100Days: z.number().openapi({ description: 'Average volume (long period)', example: 700000 }),
  })
  .openapi('RangeBreakDetails');

/**
 * Future price movement at a specific offset
 */
export const FuturePricePointSchema = z
  .object({
    date: z.string().openapi({ description: 'Date of the price point', example: '2025-01-23' }),
    price: z.number().openapi({ description: 'Closing price', example: 2680 }),
    changePercent: z.number().openapi({ description: 'Percentage change from break date', example: 3.08 }),
  })
  .openapi('FuturePricePoint');

/**
 * Future returns schema for historical screening
 */
export const FutureReturnsSchema = z
  .object({
    day5: FuturePricePointSchema.nullable().openapi({ description: 'Price movement after 5 trading days' }),
    day20: FuturePricePointSchema.nullable().openapi({ description: 'Price movement after 20 trading days' }),
    day60: FuturePricePointSchema.nullable().openapi({ description: 'Price movement after 60 trading days' }),
  })
  .openapi('FutureReturns');

/**
 * Screening result item schema
 */
export const ScreeningResultItemSchema = z
  .object({
    stockCode: z.string().openapi({ description: 'Stock code', example: '7203' }),
    companyName: z.string().openapi({ description: 'Company name', example: 'トヨタ自動車' }),
    scaleCategory: z.string().optional().openapi({ description: 'Scale category', example: 'TOPIX Large70' }),
    sector33Name: z.string().optional().openapi({ description: 'Sector name', example: '輸送用機器' }),
    screeningType: z.enum(['rangeBreakFast', 'rangeBreakSlow']).openapi({
      description: 'Type of screening match',
      example: 'rangeBreakFast',
    }),
    matchedDate: z.string().openapi({ description: 'Date of the match', example: '2025-01-15' }),
    details: z
      .object({
        rangeBreak: RangeBreakDetailsSchema.optional(),
      })
      .openapi('ScreeningDetails'),
    futureReturns: FutureReturnsSchema.optional().openapi({
      description: 'Future price movements after the match date (only included for historical screening)',
    }),
  })
  .openapi('ScreeningResultItem');

/**
 * Screening summary schema
 */
export const ScreeningSummarySchema = z
  .object({
    totalStocksScreened: z.number().int().openapi({ description: 'Total stocks screened', example: 1500 }),
    matchCount: z.number().int().openapi({ description: 'Number of matches', example: 25 }),
    skippedCount: z.number().int().openapi({ description: 'Stocks skipped (insufficient data)', example: 50 }),
    byScreeningType: z
      .object({
        rangeBreakFast: z.number().int().openapi({ description: 'Range Break Fast matches', example: 15 }),
        rangeBreakSlow: z.number().int().openapi({ description: 'Range Break Slow matches', example: 10 }),
      })
      .openapi('ScreeningTypeCounts'),
  })
  .openapi('ScreeningSummary');

/**
 * Market screening response schema
 */
export const MarketScreeningResponseSchema = z
  .object({
    results: z.array(ScreeningResultItemSchema),
    summary: ScreeningSummarySchema,
    markets: z.array(z.string()).openapi({ description: 'Markets screened', example: ['prime'] }),
    recentDays: z.number().int().openapi({ description: 'Recent days setting', example: 10 }),
    referenceDate: z.string().optional().openapi({
      description: 'Reference date for historical screening (if specified)',
      example: '2025-01-18',
    }),
    lastUpdated: z.string().datetime().openapi({ description: 'Response timestamp' }),
  })
  .openapi('MarketScreeningResponse', {
    description: 'Stock screening results with summary statistics',
  });

/**
 * Type exports
 */
export type MarketScreeningQuery = z.input<typeof MarketScreeningQuerySchema>;
export type RangeBreakDetails = z.infer<typeof RangeBreakDetailsSchema>;
export type FuturePricePoint = z.infer<typeof FuturePricePointSchema>;
export type FutureReturns = z.infer<typeof FutureReturnsSchema>;
export type ScreeningResultItem = z.infer<typeof ScreeningResultItemSchema>;
export type ScreeningSummary = z.infer<typeof ScreeningSummarySchema>;
export type MarketScreeningResponse = z.infer<typeof MarketScreeningResponseSchema>;
