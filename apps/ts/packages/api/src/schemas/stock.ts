import { z } from '@hono/zod-openapi';
import { AdjustedSchema, TimeframeSchema } from './common';

/**
 * Stock data point schema
 */
export const ApiStockDataPointSchema = z
  .object({
    time: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format'),
    open: z.number().positive('Open price must be positive'),
    high: z.number().positive('High price must be positive'),
    low: z.number().positive('Low price must be positive'),
    close: z.number().positive('Close price must be positive'),
    volume: z.number().nonnegative('Volume must be non-negative').optional(),
  })
  .openapi('ApiStockDataPoint', {
    description: 'Stock price data point for a specific date',
  });

/**
 * Stock data response schema
 */
export const ApiStockDataResponseSchema = z
  .object({
    data: z.array(ApiStockDataPointSchema),
    symbol: z.string().min(4).max(4),
    companyName: z.string().optional(),
    timeframe: z.string(),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiStockDataResponse', {
    description: 'Historical stock price data with metadata',
  });

/**
 * Margin volume ratio data point schema
 */
export const ApiMarginVolumeRatioDataSchema = z
  .object({
    date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    ratio: z.number().nonnegative(),
    weeklyAvgVolume: z.number().nonnegative(),
    marginVolume: z.number().nonnegative(),
  })
  .openapi('ApiMarginVolumeRatioData', {
    description: 'Margin volume ratio data for a specific date',
  });

/**
 * Margin volume ratio response schema
 */
export const ApiMarginVolumeRatioResponseSchema = z
  .object({
    symbol: z.string().min(4).max(4),
    longRatio: z.array(ApiMarginVolumeRatioDataSchema),
    shortRatio: z.array(ApiMarginVolumeRatioDataSchema),
    lastUpdated: z.string().datetime(),
  })
  .openapi('ApiMarginVolumeRatioResponse', {
    description: 'Long and short margin volume ratio data',
  });

/**
 * Stock symbol path parameter schema
 */
export const StockSymbolParamSchema = z
  .object({
    symbol: z
      .string()
      .length(4, 'Stock symbol must be exactly 4 characters')
      .regex(/^[0-9A-Z]+$/, 'Stock symbol must contain only numbers and uppercase letters')
      .openapi({
        param: {
          name: 'symbol',
          in: 'path',
        },
        example: '7203',
        description: 'Stock symbol (4 characters, e.g., 7203 for Toyota)',
      }),
  })
  .openapi('StockSymbolParam');

/**
 * Stock query parameters schema
 */
export const StockQuerySchema = z
  .object({
    timeframe: TimeframeSchema,
    adjusted: AdjustedSchema,
  })
  .openapi('StockQuery');

/**
 * Stock search query parameters schema
 */
export const StockSearchQuerySchema = z
  .object({
    q: z
      .string()
      .min(1, 'Search query must not be empty')
      .max(100, 'Search query too long')
      .openapi({
        param: {
          name: 'q',
          in: 'query',
        },
        example: 'トヨタ',
        description: 'Search query (stock code or company name)',
      }),
    limit: z
      .string()
      .optional()
      .default('20')
      .transform((val) => parseInt(val, 10))
      .pipe(z.number().min(1).max(100))
      .openapi({
        param: {
          name: 'limit',
          in: 'query',
        },
        example: '20',
        description: 'Maximum number of results (1-100, default: 20)',
      }),
  })
  .openapi('StockSearchQuery');

/**
 * Stock search result item schema
 */
export const StockSearchResultItemSchema = z
  .object({
    code: z.string().min(4).max(4),
    companyName: z.string(),
    companyNameEnglish: z.string().nullable(),
    marketCode: z.string(),
    marketName: z.string(),
    sector33Name: z.string(),
  })
  .openapi('StockSearchResultItem', {
    description: 'Stock search result item',
  });

/**
 * Stock search response schema
 */
export const StockSearchResponseSchema = z
  .object({
    query: z.string(),
    results: z.array(StockSearchResultItemSchema),
    count: z.number().nonnegative(),
  })
  .openapi('StockSearchResponse', {
    description: 'Stock search response with matching stocks',
  });
