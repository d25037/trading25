/**
 * Schema definitions for market.db data endpoints
 * Used by Python API clients (trading25-bt)
 */
import { z } from '@hono/zod-openapi';

// ===== Query Parameters =====

export const MarketDateRangeQuerySchema = z.object({
  start_date: z.string().optional().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
  end_date: z.string().optional().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
});

export const MarketStocksQuerySchema = z.object({
  market: z.enum(['prime', 'standard']).default('prime').openapi({ description: 'Market code', example: 'prime' }),
  history_days: z.coerce
    .number()
    .int()
    .min(1)
    .max(1000)
    .default(300)
    .openapi({ description: 'Number of days of history', example: 300 }),
});

// ===== Response Schemas =====

export const MarketOHLCVRecordSchema = z.object({
  date: z.string().openapi({ description: 'Date (YYYY-MM-DD)', example: '2024-01-15' }),
  open: z.number().openapi({ description: 'Open price', example: 2500.0 }),
  high: z.number().openapi({ description: 'High price', example: 2550.0 }),
  low: z.number().openapi({ description: 'Low price', example: 2480.0 }),
  close: z.number().openapi({ description: 'Close price', example: 2530.0 }),
  volume: z.number().int().openapi({ description: 'Volume', example: 1000000 }),
});

export const MarketOHLCRecordSchema = z.object({
  date: z.string().openapi({ description: 'Date (YYYY-MM-DD)', example: '2024-01-15' }),
  open: z.number().openapi({ description: 'Open price', example: 2500.0 }),
  high: z.number().openapi({ description: 'High price', example: 2550.0 }),
  low: z.number().openapi({ description: 'Low price', example: 2480.0 }),
  close: z.number().openapi({ description: 'Close price', example: 2530.0 }),
});

export const MarketStockDataSchema = z.object({
  code: z.string().openapi({ description: 'Stock code', example: '7203' }),
  company_name: z.string().openapi({ description: 'Company name', example: 'トヨタ自動車' }),
  data: z.array(MarketOHLCVRecordSchema).openapi({ description: 'OHLCV data array' }),
});

// ===== Single Stock Info Schema =====

export const StockInfoSchema = z
  .object({
    code: z.string().openapi({ description: 'Stock code (5-digit with trailing 0)', example: '72030' }),
    companyName: z.string().openapi({ description: 'Company name', example: 'トヨタ自動車' }),
    companyNameEnglish: z.string().openapi({ description: 'Company name in English', example: 'TOYOTA MOTOR CORPORATION' }),
    marketCode: z.string().openapi({ description: 'Market code', example: 'prime' }),
    marketName: z.string().openapi({ description: 'Market name', example: 'プライム' }),
    sector17Code: z.string().openapi({ description: 'Sector 17 code', example: '7' }),
    sector17Name: z.string().openapi({ description: 'Sector 17 name', example: '自動車・輸送機' }),
    sector33Code: z.string().openapi({ description: 'Sector 33 code', example: '16' }),
    sector33Name: z.string().openapi({ description: 'Sector 33 name', example: '輸送用機器' }),
    scaleCategory: z.string().openapi({ description: 'Scale category', example: 'TOPIX Large70' }),
    listedDate: z.string().openapi({ description: 'Listed date (YYYY-MM-DD)', example: '1949-05-16' }),
  })
  .openapi('StockInfo', {
    description: 'Stock information from market.db',
  });

// ===== Array Response Schemas =====

export const MarketOHLCVResponseSchema = z.array(MarketOHLCVRecordSchema);
export const MarketOHLCResponseSchema = z.array(MarketOHLCRecordSchema);
export const MarketStocksResponseSchema = z.array(MarketStockDataSchema);

// ===== Type exports =====

export type MarketDateRangeQuery = z.infer<typeof MarketDateRangeQuerySchema>;
export type MarketStocksQuery = z.infer<typeof MarketStocksQuerySchema>;
export type MarketOHLCVRecord = z.infer<typeof MarketOHLCVRecordSchema>;
export type MarketOHLCRecord = z.infer<typeof MarketOHLCRecordSchema>;
export type MarketStockData = z.infer<typeof MarketStockDataSchema>;
export type StockInfo = z.infer<typeof StockInfoSchema>;
