/**
 * Schema definitions for dataset data endpoints
 * Used by Python API clients (trading25-bt)
 */
import { z } from '@hono/zod-openapi';

// ===== Constants =====

/** Maximum number of stock codes allowed in a batch request */
export const MAX_BATCH_CODES = 100;

// ===== Parameter Schemas =====

/**
 * Dataset name validation schema
 * Only allows alphanumeric characters, underscores, and hyphens
 * Prevents path traversal attacks
 */
export const DatasetNameSchema = z
  .string()
  .min(1)
  .max(50)
  .regex(/^[a-zA-Z0-9_-]+$/, {
    message: 'Dataset name must contain only alphanumeric characters, underscores, or hyphens',
  })
  .openapi({ description: 'Dataset name', example: 'sampleA' });

/**
 * Stock code validation schema
 * Only allows 4-5 digit numeric codes
 */
export const StockCodeSchema = z
  .string()
  .regex(/^\d{4,5}$/, {
    message: 'Stock code must be 4-5 digits',
  })
  .openapi({ description: 'Stock code (4-5 digits)', example: '7203' });

/**
 * Index code validation schema
 * Format: 'I' followed by digits, or sector code format
 */
export const IndexCodeSchema = z
  .string()
  .min(1)
  .max(10)
  .regex(/^[A-Z]?\d+$/, {
    message: 'Index code must be a valid format (e.g., I1001 or 3650)',
  })
  .openapi({ description: 'Index code', example: 'I1001' });

// ===== Query Parameters =====

export const DateRangeQuerySchema = z.object({
  start_date: z.string().optional().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
  end_date: z.string().optional().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
});

/**
 * Period type filter for statements
 */
export const PeriodTypeSchema = z.enum(['all', 'FY', '1Q', '2Q', '3Q']).default('all');

/**
 * Statements query schema with period filtering
 */
export const StatementsQuerySchema = z.object({
  start_date: z.string().optional().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
  end_date: z.string().optional().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
  period_type: PeriodTypeSchema.openapi({
    description: 'Filter by period type: all (default), FY (full year), 1Q, 2Q, 3Q',
    example: 'FY',
  }),
  actual_only: z.enum(['true', 'false']).default('false').openapi({
    description: 'If true, exclude forecast data (records without actual financial data)',
    example: 'true',
  }),
});

export const TimeframeSchema = z.enum(['daily', 'weekly', 'monthly']).default('daily');

export const OHLCVQuerySchema = z.object({
  start_date: z.string().optional().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
  end_date: z.string().optional().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
  timeframe: TimeframeSchema.openapi({
    description: 'Timeframe for data aggregation',
    example: 'daily',
  }),
});

export const StockListQuerySchema = z.object({
  min_records: z.coerce
    .number()
    .int()
    .min(1)
    .default(100)
    .openapi({ description: 'Minimum number of records required', example: 100 }),
  limit: z.coerce.number().int().min(1).max(10000).optional().openapi({ description: 'Maximum number of results' }),
  detail: z.enum(['true', 'false']).optional().openapi({ description: 'Include date range info' }),
});

export const IndexListQuerySchema = z.object({
  min_records: z.coerce.number().int().min(1).default(100).openapi({ description: 'Minimum records required' }),
  codes: z.string().optional().openapi({ description: 'Comma-separated index codes' }),
});

export const MarginListQuerySchema = z.object({
  min_records: z.coerce.number().int().min(1).default(10).openapi({ description: 'Minimum records required' }),
  codes: z.string().optional().openapi({ description: 'Comma-separated stock codes' }),
});

/**
 * Shared batch query fields: codes with MAX_BATCH_CODES limit and optional date range
 */
const BatchQueryBaseSchema = z.object({
  codes: z
    .string()
    .refine((val) => val.split(',').length <= MAX_BATCH_CODES, {
      message: `Maximum ${MAX_BATCH_CODES} stock codes allowed per request`,
    })
    .openapi({ description: `Comma-separated stock codes (max ${MAX_BATCH_CODES})`, example: '7203,9984' }),
  start_date: z.string().optional().openapi({ description: 'Start date (YYYY-MM-DD)', example: '2024-01-01' }),
  end_date: z.string().optional().openapi({ description: 'End date (YYYY-MM-DD)', example: '2024-12-31' }),
});

export const BatchOHLCVQuerySchema = BatchQueryBaseSchema.extend({
  timeframe: TimeframeSchema.openapi({
    description: 'Timeframe for data aggregation',
    example: 'daily',
  }),
});

export const BatchMarginQuerySchema = BatchQueryBaseSchema;

export const BatchStatementsQuerySchema = BatchQueryBaseSchema.extend({
  period_type: PeriodTypeSchema.openapi({
    description: 'Filter by period type: all (default), FY (full year), 1Q, 2Q, 3Q',
    example: 'FY',
  }),
  actual_only: z.enum(['true', 'false']).default('false').openapi({
    description: 'If true, exclude forecast data (records without actual financial data)',
    example: 'true',
  }),
});

// ===== Response Schemas =====

export const OHLCVRecordSchema = z.object({
  date: z.string().openapi({ description: 'Date (YYYY-MM-DD)', example: '2024-01-15' }),
  open: z.number().openapi({ description: 'Open price', example: 2500.0 }),
  high: z.number().openapi({ description: 'High price', example: 2550.0 }),
  low: z.number().openapi({ description: 'Low price', example: 2480.0 }),
  close: z.number().openapi({ description: 'Close price', example: 2530.0 }),
  volume: z.number().int().openapi({ description: 'Volume', example: 1000000 }),
});

export const OHLCRecordSchema = z.object({
  date: z.string().openapi({ description: 'Date (YYYY-MM-DD)', example: '2024-01-15' }),
  open: z.number().openapi({ description: 'Open price', example: 2500.0 }),
  high: z.number().openapi({ description: 'High price', example: 2550.0 }),
  low: z.number().openapi({ description: 'Low price', example: 2480.0 }),
  close: z.number().openapi({ description: 'Close price', example: 2530.0 }),
});

export const StockListItemSchema = z.object({
  stockCode: z.string().openapi({ description: 'Stock code', example: '7203' }),
  record_count: z.number().int().openapi({ description: 'Number of records', example: 500 }),
  start_date: z.string().optional().openapi({ description: 'First date', example: '2020-01-06' }),
  end_date: z.string().optional().openapi({ description: 'Last date', example: '2024-12-27' }),
});

export const IndexListItemSchema = z.object({
  indexCode: z.string().openapi({ description: 'Index code', example: 'I1001' }),
  indexName: z.string().openapi({ description: 'Index name', example: '電気機器' }),
  record_count: z.number().int().openapi({ description: 'Number of records', example: 300 }),
  start_date: z.string().optional().openapi({ description: 'First date' }),
  end_date: z.string().optional().openapi({ description: 'Last date' }),
});

export const MarginRecordSchema = z.object({
  date: z.string().openapi({ description: 'Date (YYYY-MM-DD)', example: '2024-01-15' }),
  longMarginVolume: z.number().openapi({ description: 'Long margin volume', example: 10000 }),
  shortMarginVolume: z.number().openapi({ description: 'Short margin volume', example: 5000 }),
});

export const MarginListItemSchema = z.object({
  stockCode: z.string().openapi({ description: 'Stock code', example: '7203' }),
  record_count: z.number().int().openapi({ description: 'Number of records', example: 100 }),
  start_date: z.string().optional().openapi({ description: 'First date' }),
  end_date: z.string().optional().openapi({ description: 'Last date' }),
  avg_long_margin: z.number().optional().openapi({ description: 'Average long margin' }),
  avg_short_margin: z.number().optional().openapi({ description: 'Average short margin' }),
});

export const StatementsRecordSchema = z.object({
  disclosedDate: z.string().openapi({ description: 'Disclosed date', example: '2024-05-10' }),
  typeOfCurrentPeriod: z
    .string()
    .openapi({ description: 'Period type (FY, 1Q, 2Q, 3Q). Empty string if unknown.', example: 'FY' }),
  typeOfDocument: z.string().openapi({
    description: 'Document type. Empty string if unknown.',
    example: 'FYFinancialStatements_Consolidated_JP',
  }),
  earningsPerShare: z.number().nullable().openapi({ description: 'EPS', example: 150.5 }),
  profit: z.number().nullable().openapi({ description: 'Net profit', example: 1000000000 }),
  equity: z.number().nullable().openapi({ description: 'Shareholders equity', example: 5000000000 }),
  nextYearForecastEarningsPerShare: z
    .number()
    .nullable()
    .openapi({ description: 'Next year forecast EPS', example: 160.0 }),
  bps: z.number().nullable().openapi({ description: 'Book value per share', example: 2500.0 }),
  sales: z.number().nullable().openapi({ description: 'Net sales', example: 30000000000 }),
  operatingProfit: z.number().nullable().openapi({ description: 'Operating profit', example: 2000000000 }),
  ordinaryProfit: z.number().nullable().openapi({ description: 'Ordinary profit (J-GAAP)', example: 2100000000 }),
  operatingCashFlow: z.number().nullable().openapi({ description: 'Cash flow from operations', example: 1500000000 }),
  dividendFY: z.number().nullable().openapi({ description: 'Dividend per share (FY)', example: 50.0 }),
  forecastEps: z.number().nullable().openapi({ description: 'Forecast EPS for current FY', example: 155.0 }),
  // Cash flow extended fields (added 2026-01)
  investingCashFlow: z.number().nullable().openapi({ description: 'Cash flow from investing', example: -500000000 }),
  financingCashFlow: z.number().nullable().openapi({ description: 'Cash flow from financing', example: -300000000 }),
  cashAndEquivalents: z.number().nullable().openapi({ description: 'Cash and cash equivalents', example: 2000000000 }),
  totalAssets: z.number().nullable().openapi({ description: 'Total assets', example: 10000000000 }),
  sharesOutstanding: z.number().nullable().openapi({ description: 'Shares outstanding', example: 100000000 }),
  treasuryShares: z.number().nullable().openapi({ description: 'Treasury shares', example: 5000000 }),
});

export const SectorMappingRecordSchema = z.object({
  sector_code: z.string().openapi({ description: 'Sector code', example: '3650' }),
  sector_name: z.string().openapi({ description: 'Sector name', example: '電気機器' }),
  index_code: z.string().openapi({ description: 'Index code', example: 'I1001' }),
  index_name: z.string().openapi({ description: 'Index name', example: '電気機器指数' }),
});

export const StockSectorMappingItemSchema = z.object({
  code: z.string().openapi({ description: 'Stock code', example: '7203' }),
  sector33Name: z.string().openapi({ description: 'Sector name (33 categories)', example: '輸送用機器' }),
});

export const SectorWithCountRecordSchema = SectorMappingRecordSchema.extend({
  stock_count: z.number().int().openapi({ description: 'Number of stocks in this sector', example: 45 }),
});

/**
 * Sector name parameter schema
 * Allows Japanese characters (Hiragana, Katakana, Kanji, punctuation) and ASCII alphanumeric
 */
export const SectorNameSchema = z
  .string()
  .min(1)
  .max(50)
  .regex(/^[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\u3000-\u303Fa-zA-Z0-9_\-\s・]+$/, {
    message: 'Sector name must contain only Japanese characters, alphanumeric, or common punctuation',
  })
  .openapi({ description: 'Sector name', example: '電気機器' });

// ===== Array Response Schemas =====

export const OHLCVResponseSchema = z.array(OHLCVRecordSchema);
export const OHLCResponseSchema = z.array(OHLCRecordSchema);
export const StockListResponseSchema = z.array(StockListItemSchema);
export const IndexListResponseSchema = z.array(IndexListItemSchema);
export const MarginResponseSchema = z.array(MarginRecordSchema);
export const MarginListResponseSchema = z.array(MarginListItemSchema);
export const StatementsResponseSchema = z.array(StatementsRecordSchema);
export const SectorMappingResponseSchema = z.array(SectorMappingRecordSchema);
export const StockSectorMappingResponseSchema = z.array(StockSectorMappingItemSchema);
export const SectorStocksResponseSchema = z.array(z.string());
export const SectorWithCountResponseSchema = z.array(SectorWithCountRecordSchema);
export const BatchOHLCVResponseSchema = z.record(z.string(), OHLCVResponseSchema);
export const BatchMarginResponseSchema = z.record(z.string(), MarginResponseSchema);
export const BatchStatementsResponseSchema = z.record(z.string(), StatementsResponseSchema);

// ===== Type exports =====

export type DateRangeQuery = z.infer<typeof DateRangeQuerySchema>;
export type StatementsQuery = z.infer<typeof StatementsQuerySchema>;
export type PeriodType = z.infer<typeof PeriodTypeSchema>;
export type OHLCVQuery = z.infer<typeof OHLCVQuerySchema>;
export type Timeframe = z.infer<typeof TimeframeSchema>;
export type StockListQuery = z.infer<typeof StockListQuerySchema>;
export type IndexListQuery = z.infer<typeof IndexListQuerySchema>;
export type MarginListQuery = z.infer<typeof MarginListQuerySchema>;
export type BatchOHLCVQuery = z.infer<typeof BatchOHLCVQuerySchema>;
export type BatchOHLCVResponse = z.infer<typeof BatchOHLCVResponseSchema>;
export type BatchMarginQuery = z.infer<typeof BatchMarginQuerySchema>;
export type BatchMarginResponse = z.infer<typeof BatchMarginResponseSchema>;
export type BatchStatementsQuery = z.infer<typeof BatchStatementsQuerySchema>;
export type BatchStatementsResponse = z.infer<typeof BatchStatementsResponseSchema>;
export type OHLCVRecord = z.infer<typeof OHLCVRecordSchema>;
export type OHLCRecord = z.infer<typeof OHLCRecordSchema>;
export type StockListItem = z.infer<typeof StockListItemSchema>;
export type IndexListItem = z.infer<typeof IndexListItemSchema>;
export type MarginRecord = z.infer<typeof MarginRecordSchema>;
export type MarginListItem = z.infer<typeof MarginListItemSchema>;
export type StatementsRecord = z.infer<typeof StatementsRecordSchema>;
export type SectorMappingRecord = z.infer<typeof SectorMappingRecordSchema>;
export type StockSectorMappingItem = z.infer<typeof StockSectorMappingItemSchema>;
export type SectorWithCountRecord = z.infer<typeof SectorWithCountRecordSchema>;
