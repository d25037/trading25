import { z } from '@hono/zod-openapi';

/**
 * Fundamentals path parameters schema
 */
export const FundamentalsPathParamsSchema = z
  .object({
    symbol: z.string().length(4).openapi({
      description: 'Stock symbol (4-character code)',
      example: '7203',
    }),
  })
  .openapi('FundamentalsPathParams');

/**
 * Fundamentals query parameters schema
 */
export const FundamentalsQuerySchema = z
  .object({
    from: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format')
      .optional()
      .openapi({
        description: 'Start date filter (YYYY-MM-DD)',
        example: '2020-01-01',
      }),
    to: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD format')
      .optional()
      .openapi({
        description: 'End date filter (YYYY-MM-DD)',
        example: '2024-12-31',
      }),
    periodType: z.enum(['all', 'FY', '1Q', '2Q', '3Q']).default('all').openapi({
      description: 'Filter by period type (default: all)',
      example: 'all',
    }),
    preferConsolidated: z
      .enum(['true', 'false'])
      .default('true')
      .transform((val) => val !== 'false')
      .openapi({
        description: 'Prefer consolidated over non-consolidated data (default: true)',
        example: 'true',
      }),
  })
  .openapi('FundamentalsQuery');

/**
 * Fundamental data point schema
 */
export const FundamentalDataPointSchema = z
  .object({
    date: z.string().openapi({ description: 'Period end date (YYYY-MM-DD)', example: '2024-03-31' }),
    disclosedDate: z.string().openapi({ description: 'Disclosure date (YYYY-MM-DD)', example: '2024-05-10' }),
    periodType: z.string().openapi({ description: 'Period type (FY, 1Q, 2Q, 3Q)', example: 'FY' }),
    isConsolidated: z.boolean().openapi({ description: 'Whether data is consolidated', example: true }),
    accountingStandard: z
      .string()
      .nullable()
      .openapi({ description: 'Accounting standard (JGAAP, IFRS, US GAAP)', example: 'IFRS' }),
    // Core metrics
    roe: z.number().nullable().openapi({ description: 'Return on Equity (%)', example: 15.5 }),
    eps: z.number().nullable().openapi({ description: 'Earnings Per Share (JPY)', example: 250.5 }),
    dilutedEps: z.number().nullable().openapi({ description: 'Diluted EPS (JPY)', example: 248.0 }),
    bps: z.number().nullable().openapi({ description: 'Book Value Per Share (JPY)', example: 3500.0 }),
    adjustedEps: z
      .number()
      .nullable()
      .openapi({ description: 'Adjusted EPS using share count (JPY)', example: 250.5 }),
    adjustedForecastEps: z
      .number()
      .nullable()
      .openapi({ description: 'Adjusted forecast EPS using share count (JPY)', example: 280.0 }),
    adjustedBps: z
      .number()
      .nullable()
      .openapi({ description: 'Adjusted BPS using share count (JPY)', example: 3500.0 }),
    per: z
      .number()
      .nullable()
      .openapi({ description: 'Price to Earnings Ratio (calculated with disclosure date price)', example: 12.5 }),
    pbr: z
      .number()
      .nullable()
      .openapi({ description: 'Price to Book Ratio (calculated with disclosure date price)', example: 1.2 }),
    // Profitability metrics
    roa: z.number().nullable().openapi({ description: 'Return on Assets (%)', example: 8.5 }),
    operatingMargin: z.number().nullable().openapi({ description: 'Operating Profit Margin (%)', example: 12.0 }),
    netMargin: z.number().nullable().openapi({ description: 'Net Profit Margin (%)', example: 8.0 }),
    // Context data
    stockPrice: z.number().nullable().openapi({ description: 'Stock price at disclosure date (JPY)', example: 2500 }),
    netProfit: z.number().nullable().openapi({ description: 'Net profit (millions of JPY)', example: 250000 }),
    equity: z.number().nullable().openapi({ description: "Shareholders' equity (millions of JPY)", example: 1500000 }),
    totalAssets: z.number().nullable().openapi({ description: 'Total assets (millions of JPY)', example: 3000000 }),
    netSales: z.number().nullable().openapi({ description: 'Net sales / Revenue (millions of JPY)', example: 5000000 }),
    operatingProfit: z
      .number()
      .nullable()
      .openapi({ description: 'Operating profit (millions of JPY)', example: 600000 }),
    // Cash flow data
    cashFlowOperating: z
      .number()
      .nullable()
      .openapi({ description: 'Cash flows from operating activities (millions of JPY)', example: 500000 }),
    cashFlowInvesting: z
      .number()
      .nullable()
      .openapi({ description: 'Cash flows from investing activities (millions of JPY)', example: -200000 }),
    cashFlowFinancing: z
      .number()
      .nullable()
      .openapi({ description: 'Cash flows from financing activities (millions of JPY)', example: -150000 }),
    cashAndEquivalents: z
      .number()
      .nullable()
      .openapi({ description: 'Cash and cash equivalents (millions of JPY)', example: 800000 }),
    // FCF (Free Cash Flow) metrics
    fcf: z
      .number()
      .nullable()
      .openapi({ description: 'Free Cash Flow = CFO + CFI (millions of JPY)', example: 300000 }),
    fcfYield: z.number().nullable().openapi({ description: 'FCF Yield = FCF / Market Cap × 100 (%)', example: 5.5 }),
    fcfMargin: z.number().nullable().openapi({ description: 'FCF Margin = FCF / Net Sales × 100 (%)', example: 8.0 }),
    // Forecast EPS
    forecastEps: z
      .number()
      .nullable()
      .openapi({ description: 'Forecast EPS for current/next fiscal year (JPY)', example: 280.0 }),
    forecastEpsChangeRate: z
      .number()
      .nullable()
      .openapi({ description: 'Change rate from actual EPS to forecast EPS (%)', example: 12.0 }),
    // Revised forecast EPS from latest Q (only set on the latest FY entry when Q revision differs)
    revisedForecastEps: z
      .number()
      .nullable()
      .optional()
      .openapi({ description: 'Revised forecast EPS from latest quarterly statement (JPY)', example: 452.34 }),
    revisedForecastSource: z
      .string()
      .nullable()
      .optional()
      .openapi({ description: 'Source period type of revised forecast (e.g. 1Q, 2Q, 3Q)', example: '3Q' }),
    // Previous period cash flow data
    prevCashFlowOperating: z.number().nullable().openapi({
      description: 'Previous period cash flows from operating activities (millions of JPY)',
      example: 450000,
    }),
    prevCashFlowInvesting: z.number().nullable().openapi({
      description: 'Previous period cash flows from investing activities (millions of JPY)',
      example: -180000,
    }),
    prevCashFlowFinancing: z.number().nullable().openapi({
      description: 'Previous period cash flows from financing activities (millions of JPY)',
      example: -120000,
    }),
    prevCashAndEquivalents: z
      .number()
      .nullable()
      .openapi({ description: 'Previous period cash and cash equivalents (millions of JPY)', example: 750000 }),
  })
  .openapi('FundamentalDataPoint', {
    description: 'Fundamental metrics for a single period',
  });

/**
 * Daily valuation data point schema (PER/PBR with daily close prices)
 */
export const DailyValuationDataPointSchema = z
  .object({
    date: z.string().openapi({ description: 'Trading date (YYYY-MM-DD)', example: '2024-12-18' }),
    close: z.number().openapi({ description: 'Stock close price (JPY)', example: 2500 }),
    per: z.number().nullable().openapi({
      description: 'Price to Earnings Ratio (calculated with FY EPS)',
      example: 12.5,
    }),
    pbr: z.number().nullable().openapi({
      description: 'Price to Book Ratio (calculated with FY BPS)',
      example: 1.2,
    }),
    marketCap: z.number().nullable().openapi({
      description: 'Market capitalization (JPY)',
      example: 1200000000000,
    }),
  })
  .openapi('DailyValuationDataPoint', {
    description: 'Daily valuation metrics calculated with trading day close price and FY EPS/BPS',
  });

/**
 * Fundamentals response schema
 */
export const FundamentalsResponseSchema = z
  .object({
    symbol: z.string().openapi({ description: 'Stock symbol (4-character code)', example: '7203' }),
    companyName: z.string().optional().openapi({ description: 'Company name', example: 'Toyota Motor Corporation' }),
    data: z.array(FundamentalDataPointSchema).openapi({ description: 'Time series data sorted by date descending' }),
    latestMetrics: FundamentalDataPointSchema.optional().openapi({
      description: 'Most recent metrics (convenience field)',
    }),
    dailyValuation: z.array(DailyValuationDataPointSchema).optional().openapi({
      description: 'Daily PER/PBR time series (calculated with daily close prices and FY EPS/BPS)',
    }),
    lastUpdated: z
      .string()
      .openapi({ description: 'Last updated timestamp (ISO 8601 format)', example: '2024-12-18T12:00:00Z' }),
  })
  .openapi('FundamentalsResponse', {
    description: 'Fundamental analysis response with time series data',
  });

/**
 * Type exports for use in services
 */
export type FundamentalsPathParams = z.input<typeof FundamentalsPathParamsSchema>;
export type FundamentalsQuery = z.input<typeof FundamentalsQuerySchema>;
export type FundamentalDataPoint = z.infer<typeof FundamentalDataPointSchema>;
export type DailyValuationDataPoint = z.infer<typeof DailyValuationDataPointSchema>;
export type FundamentalsResponse = z.infer<typeof FundamentalsResponseSchema>;
