/**
 * Dataset V2 - Runtime Type Validators
 * Zod-based runtime type validation for API responses and data integrity
 */

import { type ZodIssue, z } from 'zod';
import type { MarginData, MarketType, SectorData, StatementsData, StockData, StockInfo, TopixData } from '../types';

// ===== MARKET TYPE VALIDATION =====

export const MarketTypeSchema = z.enum(['prime', 'standard', 'growth']);

// ===== STOCK DATA VALIDATION =====

export const StockDataSchema = z.object({
  code: z.string().min(4, 'Stock code must be at least 4 characters'),
  date: z.date(),
  open: z.number().positive('Open price must be positive'),
  high: z.number().positive('High price must be positive'),
  low: z.number().positive('Low price must be positive'),
  close: z.number().positive('Close price must be positive'),
  volume: z.number().nonnegative('Volume must be non-negative'),
  adjustmentFactor: z.number().optional(),
});

export const StockInfoSchema = z.object({
  code: z.string().min(4, 'Stock code must be at least 4 characters'),
  companyName: z.string().min(1, 'Company name is required'),
  companyNameEnglish: z.string(),
  marketCode: MarketTypeSchema,
  marketName: z.string().min(1, 'Market name is required'),
  sector17Code: z.string().min(1, 'Sector17 code must be at least 1 character'),
  sector17Name: z.string().min(1, 'Sector17 name is required'),
  sector33Code: z.string().min(2, 'Sector33 code must be at least 2 characters'),
  sector33Name: z.string().min(1, 'Sector33 name is required'),
  scaleCategory: z.string(), // Allow empty scale category
  listedDate: z.date(),
});

// ===== MARGIN DATA VALIDATION =====

export const MarginDataSchema = z.object({
  code: z.string().min(4, 'Stock code must be at least 4 characters'),
  date: z.date(),
  shortMarginVolume: z.number().nullable(),
  longMarginVolume: z.number().nullable(),
});

// ===== TOPIX DATA VALIDATION =====

export const TopixDataSchema = z.object({
  date: z.date(),
  open: z.number().positive('Open price must be positive'),
  high: z.number().positive('High price must be positive'),
  low: z.number().positive('Low price must be positive'),
  close: z.number().positive('Close price must be positive'),
  volume: z.number().nonnegative('Volume must be non-negative'),
});

// ===== SECTOR DATA VALIDATION =====

export const SectorDataSchema = z.object({
  sectorCode: z.string().min(2, 'Sector code must be at least 2 characters'),
  sectorName: z.string(), // Allow empty strings - name can be populated from index master data
  date: z.date(),
  open: z.number().nonnegative('Open price must be non-negative'), // Some indices can have 0 values
  high: z.number().nonnegative('High price must be non-negative'),
  low: z.number().nonnegative('Low price must be non-negative'),
  close: z.number().nonnegative('Close price must be non-negative'),
  volume: z.number().nonnegative('Volume must be non-negative'),
});

// ===== STATEMENTS DATA VALIDATION =====

export const StatementsDataSchema = z.object({
  code: z.string().min(4, 'Stock code must be at least 4 characters'),
  disclosedDate: z.date(),
  earningsPerShare: z.number().nullable(),
  profit: z.number().nullable(),
  equity: z.number().nullable(),
  typeOfCurrentPeriod: z.string(), // Allow empty strings
  typeOfDocument: z.string(), // Allow empty strings
  nextYearForecastEarningsPerShare: z.number().nullable(),
  // Extended financial metrics (added 2026-01)
  bps: z.number().nullable(), // Book Value Per Share
  sales: z.number().nullable(), // Net Sales
  operatingProfit: z.number().nullable(), // Operating Profit
  ordinaryProfit: z.number().nullable(), // Ordinary Profit
  operatingCashFlow: z.number().nullable(), // Cash Flow from Operations
  dividendFY: z.number().nullable(), // Dividend Per Share Fiscal Year
  forecastEps: z.number().nullable(), // Forecast EPS
  // Cash flow extended metrics (added 2026-01)
  investingCashFlow: z.number().nullable(), // Cash Flow from Investing
  financingCashFlow: z.number().nullable(), // Cash Flow from Financing
  cashAndEquivalents: z.number().nullable(), // Cash and Cash Equivalents
  totalAssets: z.number().nullable(), // Total Assets
  sharesOutstanding: z.number().nullable(), // Shares Outstanding at FY End
  treasuryShares: z.number().nullable(), // Treasury Shares at FY End
});

// ===== ARRAY VALIDATION =====

export const StockDataArraySchema = z.array(StockDataSchema);
export const StockInfoArraySchema = z.array(StockInfoSchema);
export const MarginDataArraySchema = z.array(MarginDataSchema);
export const TopixDataArraySchema = z.array(TopixDataSchema);
export const SectorDataArraySchema = z.array(SectorDataSchema);
export const StatementsDataArraySchema = z.array(StatementsDataSchema);

// ===== VALIDATION FUNCTIONS =====

/**
 * Validate single stock data with detailed error reporting
 */
export function validateStockData(data: unknown): StockData {
  try {
    return StockDataSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Stock data validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate single stock info with detailed error reporting
 */
export function validateStockInfo(data: unknown): StockInfo {
  try {
    return StockInfoSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Stock info validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate margin data with detailed error reporting
 */
export function validateMarginData(data: unknown): MarginData {
  try {
    return MarginDataSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Margin data validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate TOPIX data with detailed error reporting
 */
export function validateTopixData(data: unknown): TopixData {
  try {
    return TopixDataSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`TOPIX data validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate sector data with detailed error reporting
 */
export function validateSectorData(data: unknown): SectorData {
  try {
    return SectorDataSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Sector data validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate statements data with detailed error reporting
 */
export function validateStatementsData(data: unknown): StatementsData {
  const isDebugMode = process.env.DATASET_DEBUG === 'true';

  try {
    const result = StatementsDataSchema.parse(data);

    if (isDebugMode) {
      const typedData = data as Record<string, unknown>;
      console.log(`[VALIDATOR] ✅ Statements validation PASSED for ${typedData.code}:`, {
        code: typedData.code,
        disclosedDate: typedData.disclosedDate,
        earningsPerShare: typedData.earningsPerShare,
        profit: typedData.profit,
        equity: typedData.equity,
        typeOfCurrentPeriod: typedData.typeOfCurrentPeriod,
        typeOfDocument: typedData.typeOfDocument,
      });
    }

    return result;
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');

      if (isDebugMode) {
        const typedData = data as Record<string, unknown>;
        console.log(`[VALIDATOR] ❌ Statements validation FAILED for ${typedData.code || 'unknown'}:`, {
          errorDetails,
          rawData: data,
          issues: error.issues,
        });
      }

      throw new Error(`Statements data validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

// ===== ARRAY VALIDATION FUNCTIONS =====

/**
 * Validate array of stock data
 */
export function validateStockDataArray(data: unknown): StockData[] {
  try {
    return StockDataArraySchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Stock data array validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate array of stock info
 */
export function validateStockInfoArray(data: unknown): StockInfo[] {
  try {
    return StockInfoArraySchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorDetails = error.issues.map((err: ZodIssue) => `${err.path.join('.')}: ${err.message}`).join(', ');
      throw new Error(`Stock info array validation failed: ${errorDetails}`);
    }
    throw error;
  }
}

/**
 * Validate market type
 */
export function validateMarketType(data: unknown): MarketType {
  try {
    return MarketTypeSchema.parse(data);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new Error(`Invalid market type: expected 'prime', 'standard', or 'growth', got '${data}'`);
    }
    throw error;
  }
}

// ===== SAFE VALIDATION FUNCTIONS (no throw) =====

/**
 * Safely validate data with result object instead of throwing
 */
export function safeValidateStockInfo(
  data: unknown
): { success: true; data: StockInfo } | { success: false; error: string } {
  try {
    const validated = validateStockInfo(data);
    return { success: true, data: validated };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * Safely validate stock data array with partial success reporting
 */
export function safeValidateStockDataArray(data: unknown[]): {
  valid: StockData[];
  invalid: Array<{ index: number; data: unknown; error: string }>;
} {
  const valid: StockData[] = [];
  const invalid: Array<{ index: number; data: unknown; error: string }> = [];

  for (let i = 0; i < data.length; i++) {
    try {
      const validated = validateStockData(data[i]);
      valid.push(validated);
    } catch (error) {
      invalid.push({
        index: i,
        data: data[i],
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return { valid, invalid };
}
