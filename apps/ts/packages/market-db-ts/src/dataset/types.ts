/**
 * Dataset V2 - Core Types
 * Simplified type definitions for the new architecture
 */

// ===== Core Data Types =====

export interface StockData {
  code: string;
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  adjustmentFactor?: number;
}

export interface StockInfo {
  code: string;
  companyName: string;
  companyNameEnglish: string;
  marketCode: string;
  marketName: string;
  sector17Code: string;
  sector17Name: string;
  sector33Code: string;
  sector33Name: string;
  scaleCategory: string;
  listedDate: Date;
}

export interface MarginData {
  code: string;
  date: Date;
  shortMarginVolume: number | null;
  longMarginVolume: number | null;
}

export interface TopixData {
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  // Note: JQuants API does not provide volume for TOPIX data
}

export interface SectorData {
  sectorCode: string;
  sectorName: string;
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  // Note: JQuants API does not provide volume for sector indices
}

export interface StatementsData {
  code: string;
  disclosedDate: Date;
  earningsPerShare: number | null;
  profit: number | null;
  equity: number | null;
  typeOfCurrentPeriod: string;
  typeOfDocument: string;
  nextYearForecastEarningsPerShare: number | null;
  // Extended financial metrics (added 2026-01)
  bps: number | null; // Book Value Per Share (1株当たり純資産)
  sales: number | null; // Net Sales (売上高)
  operatingProfit: number | null; // Operating Profit (営業利益)
  ordinaryProfit: number | null; // Ordinary Profit (経常利益)
  operatingCashFlow: number | null; // Cash Flow from Operations (営業CF)
  dividendFY: number | null; // Dividend Per Share Fiscal Year (通期配当)
  forecastEps: number | null; // Forecast EPS for current FY (EPS予想)
  // Cash flow extended metrics (added 2026-01)
  investingCashFlow: number | null; // Cash Flow from Investing (投資CF)
  financingCashFlow: number | null; // Cash Flow from Financing (財務CF)
  cashAndEquivalents: number | null; // Cash and Cash Equivalents (現金及び現金同等物)
  totalAssets: number | null; // Total Assets (総資産)
  sharesOutstanding: number | null; // Shares Outstanding at FY End (発行済株式数)
  treasuryShares: number | null; // Treasury Shares at FY End (自己株式数)
}

// ===== Configuration Types =====

export type MarketType = 'prime' | 'standard' | 'growth';

export interface DatasetConfig {
  outputPath: string;
  preset?: string; // Preset name used to create this dataset (stored as metadata)
  markets: MarketType[];
  includeMargin: boolean;
  includeTOPIX: boolean;
  includeSectorIndices: boolean;
  includeStatements: boolean;
  maxStocks?: number; // For testing/sampling
  startDate?: Date; // Default: 10 years ago
  endDate?: Date; // Default: today
  // Sector filtering options
  excludeEmptySector33?: boolean; // Exclude stocks without sector33Code (default: true)
  excludeSectorCodes?: string[]; // Exclude specific sector codes (default: ["9999"] for ETFs)
  // V1 compatibility extensions
  samplingConfig?: SamplingConfig;
  marketCapFilter?: number; // Minimum market cap filter
  scaleCategories?: ScaleCategory[]; // TOPIX scale category filters (inclusion)
  excludeScaleCategories?: ScaleCategory[]; // TOPIX scale category exclusion filters
  overwrite?: boolean; // Allow overwriting existing files
}

export interface SamplingConfig {
  method: 'random';
  size: number;
  seed?: number; // For reproducible sampling
  stratification?: {
    byMarket?: boolean;
    bySector?: boolean;
  };
  constraints?: {
    minRecordCount?: number;
    requireMarginData?: boolean;
  };
  iterations?: number;
}

export type ScaleCategory = 'TOPIX Core30' | 'TOPIX Large70' | 'TOPIX Mid400' | 'TOPIX Small 1' | 'TOPIX Small 2';

export interface CustomConfigOptions {
  markets?: MarketType[];
  includeMargin?: boolean;
  minMarketCap?: number;
  maxStockCount?: number;
}

export interface DateRange {
  from: Date;
  to: Date;
}

// ===== Progress & Results =====

export interface ProgressInfo {
  stage: 'stocks' | 'quotes' | 'margin' | 'topix' | 'sectors' | 'statements' | 'saving';
  processed: number;
  total: number;
  currentItem?: string;
  errors: string[];
}

export type ProgressCallback = (progress: ProgressInfo) => void;

export interface BuildResult {
  success: boolean;
  totalStocks: number;
  processedStocks: number;
  errors: string[];
  warnings: string[];
  databasePath: string;
}

export interface DatasetStats {
  totalStocks: number;
  totalQuotes: number;
  dateRange: DateRange;
  markets: string[];
  hasMarginData: boolean;
  hasTOPIXData: boolean;
  hasSectorData: boolean;
  hasStatementsData: boolean;
  databaseSize: number;
  lastUpdated: Date;
}

/**
 * Statements field coverage statistics
 * Tracks how many records have non-null values for each financial metric
 */
export interface StatementsFieldCoverage {
  total: number; // Total statements records
  totalFY: number; // FY (full year) records only
  totalHalf: number; // FY + 2Q (half year) records
  hasExtendedFields: boolean; // Whether the extended fields exist in the schema
  hasCashFlowFields: boolean; // Whether the cash flow extended fields exist in the schema
  // Core fields (always present)
  earningsPerShare: number; // Records with EPS data
  profit: number; // Records with profit data
  equity: number; // Records with equity data
  nextYearForecastEps: number; // Records with next year forecast EPS (FY only)
  // Extended fields (added 2026-01)
  bps: number; // Records with BPS data (FY only)
  sales: number; // Records with sales data
  operatingProfit: number; // Records with operating profit data
  ordinaryProfit: number; // Records with ordinary profit (J-GAAP only)
  operatingCashFlow: number; // Records with operating cash flow (FY/2Q only)
  dividendFY: number; // Records with dividend data (FY only)
  forecastEps: number; // Records with forecast EPS data
  // Cash flow extended fields (added 2026-01)
  investingCashFlow: number; // Records with investing cash flow data (primarily FY/2Q)
  financingCashFlow: number; // Records with financing cash flow data (primarily FY/2Q)
  cashAndEquivalents: number; // Records with cash and equivalents data (primarily FY/2Q)
  totalAssets: number; // Records with total assets data (all periods)
  sharesOutstanding: number; // Records with shares outstanding data (all periods)
  treasuryShares: number; // Records with treasury shares data (all periods)
}

// ===== Error Types =====

import { InternalError } from '../errors';

export class DatasetError extends InternalError {
  override readonly code: string = 'DATASET_ERROR';
  public retryable: boolean;

  constructor(message: string, code?: string, retryable: boolean = false, cause?: Error) {
    super(message, cause);
    if (code) {
      this.code = code;
    }
    this.retryable = retryable;
    this.name = 'DatasetError';
  }
}

export class ApiError extends DatasetError {
  constructor(message: string, code?: string, cause?: Error) {
    super(message, code, true, cause);
    this.name = 'ApiError';
  }
}

export class DatabaseError extends DatasetError {
  constructor(message: string, cause?: Error) {
    super(message, 'DATABASE_ERROR', false, cause);
    this.name = 'DatabaseError';
  }
}

// ===== Debug Configuration =====

export interface DebugConfig {
  enabled: boolean;
  verbose?: boolean;
  trace?: boolean;
  logApiCalls?: boolean;
  logRateLimiting?: boolean;
  logValidation?: boolean;
}

export const DEFAULT_DEBUG_CONFIG: DebugConfig = {
  enabled: false,
  verbose: false,
  trace: false,
  logApiCalls: true,
  logRateLimiting: true,
  logValidation: true,
};

/**
 * Create debug configuration from CLI options
 */
export function createDebugConfig(options: { debug?: boolean; verbose?: boolean; trace?: boolean }): DebugConfig {
  if (!options.debug) {
    return DEFAULT_DEBUG_CONFIG;
  }

  return {
    enabled: true,
    verbose: options.verbose || options.debug,
    trace: options.trace || false,
    logApiCalls: true,
    logRateLimiting: true,
    logValidation: true,
  };
}
