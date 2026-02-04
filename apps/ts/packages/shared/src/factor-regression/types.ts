/**
 * Factor Analysis Types
 *
 * Type definitions for factor regression analysis
 */

import type { IndexCategory } from '../db/schema/market-schema';

/**
 * Daily return data point
 */
export interface DailyReturn {
  date: string; // YYYY-MM-DD
  return: number; // Daily return as decimal (-0.05 = -5%)
}

/**
 * OLS regression result
 */
export interface OLSResult {
  alpha: number; // Intercept
  beta: number; // Slope coefficient
  rSquared: number; // Coefficient of determination (0-1)
  residuals: number[]; // Residual array aligned with input data
}

/**
 * Index match result for residual regression
 */
export interface IndexMatch {
  indexCode: string; // e.g., '0040', '0085'
  indexName: string; // e.g., 'Foods', 'TOPIX-17 Automobiles'
  category: IndexCategory;
  rSquared: number; // R-squared from residual regression
  beta: number; // Beta from residual regression
}

/**
 * Factor regression result
 */
export interface FactorRegressionResult {
  stockCode: string;
  companyName?: string;

  // Stage 1: Market regression
  marketBeta: number; // beta_m from TOPIX regression
  marketRSquared: number; // R-squared from TOPIX regression

  // Stage 2: Residual factor matching (top 3 from each category)
  sector17Matches: IndexMatch[]; // Top 3 from 17 sectors
  sector33Matches: IndexMatch[]; // Top 3 from 33 sectors
  topixStyleMatches: IndexMatch[]; // Top 3 from TOPIX size + MARKET + STYLE (excluding 0000)

  // Analysis metadata
  analysisDate: string;
  dataPoints: number; // Number of trading days used
  dateRange: {
    from: string;
    to: string;
  };
}

/**
 * Options for factor regression analysis
 */
export interface FactorRegressionOptions {
  lookbackDays?: number; // Default: 252 (1 year of trading days)
  minDataPoints?: number; // Minimum required data points (default: 60)
}

import { BadRequestError } from '../errors';

/**
 * Error class for factor regression analysis
 */
export class FactorRegressionError extends BadRequestError {
  override readonly code: string;
  public readonly details?: Record<string, unknown>;

  constructor(message: string, code: string, details?: Record<string, unknown>) {
    super(message);
    this.code = code;
    this.details = details;
    this.name = 'FactorRegressionError';
  }
}

// ===== PORTFOLIO FACTOR REGRESSION =====

/**
 * Portfolio weight information for regression analysis
 */
export interface PortfolioWeight {
  /** Stock code (4-digit) */
  code: string;
  /** Company name */
  companyName: string;
  /** Weight (0-1, sum to 1) */
  weight: number;
  /** Latest stock price used for weight calculation */
  latestPrice: number;
  /** Current market value (quantity × latestPrice) */
  marketValue: number;
  /** Holding quantity */
  quantity: number;
}

/**
 * Stock excluded from portfolio analysis
 */
export interface ExcludedStock {
  /** Stock code (4-digit) */
  code: string;
  /** Company name */
  companyName: string;
  /** Reason for exclusion */
  reason: string;
}

/**
 * Portfolio factor regression result
 */
export interface PortfolioFactorRegressionResult {
  /** Portfolio ID */
  portfolioId: number;
  /** Portfolio name */
  portfolioName: string;

  // Weight information
  /** Weight breakdown for each stock */
  weights: PortfolioWeight[];
  /** Total portfolio market value */
  totalValue: number;
  /** Number of stocks in portfolio */
  stockCount: number;
  /** Number of stocks included in analysis */
  includedStockCount: number;

  // Stage 1: Market regression
  /** Market beta from TOPIX regression */
  marketBeta: number;
  /** R-squared from TOPIX regression */
  marketRSquared: number;

  // Stage 2: Residual factor matching (top 3 from each category)
  /** Top 3 matches from TOPIX-17 sectors */
  sector17Matches: IndexMatch[];
  /** Top 3 matches from 33 sectors */
  sector33Matches: IndexMatch[];
  /** Top 3 matches from TOPIX size + market + style */
  topixStyleMatches: IndexMatch[];

  // Analysis metadata
  /** Analysis date (YYYY-MM-DD) */
  analysisDate: string;
  /** Number of trading days used */
  dataPoints: number;
  /** Date range of analysis */
  dateRange: {
    from: string;
    to: string;
  };

  /** Stocks excluded from analysis */
  excludedStocks: ExcludedStock[];
}
