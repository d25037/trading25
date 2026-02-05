import type { JQuantsStatement } from '../types/jquants';

/**
 * Result of ROE calculation
 */
export interface ROEResult {
  /** Return on Equity as percentage */
  roe: number;
  /** Net profit used in calculation */
  netProfit: number;
  /** Shareholders' equity used in calculation */
  equity: number;
  /** Statement data used for calculation */
  statement: JQuantsStatement;
  /** Calculation metadata */
  metadata: ROEMetadata;
}

/**
 * Metadata for ROE calculation
 */
export interface ROEMetadata {
  /** Stock code */
  code: string;
  /** Period type (FY, 1Q, 2Q, 3Q) */
  periodType: string;
  /** Period start date */
  periodStart: string;
  /** Period end date */
  periodEnd: string;
  /** Document type */
  documentType: string;
  /** Whether this is consolidated financial statement */
  isConsolidated: boolean;
  /** Accounting standards used (JGAAP, IFRS, US GAAP) */
  accountingStandard: string | null;
  /** Whether calculation uses annualized figures for quarterly data */
  isAnnualized?: boolean;
}

/**
 * Options for ROE calculation
 */
export interface ROECalculationOptions {
  /** Whether to annualize quarterly results (default: true for 1Q, 2Q, 3Q) */
  annualize?: boolean;
  /** Prefer consolidated over non-consolidated data (default: true) */
  preferConsolidated?: boolean;
  /** Minimum equity threshold to avoid division issues (default: 1000) */
  minEquityThreshold?: number;
}

import { BadRequestError } from '../errors';

/**
 * Error thrown when ROE cannot be calculated
 */
export class ROECalculationError extends BadRequestError {
  override readonly code: string;
  public readonly statement?: JQuantsStatement;
  public readonly reason?: string;

  constructor(message: string, code: string, statement?: JQuantsStatement, reason?: string) {
    super(message);
    this.code = code;
    this.statement = statement;
    this.reason = reason;
    this.name = 'ROECalculationError';
  }
}

/**
 * Financial ratios result structure (extensible for other ratios)
 */
export interface FinancialRatios {
  /** Return on Equity */
  roe?: number;
  /** Return on Assets */
  roa?: number;
  /** Debt to Equity ratio */
  debtToEquity?: number;
  /** Price to Earnings ratio (requires market data) */
  pe?: number;
  /** Price to Book ratio (requires market data) */
  pb?: number;
}

/**
 * Fundamental analysis result combining multiple metrics
 */
export interface FundamentalAnalysisResult {
  /** Stock code */
  code: string;
  /** Company name if available */
  companyName?: string;
  /** Financial ratios */
  ratios: FinancialRatios;
  /** Raw financial data */
  financialData: JQuantsStatement;
  /** Analysis timestamp */
  analyzedAt: Date;
  /** Metadata about the analysis */
  metadata: {
    periodType: string;
    periodEnd: string;
    dataSource: string;
    warnings?: string[];
  };
}
