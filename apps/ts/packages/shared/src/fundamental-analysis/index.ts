// Fundamental Analysis Module
// Core functions and types for financial ratio calculations (ROE, ROA, PER, PBR, EPS, BPS)

export {
  calculateFCFMargin,
  calculateFCFYield,
  calculateSimpleFCF,
  getCashAndEquivalents,
  getCashFlowFinancing,
  getCashFlowInvesting,
  getCashFlowOperating,
  getSharesOutstanding,
  getTreasuryShares,
} from './fcf.js';
export {
  calculateBPS,
  calculateDilutedEPS,
  calculateEPS,
  calculateNetMargin,
  calculateOperatingMargin,
  calculatePBR,
  calculatePER,
  calculateROA,
  getAccountingStandard,
  getEquity,
  getNetProfit,
  getNetSales,
  getOperatingProfit,
  getTotalAssets,
  hasConsolidatedData,
  isConsolidatedStatement,
} from './metrics.js';
export {
  calculateROE,
  calculateROEBatch,
  canCalculateROE,
  formatROEResult,
} from './roe.js';
export type {
  FinancialRatios,
  FundamentalAnalysisResult,
  ROECalculationOptions,
  ROEMetadata,
  ROEResult,
} from './types.js';
export { ROECalculationError } from './types.js';
export type { FinancialDataInput, FYDataPoint } from './utils.js';
export {
  calculateDailyValuation,
  filterValidFYData,
  findApplicableFY,
  hasActualFinancialData,
  hasValidValuationMetrics,
  isFiscalYear,
  isQuarterlyPeriod,
  isValidBps,
  isValidEps,
  normalizePeriodType,
  toNumberOrNull,
} from './utils.js';
