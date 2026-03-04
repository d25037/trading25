/**
 * Portfolio Performance Types
 *
 * Type definitions for portfolio P&L and benchmark comparison
 */

/**
 * Individual holding performance metrics
 */
export interface HoldingPerformance {
  /** Stock code (4-digit) */
  code: string;
  /** Company name */
  companyName: string;
  /** Holding quantity */
  quantity: number;
  /** Purchase price per share */
  purchasePrice: number;
  /** Current price per share (latest close from market DB) */
  currentPrice: number;
  /** Total cost (purchasePrice × quantity) */
  cost: number;
  /** Current market value (currentPrice × quantity) */
  marketValue: number;
  /** Unrealized P&L (marketValue - cost) */
  pnl: number;
  /** Return rate as decimal (pnl / cost) */
  returnRate: number;
  /** Weight in portfolio (marketValue / totalValue) */
  weight: number;
  /** Purchase date (YYYY-MM-DD) */
  purchaseDate: string;
  /** Account name (optional) */
  account?: string;
}

/**
 * Portfolio summary metrics
 */
export interface PortfolioSummary {
  /** Total cost (sum of all holdings' cost) */
  totalCost: number;
  /** Current total market value */
  currentValue: number;
  /** Total unrealized P&L */
  totalPnL: number;
  /** Total return rate as decimal */
  returnRate: number;
}

/**
 * Time series data point for portfolio performance
 */
export interface PerformanceDataPoint {
  /** Date (YYYY-MM-DD) */
  date: string;
  /** Daily log return */
  dailyReturn: number;
  /** Cumulative return from start date */
  cumulativeReturn: number;
  /** Portfolio value on this date (optional) */
  portfolioValue?: number;
}

/**
 * Benchmark comparison metrics
 */
export interface BenchmarkMetrics {
  /** Benchmark index code (e.g., "0000" for TOPIX) */
  code: string;
  /** Benchmark name (e.g., "TOPIX") */
  name: string;
  /** Portfolio beta vs benchmark */
  beta: number;
  /** Jensen's alpha (excess return) */
  alpha: number;
  /** Pearson correlation coefficient */
  correlation: number;
  /** R-squared from regression */
  rSquared: number;
  /** Benchmark cumulative return over period */
  benchmarkReturn: number;
  /** Relative return (portfolio - benchmark) */
  relativeReturn: number;
}

/**
 * Benchmark time series data point
 */
export interface BenchmarkDataPoint {
  /** Date (YYYY-MM-DD) */
  date: string;
  /** Portfolio cumulative return */
  portfolioReturn: number;
  /** Benchmark cumulative return */
  benchmarkReturn: number;
}

/**
 * Complete portfolio performance result
 */
export interface PortfolioPerformanceResult {
  /** Portfolio ID */
  portfolioId: number;
  /** Portfolio name */
  portfolioName: string;
  /** Portfolio description */
  portfolioDescription?: string;

  /** Summary metrics */
  summary: PortfolioSummary;

  /** Per-holding performance */
  holdings: HoldingPerformance[];

  /** Time series performance data */
  timeSeries: PerformanceDataPoint[];

  /** Benchmark comparison (null if insufficient data) */
  benchmark: BenchmarkMetrics | null;

  /** Benchmark vs portfolio time series (null if insufficient data) */
  benchmarkTimeSeries: BenchmarkDataPoint[] | null;

  /** Analysis metadata */
  analysisDate: string;
  /** Date range of time series analysis */
  dateRange: {
    from: string;
    to: string;
  } | null;
  /** Number of data points in time series */
  dataPoints: number;

  /** Warnings or notes */
  warnings: string[];
}

/**
 * Options for portfolio performance calculation
 */
export interface PortfolioPerformanceOptions {
  /** Benchmark index code (default: "0000" for TOPIX) */
  benchmarkCode?: string;
  /** Lookback days for time series analysis (default: 252) */
  lookbackDays?: number;
  /** Minimum data points required for benchmark analysis (default: 30) */
  minDataPoints?: number;
}

/**
 * Error class for portfolio performance calculations
 */
export class PortfolioPerformanceError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'PortfolioPerformanceError';
  }
}
