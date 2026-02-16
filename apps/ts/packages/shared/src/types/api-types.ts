export interface ApiStockDataPoint {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface ApiStockDataResponse {
  data: ApiStockDataPoint[];
  symbol: string;
  companyName?: string;
  timeframe: string;
  lastUpdated: string;
}

export interface IndicatorValue {
  time: string;
  value: number;
}

export interface MACDIndicatorData {
  time: string;
  macd: number;
  signal: number;
  histogram: number;
}

export interface PPOIndicatorData {
  time: string;
  ppo: number;
  signal: number;
  histogram: number;
}

export type IndicatorData = IndicatorValue | MACDIndicatorData | PPOIndicatorData;

export interface ApiMarginVolumeRatioData {
  date: string;
  ratio: number;
  weeklyAvgVolume: number;
  marginVolume: number;
}

export interface ApiMarginVolumeRatioResponse {
  symbol: string;
  longRatio: ApiMarginVolumeRatioData[];
  shortRatio: ApiMarginVolumeRatioData[];
  lastUpdated: string;
}

// ===== MARGIN PRESSURE INDICATORS =====

/**
 * Margin long pressure data point
 * Measures (LongVol - ShortVol) / N-day avg volume
 */
export interface ApiMarginLongPressureData {
  /** Date (YYYY-MM-DD) */
  date: string;
  /** (LongVol - ShrtVol) / N-day avg volume */
  pressure: number;
  /** Long margin volume (株) */
  longVol: number;
  /** Short margin volume (株) */
  shortVol: number;
  /** N-day average trading volume */
  avgVolume: number;
}

/**
 * Margin flow pressure data point
 * Measures Delta(LongVol - ShortVol) / N-day avg volume
 */
export interface ApiMarginFlowPressureData {
  /** Date (YYYY-MM-DD) */
  date: string;
  /** Delta(LongVol - ShrtVol) / N-day avg volume */
  flowPressure: number;
  /** Current net margin (LongVol - ShrtVol) */
  currentNetMargin: number;
  /** Previous week's net margin (null for first data point) */
  previousNetMargin: number | null;
  /** N-day average trading volume */
  avgVolume: number;
}

/**
 * Margin turnover days data point
 * Measures LongVol / N-day avg volume
 */
export interface ApiMarginTurnoverDaysData {
  /** Date (YYYY-MM-DD) */
  date: string;
  /** LongVol / N-day avg volume (days to absorb) */
  turnoverDays: number;
  /** Long margin volume (株) */
  longVol: number;
  /** N-day average trading volume */
  avgVolume: number;
}

/**
 * Response for margin pressure indicators API endpoint
 */
export interface ApiMarginPressureIndicatorsResponse {
  /** Stock symbol (4-digit code) */
  symbol: string;
  /** Rolling average period used for calculation (days) */
  averagePeriod: number;
  /** Margin long pressure time series */
  longPressure: ApiMarginLongPressureData[];
  /** Margin flow pressure time series */
  flowPressure: ApiMarginFlowPressureData[];
  /** Margin turnover days time series */
  turnoverDays: ApiMarginTurnoverDaysData[];
  /** Last updated timestamp */
  lastUpdated: string;
}

export interface ApiTopixDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ApiTopixDataResponse {
  topix: ApiTopixDataPoint[];
  lastUpdated: string;
}

/**
 * Daily valuation data point (PER/PBR calculated with daily close prices)
 */
export interface ApiDailyValuationDataPoint {
  /** Trading date (YYYY-MM-DD) */
  date: string;
  /** Stock close price on this date (円) */
  close: number;
  /** Price to Earnings Ratio using FY EPS (倍) */
  per: number | null;
  /** Price to Book Ratio using FY BPS (倍) */
  pbr: number | null;
  /** Market capitalization (円) */
  marketCap: number | null;
}

/**
 * Single data point for fundamental metrics time series
 */
export interface ApiFundamentalDataPoint {
  /** Period end date (YYYY-MM-DD) */
  date: string;
  /** Date when the data was disclosed (YYYY-MM-DD) */
  disclosedDate: string;
  /** Period type (FY, 1Q, 2Q, 3Q) */
  periodType: string;
  /** Whether this is consolidated financial data */
  isConsolidated: boolean;
  /** Accounting standard (JGAAP, IFRS, US GAAP) */
  accountingStandard: string | null;
  // Core metrics
  /** Return on Equity (%) */
  roe: number | null;
  /** Earnings Per Share (円) */
  eps: number | null;
  /** Diluted Earnings Per Share (円) */
  dilutedEps: number | null;
  /** Book Value Per Share (円) */
  bps: number | null;
  /** Adjusted EPS using share count (円) */
  adjustedEps?: number | null;
  /** Adjusted forecast EPS using share count (円) */
  adjustedForecastEps?: number | null;
  /** Adjusted BPS using share count (円) */
  adjustedBps?: number | null;
  /** FY dividend per share (円) */
  dividendFy?: number | null;
  /** Adjusted FY dividend per share using share count (円) */
  adjustedDividendFy?: number | null;
  /** Price to Earnings Ratio (倍) - calculated with disclosure date price */
  per: number | null;
  /** Price to Book Ratio (倍) - calculated with disclosure date price */
  pbr: number | null;
  // Profitability metrics
  /** Return on Assets (%) */
  roa: number | null;
  /** Operating Profit Margin (%) */
  operatingMargin: number | null;
  /** Net Profit Margin (%) */
  netMargin: number | null;
  // Context data
  /** Stock price at disclosure date (円) */
  stockPrice: number | null;
  /** Net profit (百万円) */
  netProfit: number | null;
  /** Shareholders' equity (百万円) */
  equity: number | null;
  /** Total assets (百万円) */
  totalAssets: number | null;
  /** Net sales / Revenue (百万円) */
  netSales: number | null;
  /** Operating profit (百万円) */
  operatingProfit: number | null;
  // Cash flow data
  /** Cash flows from operating activities (百万円) */
  cashFlowOperating: number | null;
  /** Cash flows from investing activities (百万円) */
  cashFlowInvesting: number | null;
  /** Cash flows from financing activities (百万円) */
  cashFlowFinancing: number | null;
  /** Cash and cash equivalents (百万円) */
  cashAndEquivalents: number | null;
  // FCF (Free Cash Flow) metrics
  /** Free Cash Flow = CFO + CFI (百万円) */
  fcf: number | null;
  /** FCF Yield = FCF / Market Cap × 100 (%) */
  fcfYield: number | null;
  /** FCF Margin = FCF / Net Sales × 100 (%) */
  fcfMargin: number | null;
  /** Operating Cash Flow / Net Profit (x) */
  cfoToNetProfitRatio: number | null;
  /** Market cap / N-day average trading value (x) */
  tradingValueToMarketCapRatio: number | null;
  // Forecast EPS
  /** Forecast EPS for current/next fiscal year (円) */
  forecastEps?: number | null;
  /** Change rate from actual EPS to forecast EPS (%) */
  forecastEpsChangeRate?: number | null;
  /** Revised forecast EPS from latest quarterly statement (円) */
  revisedForecastEps?: number | null;
  /** Source period type of revised forecast (e.g. 1Q, 2Q, 3Q) */
  revisedForecastSource?: string | null;
  // Previous period cash flow data
  /** Previous period cash flows from operating activities (百万円) */
  prevCashFlowOperating?: number | null;
  /** Previous period cash flows from investing activities (百万円) */
  prevCashFlowInvesting?: number | null;
  /** Previous period cash flows from financing activities (百万円) */
  prevCashFlowFinancing?: number | null;
  /** Previous period cash and cash equivalents (百万円) */
  prevCashAndEquivalents?: number | null;
}

/**
 * Response for fundamental metrics API endpoint
 */
export interface ApiFundamentalsResponse {
  /** Stock symbol (4-digit code) */
  symbol: string;
  /** Company name */
  companyName?: string;
  /** Time series data sorted by date descending */
  data: ApiFundamentalDataPoint[];
  /** Most recent metrics (convenience field) */
  latestMetrics?: ApiFundamentalDataPoint;
  /** Daily PER/PBR time series (calculated with daily close prices and FY EPS/BPS) */
  dailyValuation?: ApiDailyValuationDataPoint[];
  /** Rolling average period used for trading value to market cap ratio (days) */
  tradingValuePeriod: number;
  /** Last updated timestamp */
  lastUpdated: string;
}

// ===== FACTOR REGRESSION ANALYSIS =====

/**
 * Index match result from factor regression
 */
export interface ApiIndexMatch {
  /** Index code (e.g., "0085", "8100") */
  indexCode: string;
  /** Index name (e.g., "TOPIX-17 自動車・輸送機") */
  indexName: string;
  /** Index category (e.g., "sector17", "style") */
  category: string;
  /** R-squared value (0-1) */
  rSquared: number;
  /** Beta coefficient */
  beta: number;
}

/**
 * Factor regression analysis response
 */
export interface ApiFactorRegressionResponse {
  /** Stock code (4-digit) */
  stockCode: string;
  /** Company name */
  companyName?: string;
  /** Market beta from Stage 1 regression */
  marketBeta: number;
  /** Market R-squared from Stage 1 regression */
  marketRSquared: number;
  /** Top 3 matches from TOPIX-17 sectors */
  sector17Matches: ApiIndexMatch[];
  /** Top 3 matches from 33 sectors */
  sector33Matches: ApiIndexMatch[];
  /** Top 3 matches from TOPIX size + market + style indices */
  topixStyleMatches: ApiIndexMatch[];
  /** Analysis date (YYYY-MM-DD) */
  analysisDate: string;
  /** Number of data points used */
  dataPoints: number;
  /** Date range of analysis */
  dateRange: {
    from: string;
    to: string;
  };
}

// ===== PORTFOLIO FACTOR REGRESSION =====

/**
 * Portfolio weight information in API response
 */
export interface ApiPortfolioWeight {
  /** Stock code (4-digit) */
  code: string;
  /** Company name */
  companyName: string;
  /** Weight (0-1) */
  weight: number;
  /** Latest stock price used for weight calculation */
  latestPrice: number;
  /** Current market value */
  marketValue: number;
  /** Holding quantity */
  quantity: number;
}

/**
 * Stock excluded from portfolio analysis
 */
export interface ApiExcludedStock {
  /** Stock code (4-digit) */
  code: string;
  /** Company name */
  companyName: string;
  /** Reason for exclusion */
  reason: string;
}

/**
 * Portfolio factor regression analysis response
 */
export interface ApiPortfolioFactorRegressionResponse {
  /** Portfolio ID */
  portfolioId: number;
  /** Portfolio name */
  portfolioName: string;
  /** Weight breakdown for each stock */
  weights: ApiPortfolioWeight[];
  /** Total portfolio market value */
  totalValue: number;
  /** Number of stocks in portfolio */
  stockCount: number;
  /** Number of stocks included in analysis */
  includedStockCount: number;
  /** Market beta from Stage 1 regression */
  marketBeta: number;
  /** Market R-squared from Stage 1 regression */
  marketRSquared: number;
  /** Top 3 matches from TOPIX-17 sectors */
  sector17Matches: ApiIndexMatch[];
  /** Top 3 matches from 33 sectors */
  sector33Matches: ApiIndexMatch[];
  /** Top 3 matches from TOPIX size + market + style indices */
  topixStyleMatches: ApiIndexMatch[];
  /** Analysis date (YYYY-MM-DD) */
  analysisDate: string;
  /** Number of data points used */
  dataPoints: number;
  /** Date range of analysis */
  dateRange: {
    from: string;
    to: string;
  };
  /** Stocks excluded from analysis */
  excludedStocks: ApiExcludedStock[];
}
