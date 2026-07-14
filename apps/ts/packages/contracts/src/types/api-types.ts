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

export interface ResponseDiagnostics {
  missing_required_data?: string[];
  used_fields?: string[];
  effective_period_type?: string | null;
  warnings?: string[];
}

export interface DataProvenance {
  source_kind: 'market' | 'dataset';
  market_snapshot_id?: string | null;
  dataset_snapshot_id?: string | null;
  reference_date?: string | null;
  loaded_domains?: string[];
  strategy_name?: string | null;
  strategy_fingerprint?: string | null;
  warnings?: string[];
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
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
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
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
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
  /** Adjusted actual FY EPS used for valuation (円) */
  eps?: number | null;
  /** Adjusted actual FY BPS used for valuation (円) */
  bps?: number | null;
  /** Price to Earnings Ratio using FY EPS (倍) */
  per: number | null;
  /** Forward EPS used for valuation (円) */
  forwardEps?: number | null;
  /** Forward Price to Earnings Ratio (倍) */
  forwardPer?: number | null;
  /** Actual FY sales used for PSR (円) */
  sales?: number | null;
  /** Forward sales used for forward PSR (円) */
  forwardSales?: number | null;
  /** Price to Sales Ratio (倍) */
  psr?: number | null;
  /** Forward Price to Sales Ratio (倍) */
  forwardPsr?: number | null;
  /** Price to Operating Profit Ratio (倍) */
  pOp?: number | null;
  /** Forward Price to Operating Profit Ratio (倍) */
  forwardPOp?: number | null;
  /** Price to Book Ratio using FY BPS (倍) */
  pbr: number | null;
  /** Market capitalization using shares outstanding (円) */
  marketCap: number | null;
  /** Market capitalization using free-float shares (円) */
  freeFloatMarketCap?: number | null;
  /** Disclosure date of the FY actual EPS/BPS source */
  statementDisclosedDate?: string | null;
  /** Disclosure date of the forward EPS source */
  forwardEpsDisclosedDate?: string | null;
  /** Forward EPS source classifier */
  forwardEpsSource?: 'revised' | 'fy' | null;
  /** Disclosure date of the forward sales source */
  forwardSalesDisclosedDate?: string | null;
  /** Forward sales source classifier */
  forwardSalesSource?: 'revised' | 'fy' | null;
}

export interface ApiLatestMetricsSourceItem {
  table: 'daily_valuation' | 'statements';
  date?: string | null;
  periodType?: string | null;
  disclosedDate?: string | null;
  source?: string | null;
}

export interface ApiLatestMetricsSource {
  actualPerShare: ApiLatestMetricsSourceItem;
  valuation: ApiLatestMetricsSourceItem;
  forecast?: ApiLatestMetricsSourceItem | null;
  latestDisclosure?: ApiLatestMetricsSourceItem | null;
}

export interface ApiLiquidityProfileWindow {
  advWindow: number;
  averageTradingValue?: number | null;
  freeFloatTradingValueRatioPct?: number | null;
  liquidityResidualZ?: number | null;
  liquidityImpliedFreeFloatMarketCap?: number | null;
  liquidityImpliedPrice?: number | null;
  liquidityImpliedPriceGapPct?: number | null;
  liquidityRegime?: string | null;
  regressionAlpha?: number | null;
  regressionBeta?: number | null;
  regressionRSquared?: number | null;
  regressionObservationCount?: number | null;
}

export interface ApiLiquidityProfile {
  supported: boolean;
  unsupportedReason?: string | null;
  modelScope: string;
  date?: string | null;
  currentPrice?: number | null;
  freeFloatMarketCap?: number | null;
  recentReturn20dPct?: number | null;
  recentReturn60dPct?: number | null;
  windows: ApiLiquidityProfileWindow[];
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
  /** Forecast FY dividend per share (円) */
  forecastDividendFy?: number | null;
  /** Adjusted forecast FY dividend per share using share count (円) */
  adjustedForecastDividendFy?: number | null;
  /** Change rate from actual dividend to forecast dividend (%) */
  forecastDividendFyChangeRate?: number | null;
  /** Payout ratio (%) */
  payoutRatio?: number | null;
  /** Forecast payout ratio (%) */
  forecastPayoutRatio?: number | null;
  /** Change rate from actual payout ratio to forecast payout ratio (%) */
  forecastPayoutRatioChangeRate?: number | null;
  /** Price to Earnings Ratio (倍) - calculated with disclosure date price */
  per: number | null;
  /** Forward Price to Earnings Ratio (倍) */
  forwardPer?: number | null;
  /** Price to Sales Ratio (倍) */
  psr?: number | null;
  /** Forward Price to Sales Ratio (倍) */
  forwardPsr?: number | null;
  /** Price to Operating Profit Ratio (倍) */
  pOp?: number | null;
  /** Forward Price to Operating Profit Ratio (倍) */
  forwardPOp?: number | null;
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
  /** Forecast sales / Revenue (百万円) */
  forecastSales?: number | null;
  /** Operating profit (百万円) */
  operatingProfit: number | null;
  /** Forecast operating profit (百万円) */
  forecastOperatingProfit?: number | null;
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
  /** CFO Yield = CFO / Market Cap × 100 (%) */
  cfoYield: number | null;
  /** CFO Margin = CFO / Net Sales × 100 (%) */
  cfoMargin: number | null;
  /** Operating Cash Flow / Net Profit (x) */
  cfoToNetProfitRatio: number | null;
  /** Market cap / N-day average trading value (x) */
  tradingValueToMarketCapRatio: number | null;
  /** Market capitalization using shares outstanding (円) */
  marketCap?: number | null;
  /** Market capitalization using free-float shares (円) */
  freeFloatMarketCap?: number | null;
  // Forecast EPS
  /** Forecast EPS for current/next fiscal year (円) */
  forecastEps?: number | null;
  /** Change rate from actual EPS to forecast EPS (%) */
  forecastEpsChangeRate?: number | null;
  /** Change rate from actual sales to forecast sales (%) */
  forecastSalesChangeRate?: number | null;
  /** Change rate from actual operating profit to forecast operating profit (%) */
  forecastOperatingProfitChangeRate?: number | null;
  /** Whether latest forecast EPS exceeds recent FY actual EPS values (lookback window) */
  forecastEpsAboveRecentFyActuals?: boolean | null;
  /** Lookback FY count used for forecast-vs-actual EPS comparison */
  forecastEpsLookbackFyCount?: number;
  /** Revised forecast EPS from latest quarterly statement (円) */
  revisedForecastEps?: number | null;
  /** Revised forecast sales from latest quarterly statement (百万円) */
  revisedForecastSales?: number | null;
  /** Revised forecast operating profit from latest quarterly statement (百万円) */
  revisedForecastOperatingProfit?: number | null;
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
  /** Source tables and dates used to compose latestMetrics */
  latestMetricsSource?: ApiLatestMetricsSource | null;
  /** Daily PER/PBR time series (calculated with daily close prices and FY EPS/BPS) */
  dailyValuation?: ApiDailyValuationDataPoint[];
  /** Adjusted price basis date used by daily valuation */
  priceBasisDate?: string | null;
  /** Adjusted valuation materialization basis version */
  valuationBasisVersion?: string | null;
  /** Prime-only free-float liquidity diagnostic for Symbol Workbench */
  liquidityProfile?: ApiLiquidityProfile | null;
  /** Rolling average period used for trading value to market cap ratio (days) */
  tradingValuePeriod: number;
  /** Lookback FY count used for forecast-vs-actual EPS comparison */
  forecastEpsLookbackFyCount: number;
  /** Last updated timestamp */
  lastUpdated: string;
  provenance: DataProvenance;
  diagnostics: ResponseDiagnostics;
}
