/**
 * Backtest fundamentals and OHLCV API Types
 */

// ============================================
// Fundamentals Types
// ============================================

export type FundamentalsPeriodType = 'all' | 'FY' | '1Q' | '2Q' | '3Q';

export interface FundamentalsComputeRequest {
  symbol: string;
  from_date?: string;
  to_date?: string;
  period_type?: FundamentalsPeriodType;
  prefer_consolidated?: boolean;
}

export interface FundamentalDataPoint {
  date: string;
  disclosedDate: string;
  periodType: string;
  isConsolidated: boolean;
  accountingStandard: string | null;
  // Core metrics
  roe: number | null;
  eps: number | null;
  dilutedEps: number | null;
  bps: number | null;
  adjustedEps: number | null;
  adjustedForecastEps: number | null;
  adjustedBps: number | null;
  per: number | null;
  forwardPer?: number | null;
  psr?: number | null;
  forwardPsr?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  pbr: number | null;
  // Profitability metrics
  roa: number | null;
  operatingMargin: number | null;
  netMargin: number | null;
  // Financial data (millions of JPY)
  stockPrice: number | null;
  netProfit: number | null;
  equity: number | null;
  totalAssets: number | null;
  netSales: number | null;
  operatingProfit: number | null;
  forecastOperatingProfit?: number | null;
  // Cash flow data (millions of JPY)
  cashFlowOperating: number | null;
  cashFlowInvesting: number | null;
  cashFlowFinancing: number | null;
  cashAndEquivalents: number | null;
  // FCF metrics
  fcf: number | null;
  fcfYield: number | null;
  fcfMargin: number | null;
  // Forecast EPS
  forecastEps: number | null;
  forecastEpsChangeRate: number | null;
  forecastOperatingProfitChangeRate?: number | null;
  // Revised forecast (from latest Q)
  revisedForecastEps?: number | null;
  revisedForecastSource?: string | null;
  // Previous period CF data
  prevCashFlowOperating: number | null;
  prevCashFlowInvesting: number | null;
  prevCashFlowFinancing: number | null;
  prevCashAndEquivalents: number | null;
}

export interface DailyValuationDataPoint {
  date: string;
  close: number;
  eps?: number | null;
  bps?: number | null;
  per: number | null;
  forwardPer?: number | null;
  sales?: number | null;
  forwardSales?: number | null;
  psr?: number | null;
  forwardPsr?: number | null;
  pOp?: number | null;
  forwardPOp?: number | null;
  pbr: number | null;
  marketCap: number | null;
  freeFloatMarketCap?: number | null;
  statementDisclosedDate?: string | null;
  forwardEps?: number | null;
  forwardEpsDisclosedDate?: string | null;
  forwardEpsSource?: 'revised' | 'fy' | null;
  forwardSalesDisclosedDate?: string | null;
  forwardSalesSource?: 'revised' | 'fy' | null;
  priceBasisDate?: string | null;
  basisVersion?: string | null;
}

export interface FundamentalsComputeResponse {
  symbol: string;
  asOfDate: string;
  companyName?: string;
  data: FundamentalDataPoint[];
  latestMetrics?: FundamentalDataPoint;
  dailyValuation?: DailyValuationDataPoint[];
  lastUpdated: string;
}

// ============================================
// OHLCV Resample Types
// ============================================

export type Timeframe = 'daily' | 'weekly' | 'monthly';
export type HandleZeroDivision = 'skip' | 'zero' | 'null';

export interface RelativeOHLCOptions {
  handle_zero_division?: HandleZeroDivision;
}

export interface OHLCVResampleRequest {
  stock_code: string;
  source?: string;
  timeframe?: Timeframe;
  start_date?: string;
  end_date?: string;
  benchmark_code?: string;
  relative_options?: RelativeOHLCOptions;
}

export interface OHLCVRecord {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface OHLCVResampleResponse {
  stock_code: string;
  timeframe: string;
  benchmark_code?: string;
  meta: {
    source_bars: number;
    resampled_bars: number;
  };
  data: OHLCVRecord[];
}
