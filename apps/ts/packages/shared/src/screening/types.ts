/**
 * Screening Types
 * Type definitions for stock screening functionality
 */

export type ScreeningType = 'rangeBreakFast' | 'rangeBreakSlow';

export interface FuturePricePoint {
  date: string;
  price: number;
  changePercent: number;
}

export interface FutureReturns {
  day5: FuturePricePoint | null;
  day20: FuturePricePoint | null;
  day60: FuturePricePoint | null;
}

export interface ScreeningResult {
  stockCode: string;
  companyName: string;
  scaleCategory?: string;
  sector33Name?: string;
  screeningType: ScreeningType;
  matchedDate: Date;
  details: ScreeningDetails;
  futureReturns?: FutureReturns;
}

export interface ScreeningDetails {
  rangeBreak?: RangeBreakDetails;
}

export interface RangeBreakDetails {
  breakDate: Date;
  currentHigh: number;
  maxHighInLookback: number;
  breakPercentage: number;
  volumeRatio: number;
  avgVolume20Days: number;
  avgVolume100Days: number;
}

export interface ScreeningConfig {
  rangeBreakFastEnabled: boolean;
  rangeBreakSlowEnabled: boolean;
  rangeBreakFastParams: RangeBreakParams;
  rangeBreakSlowParams: RangeBreakParams;
  recentDays: number;
}

export interface RangeBreakParams {
  period: number; // Long-term period for max high (e.g., 100 days)
  lookbackDays: number; // Short-term period for recent max high (e.g., 10 days)
  volumeRatioThreshold: number;
  volumeShortPeriod: number;
  volumeLongPeriod: number;
  volumeType: 'sma' | 'ema'; // Moving average type for volume calculation
}

export interface VolumeAnalysis {
  period: number;
  average: number;
  current: number;
  ratio: number;
}

export interface ScreeningDateRange {
  from: Date;
  to: Date;
}

export interface StockDataPoint {
  date: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ScreeningInput {
  stockCode: string;
  companyName: string;
  scaleCategory?: string;
  sector33Name?: string;
  data: StockDataPoint[];
}

/**
 * Database row interface for stock data from market.db
 */
export interface DatabaseStockRow {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

/**
 * Filter criteria for screening results
 */
export interface FilterCriteria {
  minBreakPercentage?: number;
  minVolumeRatio?: number;
}

export const DEFAULT_SCREENING_CONFIG: ScreeningConfig = {
  rangeBreakFastEnabled: true,
  rangeBreakSlowEnabled: true,
  rangeBreakFastParams: {
    period: 200, // Long-term period for max high comparison
    lookbackDays: 10, // Short-term period for recent max high
    volumeRatioThreshold: 1.7,
    volumeShortPeriod: 30,
    volumeLongPeriod: 120,
    volumeType: 'ema', // Use EMA for volume calculation
  },
  rangeBreakSlowParams: {
    period: 200, // Long-term period for max high comparison
    lookbackDays: 10, // Short-term period for recent max high
    volumeRatioThreshold: 1.7,
    volumeShortPeriod: 50,
    volumeLongPeriod: 150,
    volumeType: 'sma', // Use SMA for volume calculation
  },
  recentDays: 10, // Screening target period (days to check from latest)
};
