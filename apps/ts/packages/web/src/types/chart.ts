import type {
  IndicatorData as ApiIndicatorData,
  IndicatorValue as ApiIndicatorValue,
  MACDIndicatorData as ApiMACDIndicatorData,
  ApiMarginFlowPressureData,
  ApiMarginLongPressureData,
  ApiMarginPressureIndicatorsResponse,
  ApiMarginTurnoverDaysData,
  ApiMarginVolumeRatioData,
  PPOIndicatorData as ApiPPOIndicatorData,
  ApiStockDataPoint,
} from '@trading25/contracts/types/api-types';

// Type aliases for compatibility
export type StockDataPoint = ApiStockDataPoint;
export type IndicatorData = ApiIndicatorData;
export type MACDIndicatorData = ApiMACDIndicatorData;
export type PPOIndicatorData = ApiPPOIndicatorData;
export type IndicatorValue = ApiIndicatorValue;
export type RecentReturnData = ApiIndicatorValue;
export type RiskAdjustedReturnData = ApiIndicatorValue;
export type MarginVolumeRatioData = ApiMarginVolumeRatioData;
export type MarginLongPressureData = ApiMarginLongPressureData;
export type MarginFlowPressureData = ApiMarginFlowPressureData;
export type MarginTurnoverDaysData = ApiMarginTurnoverDaysData;
export type MarginPressureIndicatorsResponse = ApiMarginPressureIndicatorsResponse;

// Bollinger Bands data
export interface BollingerBandsData {
  time: string;
  upper: number;
  middle: number;
  lower: number;
}

export interface SMAATRBandsData {
  time: string;
  upper: number;
  middle: number;
  lower: number;
  deviation: number;
}

// Volume Comparison data
export interface VolumeComparisonData {
  time: string;
  shortMA: number;
  longThresholdLower: number;
  longThresholdHigher: number;
}

// Trading Value MA data
export interface TradingValueMAData {
  time: string;
  value: number;
}

export interface ChartData {
  candlestickData: StockDataPoint[];
  indicators: Record<string, IndicatorData[]>;
  bollingerBands?: BollingerBandsData[];
  smaAtrBands?: SMAATRBandsData[];
  volumeComparison?: VolumeComparisonData[];
  tradingValueMA?: TradingValueMAData[];
}

export interface VolumeData {
  time: string;
  value: number;
  color: string;
}
