import type {
  IndicatorData as ApiIndicatorData,
  IndicatorValue as ApiIndicatorValue,
  MACDIndicatorData as ApiMACDIndicatorData,
  ApiMarginFlowPressureData,
  ApiMarginLongPressureData,
  ApiMarginPressureIndicatorsResponse,
  ApiMarginTurnoverDaysData,
  ApiMarginVolumeRatioData,
  ApiMarginVolumeRatioResponse,
  PPOIndicatorData as ApiPPOIndicatorData,
  ApiStockDataPoint,
  ApiStockDataResponse,
} from '@trading25/shared/types/api-types';

// Type aliases for compatibility
export type StockDataPoint = ApiStockDataPoint;
export type StockDataResponse = ApiStockDataResponse;
export type IndicatorData = ApiIndicatorData;
export type MACDIndicatorData = ApiMACDIndicatorData;
export type PPOIndicatorData = ApiPPOIndicatorData;
export type IndicatorValue = ApiIndicatorValue;
export type MarginVolumeRatioData = ApiMarginVolumeRatioData;
export type MarginVolumeRatioResponse = ApiMarginVolumeRatioResponse;
export type MarginLongPressureData = ApiMarginLongPressureData;
export type MarginFlowPressureData = ApiMarginFlowPressureData;
export type MarginTurnoverDaysData = ApiMarginTurnoverDaysData;
export type MarginPressureIndicatorsResponse = ApiMarginPressureIndicatorsResponse;

// Chart data structures (keeping only what's needed)

export interface CandlestickSeriesOptions {
  upColor?: string;
  downColor?: string;
  borderVisible?: boolean;
  wickUpColor?: string;
  wickDownColor?: string;
}

export interface HistogramSeriesOptions {
  color?: string;
  priceFormat?: {
    type: string;
  };
  priceScaleId?: string;
}

// Bollinger Bands data
export interface BollingerBandsData {
  time: string;
  upper: number;
  middle: number;
  lower: number;
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
  volumeComparison?: VolumeComparisonData[];
  tradingValueMA?: TradingValueMAData[];
}

export interface VolumeData {
  time: string;
  value: number;
  color: string;
}
