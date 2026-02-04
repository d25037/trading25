import type { DisplayTimeframe } from '@/stores/chartStore';
import type { ChartData } from '@/types/chart';
import {
  isBollingerBandsDataArray,
  isIndicatorValueArray,
  isPPOIndicatorDataArray,
  isTradingValueMADataArray,
  isVolumeComparisonDataArray,
} from './typeGuards';

/**
 * Safely extract indicator data from chart data with type validation
 */
export function getIndicatorData<T>(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe,
  indicatorName: string,
  validator: (data: unknown) => data is T[]
): T[] | undefined {
  const data = chartData?.[timeframe]?.indicators[indicatorName];
  if (validator(data)) {
    return data;
  }
  return undefined;
}

/**
 * Get ATR Support indicator data
 */
export function getAtrSupportData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  return getIndicatorData(chartData, timeframe, 'atrSupport', isIndicatorValueArray);
}

/**
 * Get N-Bar Support indicator data
 */
export function getNBarSupportData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  return getIndicatorData(chartData, timeframe, 'nBarSupport', isIndicatorValueArray);
}

/**
 * Get PPO indicator data
 */
export function getPpoData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  return getIndicatorData(chartData, timeframe, 'ppo', isPPOIndicatorDataArray);
}

/**
 * Safely get Bollinger Bands data from chart data
 */
export function getBollingerBandsData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  const data = chartData?.[timeframe]?.bollingerBands;
  if (isBollingerBandsDataArray(data)) {
    return data;
  }
  return undefined;
}

/**
 * Safely get Volume Comparison data from chart data
 */
export function getVolumeComparisonData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  const data = chartData?.[timeframe]?.volumeComparison;
  if (isVolumeComparisonDataArray(data)) {
    return data;
  }
  return undefined;
}

/**
 * Safely get Trading Value MA data from chart data
 */
export function getTradingValueMAData(
  chartData: Record<DisplayTimeframe, ChartData | undefined> | undefined,
  timeframe: DisplayTimeframe
) {
  const data = chartData?.[timeframe]?.tradingValueMA;
  if (isTradingValueMADataArray(data)) {
    return data;
  }
  return undefined;
}
