import type {
  BollingerBandsData,
  IndicatorValue,
  MACDIndicatorData,
  PPOIndicatorData,
  StockDataPoint,
  TradingValueMAData,
  VolumeComparisonData,
} from '@/types/chart';

export function isStockDataPoint(item: unknown): item is StockDataPoint {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as StockDataPoint).time === 'string' &&
    typeof (item as StockDataPoint).open === 'number' &&
    typeof (item as StockDataPoint).high === 'number' &&
    typeof (item as StockDataPoint).low === 'number' &&
    typeof (item as StockDataPoint).close === 'number'
  );
}

export function hasVolumeData(item: StockDataPoint): item is StockDataPoint & { volume: number } {
  return typeof item.volume === 'number' && item.volume > 0;
}

export function isValidIndicatorValue(value: unknown): value is number {
  return typeof value === 'number' && !Number.isNaN(value) && Number.isFinite(value);
}

export function isIndicatorValue(item: unknown): item is IndicatorValue {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as IndicatorValue).time === 'string' &&
    typeof (item as IndicatorValue).value === 'number'
  );
}

export function isMACDIndicatorData(item: unknown): item is MACDIndicatorData {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as MACDIndicatorData).time === 'string' &&
    typeof (item as MACDIndicatorData).macd === 'number' &&
    typeof (item as MACDIndicatorData).signal === 'number' &&
    typeof (item as MACDIndicatorData).histogram === 'number'
  );
}

export function isPPOIndicatorData(item: unknown): item is PPOIndicatorData {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as PPOIndicatorData).time === 'string' &&
    typeof (item as PPOIndicatorData).ppo === 'number' &&
    typeof (item as PPOIndicatorData).signal === 'number' &&
    typeof (item as PPOIndicatorData).histogram === 'number'
  );
}

export function isBollingerBandsData(item: unknown): item is BollingerBandsData {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as BollingerBandsData).time === 'string' &&
    typeof (item as BollingerBandsData).upper === 'number' &&
    typeof (item as BollingerBandsData).middle === 'number' &&
    typeof (item as BollingerBandsData).lower === 'number'
  );
}

export function isVolumeComparisonData(item: unknown): item is VolumeComparisonData {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as VolumeComparisonData).time === 'string' &&
    typeof (item as VolumeComparisonData).shortMA === 'number' &&
    typeof (item as VolumeComparisonData).longThresholdLower === 'number' &&
    typeof (item as VolumeComparisonData).longThresholdHigher === 'number'
  );
}

export function isTradingValueMAData(item: unknown): item is TradingValueMAData {
  return (
    typeof item === 'object' &&
    item !== null &&
    typeof (item as TradingValueMAData).time === 'string' &&
    typeof (item as TradingValueMAData).value === 'number'
  );
}

/**
 * Type guard for checking if an array contains elements of a specific type
 */
export function isArrayOf<T>(array: unknown, guard: (item: unknown) => item is T): array is T[] {
  return Array.isArray(array) && array.every(guard);
}

export function isIndicatorValueArray(data: unknown): data is IndicatorValue[] {
  return isArrayOf(data, isIndicatorValue);
}

export function isPPOIndicatorDataArray(data: unknown): data is PPOIndicatorData[] {
  return isArrayOf(data, isPPOIndicatorData);
}

export function isBollingerBandsDataArray(data: unknown): data is BollingerBandsData[] {
  return isArrayOf(data, isBollingerBandsData);
}

export function isVolumeComparisonDataArray(data: unknown): data is VolumeComparisonData[] {
  return isArrayOf(data, isVolumeComparisonData);
}

export function isTradingValueMADataArray(data: unknown): data is TradingValueMAData[] {
  return isArrayOf(data, isTradingValueMAData);
}
