/**
 * Volume Utilities
 * Volume moving average calculations for screening
 */

import type { StockDataPoint, VolumeAnalysis } from './types';

/**
 * Simple Moving Average (inlined from ta/sma.ts to remove ta/ dependency)
 */
function sma(values: number[], period: number): number[] {
  if (period <= 0) {
    throw new Error('Period must be greater than 0');
  }

  if (values.length < period) {
    return [];
  }

  const resultLength = values.length - period + 1;
  const result = new Array(resultLength);

  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += values[i] ?? 0;
  }
  result[0] = sum / period;

  for (let i = 1; i < resultLength; i++) {
    sum = sum - (values[i - 1] ?? 0) + (values[i + period - 1] ?? 0);
    result[i] = sum / period;
  }

  return result;
}

/**
 * Exponential Moving Average (inlined from ta/ema.ts to remove ta/ dependency)
 */
function ema(values: number[], period: number, smoothing = 2): number[] {
  if (period <= 0) {
    throw new Error('Period must be greater than 0');
  }

  if (values.length < period) {
    return [];
  }

  const resultLength = values.length - period + 1;
  const result = new Array(resultLength);
  const multiplier = smoothing / (period + 1);

  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += values[i] ?? 0;
  }
  let previousEMA = sum / period;
  result[0] = previousEMA;

  for (let i = period; i < values.length; i++) {
    const currentValue = values[i] ?? 0;
    const currentEMA = (currentValue - previousEMA) * multiplier + previousEMA;
    result[i - period + 1] = currentEMA;
    previousEMA = currentEMA;
  }

  return result;
}

/**
 * Calculate moving average volume for given period
 */
export function calculateVolumeMA(data: StockDataPoint[], period: number, type: 'sma' | 'ema' = 'sma'): number[] {
  const volumes = data.map((d) => d.volume);
  return type === 'ema' ? ema(volumes, period) : sma(volumes, period);
}

/**
 * Get volume analysis for specific period
 */
export function getVolumeAnalysis(
  data: StockDataPoint[],
  period: number,
  endIndex: number = data.length - 1,
  type: 'sma' | 'ema' = 'sma'
): VolumeAnalysis | null {
  if (data.length === 0 || endIndex < 0 || endIndex >= data.length) {
    return null;
  }

  if (data.length < period) {
    return null;
  }

  const volumeMA = calculateVolumeMA(data, period, type);

  // Calculate which index in the MA array corresponds to our endIndex
  const maIndex = endIndex - period + 1;

  if (maIndex < 0 || maIndex >= volumeMA.length) {
    return null;
  }

  const average = volumeMA[maIndex];
  const current = data[endIndex]?.volume ?? 0;

  if (typeof average !== 'number') {
    return null;
  }

  const ratio = average > 0 ? current / average : 0;

  return {
    period,
    average,
    current,
    ratio,
  };
}

/**
 * Check if volume condition is met
 */
export function checkVolumeCondition(
  data: StockDataPoint[],
  shortPeriod: number,
  longPeriod: number,
  threshold: number,
  endIndex: number = data.length - 1,
  type: 'sma' | 'ema' = 'sma'
): boolean {
  const shortAnalysis = getVolumeAnalysis(data, shortPeriod, endIndex, type);
  const longAnalysis = getVolumeAnalysis(data, longPeriod, endIndex, type);

  if (!shortAnalysis || !longAnalysis) {
    return false;
  }

  return shortAnalysis.average > longAnalysis.average * threshold;
}

/**
 * Check volume condition at specific index with details
 * Returns volume analysis details if condition is matched
 */
export function checkVolumeConditionAtIndex(
  data: StockDataPoint[],
  shortPeriod: number,
  longPeriod: number,
  threshold: number,
  index: number,
  type: 'sma' | 'ema' = 'sma'
): { matched: boolean; details?: { short: VolumeAnalysis; long: VolumeAnalysis } } {
  const shortAnalysis = getVolumeAnalysis(data, shortPeriod, index, type);
  const longAnalysis = getVolumeAnalysis(data, longPeriod, index, type);

  if (!shortAnalysis || !longAnalysis) {
    return { matched: false };
  }

  const matched = shortAnalysis.average > longAnalysis.average * threshold;

  return {
    matched,
    details: matched ? { short: shortAnalysis, long: longAnalysis } : undefined,
  };
}

/**
 * Check volume condition for recent days range
 */
export function checkVolumeConditionInRange(
  data: StockDataPoint[],
  shortPeriod: number,
  longPeriod: number,
  threshold: number,
  recentDays: number,
  type: 'sma' | 'ema' = 'sma'
): { matched: boolean; matchedIndex?: number; details?: { short: VolumeAnalysis; long: VolumeAnalysis } } {
  const startIndex = Math.max(0, data.length - recentDays);

  for (let i = data.length - 1; i >= startIndex; i--) {
    const result = checkVolumeConditionAtIndex(data, shortPeriod, longPeriod, threshold, i, type);
    if (result.matched && result.details) {
      return {
        matched: true,
        matchedIndex: i,
        details: result.details,
      };
    }
  }

  return { matched: false };
}

/**
 * Get volume data for a specific date range
 */
export function getVolumeDataInRange(data: StockDataPoint[], startIndex: number, endIndex: number): number[] {
  if (startIndex < 0 || endIndex >= data.length || startIndex > endIndex) {
    return [];
  }

  return data.slice(startIndex, endIndex + 1).map((d) => d.volume);
}

/**
 * Calculate volume statistics
 */
export function calculateVolumeStats(volumes: number[]): {
  min: number;
  max: number;
  average: number;
  median: number;
} {
  if (volumes.length === 0) {
    return { min: 0, max: 0, average: 0, median: 0 };
  }

  const sorted = [...volumes].sort((a, b) => a - b);
  const sum = volumes.reduce((acc, vol) => acc + vol, 0);
  const average = sum / volumes.length;
  const median =
    sorted.length % 2 === 0
      ? ((sorted[sorted.length / 2 - 1] || 0) + (sorted[sorted.length / 2] || 0)) / 2
      : sorted[Math.floor(sorted.length / 2)] || 0;

  const min = sorted[0];
  const max = sorted[sorted.length - 1];

  return {
    min: typeof min === 'number' ? min : 0,
    max: typeof max === 'number' ? max : 0,
    average,
    median,
  };
}
