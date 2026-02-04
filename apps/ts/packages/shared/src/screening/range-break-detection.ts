/**
 * Range Break Detection
 * Detect price breakouts above historical highs for screening
 */

import type { RangeBreakDetails, RangeBreakParams, StockDataPoint } from './types';
import { checkVolumeConditionAtIndex } from './volume-utils';

export interface RangeBreakResult {
  found: boolean;
  breakIndex?: number;
  details?: RangeBreakDetails;
}

/**
 * Find maximum high in a given range
 */
export function findMaxHighInRange(
  data: StockDataPoint[],
  startIndex: number,
  endIndex: number
): { maxHigh: number; maxIndex: number } {
  if (startIndex < 0 || endIndex >= data.length || startIndex > endIndex) {
    return { maxHigh: 0, maxIndex: -1 };
  }

  let maxHigh = data[startIndex]?.high || 0;
  let maxIndex = startIndex;

  for (let i = startIndex + 1; i <= endIndex; i++) {
    const high = data[i]?.high || 0;
    if (high > maxHigh) {
      maxHigh = high;
      maxIndex = i;
    }
  }

  return { maxHigh, maxIndex };
}

/**
 * Check if there's a range break at specific index
 * Range break: Recent max high (lookbackDays) >= Long-term max high (period)
 */
export function isRangeBreakAt(
  data: StockDataPoint[],
  index: number,
  period: number, // Long-term period (e.g., 100 days)
  lookbackDays: number // Short-term period (e.g., 10 days)
): { isBreak: boolean; recentMaxHigh: number; periodMaxHigh: number; breakPercentage: number } {
  if (index < period || index >= data.length) {
    return { isBreak: false, recentMaxHigh: 0, periodMaxHigh: 0, breakPercentage: 0 };
  }

  // Recent max high: max(data[i-lookbackDays+1] to data[i])
  const recentStartIndex = index - lookbackDays + 1;
  const recentEndIndex = index;
  const { maxHigh: recentMaxHigh } = findMaxHighInRange(data, recentStartIndex, recentEndIndex);

  // Long-term max high: max(data[i-period] to data[i-lookbackDays])
  const periodStartIndex = index - period;
  const periodEndIndex = index - lookbackDays;
  const { maxHigh: periodMaxHigh } = findMaxHighInRange(data, periodStartIndex, periodEndIndex);

  if (periodMaxHigh === 0 || recentMaxHigh === 0) {
    return { isBreak: false, recentMaxHigh, periodMaxHigh, breakPercentage: 0 };
  }

  const isBreak = recentMaxHigh >= periodMaxHigh;
  const breakPercentage = ((recentMaxHigh - periodMaxHigh) / periodMaxHigh) * 100;

  return { isBreak, recentMaxHigh, periodMaxHigh, breakPercentage };
}

/**
 * Detect range break in recent days
 */
export function detectRangeBreak(
  data: StockDataPoint[],
  params: RangeBreakParams,
  recentDays: number
): RangeBreakResult {
  if (data.length < params.period + recentDays) {
    return { found: false };
  }

  // Check for range break in recent days
  const endIndex = data.length - 1;
  const startIndex = Math.max(params.period, endIndex - recentDays + 1);

  for (let i = endIndex; i >= startIndex; i--) {
    const rangeBreakCheck = isRangeBreakAt(data, i, params.period, params.lookbackDays);

    if (rangeBreakCheck.isBreak) {
      // Check volume condition at the specific day when range break occurred
      const volumeCheck = checkVolumeConditionAtIndex(
        data,
        params.volumeShortPeriod,
        params.volumeLongPeriod,
        params.volumeRatioThreshold,
        i,
        params.volumeType
      );

      if (volumeCheck.matched && volumeCheck.details) {
        const breakDetails: RangeBreakDetails = {
          breakDate: data[i]?.date || new Date(),
          currentHigh: rangeBreakCheck.recentMaxHigh,
          maxHighInLookback: rangeBreakCheck.periodMaxHigh,
          breakPercentage: rangeBreakCheck.breakPercentage,
          volumeRatio: volumeCheck.details.short.average / volumeCheck.details.long.average,
          avgVolume20Days: volumeCheck.details.short.average,
          avgVolume100Days: volumeCheck.details.long.average,
        };

        return {
          found: true,
          breakIndex: i,
          details: breakDetails,
        };
      }
    }
  }

  return { found: false };
}

/**
 * Calculate support and resistance levels
 */
export function calculateSupportResistance(
  data: StockDataPoint[],
  lookbackDays: number = 200
): {
  resistance: number;
  support: number;
  resistanceIndex: number;
  supportIndex: number;
} {
  if (data.length < lookbackDays) {
    lookbackDays = data.length;
  }

  const startIndex = Math.max(0, data.length - lookbackDays);
  const endIndex = data.length - 1;

  let maxHigh = data[startIndex]?.high || 0;
  let minLow = data[startIndex]?.low || Number.MAX_SAFE_INTEGER;
  let resistanceIndex = startIndex;
  let supportIndex = startIndex;

  for (let i = startIndex; i <= endIndex; i++) {
    const high = data[i]?.high || 0;
    const low = data[i]?.low || 0;

    if (high > maxHigh) {
      maxHigh = high;
      resistanceIndex = i;
    }

    if (low < minLow) {
      minLow = low;
      supportIndex = i;
    }
  }

  return {
    resistance: maxHigh,
    support: minLow,
    resistanceIndex,
    supportIndex,
  };
}

/**
 * Analyze price strength around break
 */
export function analyzePriceStrength(
  data: StockDataPoint[],
  breakIndex: number,
  days: number = 5
): {
  preBreakStrength: number;
  postBreakStrength: number;
  momentum: number;
} {
  if (breakIndex - days < 0 || breakIndex + days >= data.length) {
    return { preBreakStrength: 0, postBreakStrength: 0, momentum: 0 };
  }

  // Calculate pre-break strength (price trend before break)
  const preStartIndex = breakIndex - days;
  const preStartPrice = data[preStartIndex]?.close || 0;
  const preEndPrice = data[breakIndex - 1]?.close || 0;
  const preBreakStrength = preStartPrice > 0 ? (preEndPrice - preStartPrice) / preStartPrice : 0;

  // Calculate post-break strength (price trend after break)
  const postStartPrice = data[breakIndex]?.close || 0;
  const postEndIndex = Math.min(breakIndex + days, data.length - 1);
  const postEndPrice = data[postEndIndex]?.close || 0;
  const postBreakStrength = postStartPrice > 0 ? (postEndPrice - postStartPrice) / postStartPrice : 0;

  // Calculate momentum (acceleration)
  const momentum = postBreakStrength - preBreakStrength;

  return { preBreakStrength, postBreakStrength, momentum };
}

/**
 * Get recent highs and lows
 */
export function getRecentHighsLows(
  data: StockDataPoint[],
  days: number = 10
): {
  recentHighs: { value: number; index: number; date: Date }[];
  recentLows: { value: number; index: number; date: Date }[];
} {
  if (data.length < days) {
    return { recentHighs: [], recentLows: [] };
  }

  const startIndex = Math.max(0, data.length - days);
  const recentData = data.slice(startIndex);

  const recentHighs = recentData
    .map((d, i) => ({ value: d.high, index: startIndex + i, date: d.date }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 3);

  const recentLows = recentData
    .map((d, i) => ({ value: d.low, index: startIndex + i, date: d.date }))
    .sort((a, b) => a.value - b.value)
    .slice(0, 3);

  return { recentHighs, recentLows };
}
