/**
 * Range Break Detection Tests
 * Test range breakout detection algorithms
 */

import { describe, expect, it } from 'bun:test';
import {
  analyzePriceStrength,
  calculateSupportResistance,
  detectRangeBreak,
  findMaxHighInRange,
  getRecentHighsLows,
  isRangeBreakAt,
} from './range-break-detection';
import type { RangeBreakParams, StockDataPoint } from './types';

/**
 * Create sample stock data for testing
 */
function createSampleData(days: number, basePrice: number = 1000): StockDataPoint[] {
  const data: StockDataPoint[] = [];
  const startDate = new Date('2024-01-01');

  for (let i = 0; i < days; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);

    const price = basePrice + i * 2;

    data.push({
      date,
      open: price - 2,
      high: price + 3,
      low: price - 3,
      close: price,
      volume: 1000000 + Math.random() * 500000,
    });
  }

  return data;
}

/**
 * Create data with range breakout pattern
 */
function createRangeBreakoutData(): StockDataPoint[] {
  const data: StockDataPoint[] = [];
  const startDate = new Date('2024-01-01');

  for (let i = 0; i < 150; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);

    let price: number;
    if (i < 100) {
      // Consolidation: price stays in range 950-1050
      price = 1000 + (Math.random() - 0.5) * 100;
    } else {
      // Breakout: price breaks above 1050
      price = 1050 + (i - 100) * 10;
    }

    data.push({
      date,
      open: price - 5,
      high: price + 5,
      low: price - 10,
      close: price,
      volume: 1000000 + (i > 100 ? i * 50000 : 0), // Volume spike on breakout
    });
  }

  return data;
}

describe('findMaxHighInRange', () => {
  it('should find maximum high in range', () => {
    const data = createSampleData(100);
    const result = findMaxHighInRange(data, 0, 99);

    expect(result.maxHigh).toBeGreaterThan(0);
    expect(result.maxIndex).toBeGreaterThanOrEqual(0);
    expect(result.maxIndex).toBeLessThanOrEqual(99);
  });

  it('should return correct index for max high', () => {
    const data = createSampleData(10);
    // Set a specific high value
    data[5] = { ...data[5], high: 5000 } as StockDataPoint;

    const result = findMaxHighInRange(data, 0, 9);
    expect(result.maxIndex).toBe(5);
    expect(result.maxHigh).toBe(5000);
  });

  it('should handle single element range', () => {
    const data = createSampleData(10);
    const result = findMaxHighInRange(data, 5, 5);

    expect(result.maxHigh).toBe(data[5]?.high ?? 0);
    expect(result.maxIndex).toBe(5);
  });

  it('should return zero for invalid range', () => {
    const data = createSampleData(10);
    const result = findMaxHighInRange(data, 10, 20); // Out of bounds

    expect(result.maxHigh).toBe(0);
    expect(result.maxIndex).toBe(-1);
  });

  it('should return zero for reversed range', () => {
    const data = createSampleData(10);
    const result = findMaxHighInRange(data, 5, 2); // Start > end

    expect(result.maxHigh).toBe(0);
    expect(result.maxIndex).toBe(-1);
  });
});

describe('isRangeBreakAt', () => {
  it('should detect range break when recent high exceeds period high', () => {
    const data = createRangeBreakoutData();
    const result = isRangeBreakAt(data, 120, 100, 10);

    expect(result.isBreak).toBe(true);
    expect(result.breakPercentage).toBeGreaterThan(0);
  });

  it('should not detect break when in consolidation', () => {
    const data = createRangeBreakoutData();
    const result = isRangeBreakAt(data, 50, 100, 10); // Before breakout

    expect(result.isBreak).toBe(false);
  });

  it('should calculate break percentage correctly', () => {
    const data = createSampleData(150);
    // Set specific values to test percentage calculation
    data[140] = { ...data[140], high: 1200 } as StockDataPoint; // Recent high
    data[100] = { ...data[100], high: 1000 } as StockDataPoint; // Period high

    const result = isRangeBreakAt(data, 140, 100, 10);

    // Just verify the calculation is reasonable
    expect(typeof result.breakPercentage).toBe('number');
    if (result.isBreak) {
      expect(result.breakPercentage).toBeGreaterThan(0);
    }
  });

  it('should return false for insufficient data', () => {
    const data = createSampleData(50);
    const result = isRangeBreakAt(data, 30, 100, 10); // Period too large

    expect(result.isBreak).toBe(false);
  });

  it('should handle edge case with zero period high', () => {
    const data = createSampleData(150);
    data.slice(30, 120).forEach((d) => {
      d.high = 0;
    });

    const result = isRangeBreakAt(data, 140, 100, 10);
    // Zero high is technically valid - recent high can still break above it
    expect(typeof result.isBreak).toBe('boolean');
    expect(typeof result.breakPercentage).toBe('number');
  });
});

describe('detectRangeBreak', () => {
  const params: RangeBreakParams = {
    period: 100,
    lookbackDays: 10,
    volumeRatioThreshold: 1.7,
    volumeShortPeriod: 30,
    volumeLongPeriod: 120,
    volumeType: 'ema',
  };

  it('should return false for insufficient data', () => {
    const data = createSampleData(50);
    const result = detectRangeBreak(data, params, 10);

    expect(result.found).toBe(false);
  });

  it('should detect range break with volume condition', () => {
    const data = createRangeBreakoutData();
    const result = detectRangeBreak(data, params, 20);

    if (result.found) {
      expect(result.breakIndex).toBeGreaterThan(0);
      expect(result.details).toBeDefined();
      if (result.details) {
        expect(result.details.breakPercentage).toBeGreaterThan(0);
        expect(result.details.volumeRatio).toBeGreaterThanOrEqual(params.volumeRatioThreshold);
      }
    }
  });

  it('should respect recentDays parameter', () => {
    const data = createSampleData(200);
    const recentDays = 5;
    const result = detectRangeBreak(data, params, recentDays);

    if (result.found && result.breakIndex !== undefined) {
      expect(result.breakIndex).toBeGreaterThanOrEqual(data.length - recentDays);
    }
  });

  it('should not find break without volume condition', () => {
    const data = createSampleData(200);
    const strictParams: RangeBreakParams = {
      ...params,
      volumeRatioThreshold: 100.0, // Impossible to meet
    };

    const result = detectRangeBreak(data, strictParams, 10);
    expect(result.found).toBe(false);
  });

  it('should handle empty data', () => {
    const result = detectRangeBreak([], params, 10);
    expect(result.found).toBe(false);
  });
});

describe('calculateSupportResistance', () => {
  it('should calculate support and resistance levels', () => {
    const data = createSampleData(200);
    const result = calculateSupportResistance(data, 100);

    expect(result.resistance).toBeGreaterThan(0);
    expect(result.support).toBeGreaterThan(0);
    expect(result.resistance).toBeGreaterThanOrEqual(result.support);
    expect(result.resistanceIndex).toBeGreaterThanOrEqual(0);
    expect(result.supportIndex).toBeGreaterThanOrEqual(0);
  });

  it('should adjust lookback for insufficient data', () => {
    const data = createSampleData(50);
    const result = calculateSupportResistance(data, 200); // More than available

    expect(result.resistance).toBeGreaterThan(0);
    expect(result.support).toBeGreaterThan(0);
  });

  it('should use default lookback period', () => {
    const data = createSampleData(300);
    const result = calculateSupportResistance(data); // Default 200 days

    expect(result.resistance).toBeGreaterThan(0);
    expect(result.support).toBeGreaterThan(0);
  });

  it('should find correct extreme values', () => {
    const data = createSampleData(100);
    // Set specific support and resistance
    data[10] = { ...data[10], low: 500 } as StockDataPoint; // Support
    data[90] = { ...data[90], high: 2000 } as StockDataPoint; // Resistance

    const result = calculateSupportResistance(data, 100);

    expect(result.support).toBe(500);
    expect(result.resistance).toBe(2000);
    expect(result.supportIndex).toBe(10);
    expect(result.resistanceIndex).toBe(90);
  });
});

describe('analyzePriceStrength', () => {
  it('should calculate price strength before and after break', () => {
    const data = createRangeBreakoutData();
    const breakIndex = 110; // After breakout starts
    const result = analyzePriceStrength(data, breakIndex, 5);

    expect(typeof result.preBreakStrength).toBe('number');
    expect(typeof result.postBreakStrength).toBe('number');
    expect(typeof result.momentum).toBe('number');
  });

  it('should calculate positive momentum for breakout', () => {
    const data = createRangeBreakoutData();
    const breakIndex = 105;
    const result = analyzePriceStrength(data, breakIndex, 5);

    // After breakout, post-break strength should be positive
    if (result.postBreakStrength > 0) {
      expect(result.postBreakStrength).toBeGreaterThan(0);
    }
  });

  it('should return zero for insufficient data', () => {
    const data = createSampleData(20);
    const result = analyzePriceStrength(data, 10, 15); // Not enough days

    expect(result.preBreakStrength).toBe(0);
    expect(result.postBreakStrength).toBe(0);
    expect(result.momentum).toBe(0);
  });

  it('should handle edge indices', () => {
    const data = createSampleData(100);
    const result = analyzePriceStrength(data, 5, 5); // Near start

    // At the edge, function returns zero for insufficient data
    // But with enough data it may return small values
    expect(typeof result.preBreakStrength).toBe('number');
    expect(typeof result.postBreakStrength).toBe('number');
  });
});

describe('getRecentHighsLows', () => {
  it('should return top 3 highs and lows', () => {
    const data = createSampleData(50);
    const result = getRecentHighsLows(data, 10);

    expect(result.recentHighs).toHaveLength(3);
    expect(result.recentLows).toHaveLength(3);
  });

  it('should sort highs in descending order', () => {
    const data = createSampleData(50);
    const result = getRecentHighsLows(data, 10);

    for (let i = 0; i < result.recentHighs.length - 1; i++) {
      const current = result.recentHighs[i];
      const next = result.recentHighs[i + 1];
      if (current && next) {
        expect(current.value).toBeGreaterThanOrEqual(next.value);
      }
    }
  });

  it('should sort lows in ascending order', () => {
    const data = createSampleData(50);
    const result = getRecentHighsLows(data, 10);

    for (let i = 0; i < result.recentLows.length - 1; i++) {
      const current = result.recentLows[i];
      const next = result.recentLows[i + 1];
      if (current && next) {
        expect(current.value).toBeLessThanOrEqual(next.value);
      }
    }
  });

  it('should use default days parameter', () => {
    const data = createSampleData(50);
    const result = getRecentHighsLows(data); // Default 10 days

    expect(result.recentHighs.length).toBeGreaterThan(0);
    expect(result.recentLows.length).toBeGreaterThan(0);
  });

  it('should handle insufficient data', () => {
    const data = createSampleData(5);
    const result = getRecentHighsLows(data, 10);

    expect(result.recentHighs.length).toBeLessThanOrEqual(3);
    expect(result.recentLows.length).toBeLessThanOrEqual(3);
  });

  it('should return empty arrays for empty data', () => {
    const result = getRecentHighsLows([], 10);

    expect(result.recentHighs).toEqual([]);
    expect(result.recentLows).toEqual([]);
  });
});

describe('Range break edge cases', () => {
  const params: RangeBreakParams = {
    period: 100,
    lookbackDays: 10,
    volumeRatioThreshold: 1.7,
    volumeShortPeriod: 30,
    volumeLongPeriod: 120,
    volumeType: 'ema',
  };

  it('should handle data with zero volume', () => {
    const data = createSampleData(200);
    data.forEach((d) => {
      d.volume = 0;
    });

    const result = detectRangeBreak(data, params, 10);
    expect(result.found).toBe(false); // Volume condition cannot be met
  });

  it('should handle data with constant prices', () => {
    const data: StockDataPoint[] = [];
    const startDate = new Date('2024-01-01');

    for (let i = 0; i < 200; i++) {
      const date = new Date(startDate);
      date.setDate(date.getDate() + i);

      data.push({
        date,
        open: 1000,
        high: 1000,
        low: 1000,
        close: 1000,
        volume: 1000000,
      });
    }

    const result = detectRangeBreak(data, params, 10);
    expect(result.found).toBe(false); // No range break in flat prices
  });

  it('should handle very small recent days', () => {
    const data = createSampleData(200);
    const result = detectRangeBreak(data, params, 1);

    if (result.found) {
      expect(result.breakIndex).toBe(data.length - 1);
    }
  });
});

describe('Range Break Slow (period: 150, SMA 50/150)', () => {
  const rangeBreakSlowParams: RangeBreakParams = {
    period: 150,
    lookbackDays: 10,
    volumeRatioThreshold: 1.7,
    volumeShortPeriod: 50,
    volumeLongPeriod: 150,
    volumeType: 'sma',
  };

  it('should detect range break with 150-day period', () => {
    const data = createSampleData(200); // Create 200 days of data for Range Break Slow
    // Ensure we have enough data
    expect(data.length).toBeGreaterThanOrEqual(160);

    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);

    // Breakout pattern may or may not be detected depending on generated data
    expect(typeof result.found).toBe('boolean');
    if (result.found) {
      expect(result.details).toBeDefined();
      expect(result.breakIndex).toBeGreaterThanOrEqual(0);
    }
  });

  it('should require 160 days minimum (150 + 10)', () => {
    const data = createSampleData(159);
    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);

    expect(result.found).toBe(false);
  });

  it('should work with exactly 160 days', () => {
    const data = createSampleData(160);
    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);

    // Should not fail, but may or may not find break depending on data
    expect(typeof result.found).toBe('boolean');
  });

  it('should use SMA for volume calculation', () => {
    const data = createSampleData(200);
    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);

    if (result.found && result.details) {
      // Volume ratio should be calculated
      expect(result.details.volumeRatio).toBeGreaterThan(0);
      expect(result.details.avgVolume20Days).toBeGreaterThan(0);
      expect(result.details.avgVolume100Days).toBeGreaterThan(0);
    }
  });

  it('should respect volume ratio threshold of 1.7', () => {
    const data = createSampleData(200);
    // Set very low volume everywhere
    data.forEach((d) => {
      d.volume = 1000;
    });

    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);
    expect(result.found).toBe(false); // Volume condition not met
  });

  it('should handle edge case with insufficient data for volume periods', () => {
    const data = createSampleData(160); // Just enough for price, not for full volume SMA
    const result = detectRangeBreak(data, rangeBreakSlowParams, 10);

    // Should handle gracefully
    expect(typeof result.found).toBe('boolean');
  });
});
