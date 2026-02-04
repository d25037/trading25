import { describe, expect, it } from 'bun:test';
import type { StockDataPoint } from './types';
import {
  calculateVolumeMA,
  calculateVolumeStats,
  checkVolumeCondition,
  checkVolumeConditionAtIndex,
  checkVolumeConditionInRange,
  getVolumeAnalysis,
  getVolumeDataInRange,
} from './volume-utils';

function makeData(volumes: number[]): StockDataPoint[] {
  return volumes.map((volume, i) => ({
    date: new Date(`2024-01-${String(i + 1).padStart(2, '0')}`),
    open: 100,
    high: 110,
    low: 90,
    close: 100,
    volume,
    code: '7203',
  }));
}

describe('calculateVolumeMA', () => {
  it('calculates SMA of volumes', () => {
    const data = makeData([100, 200, 300, 400, 500]);
    const result = calculateVolumeMA(data, 3, 'sma');
    expect(result).toEqual([200, 300, 400]);
  });

  it('calculates EMA of volumes', () => {
    const data = makeData([100, 200, 300, 400, 500]);
    const result = calculateVolumeMA(data, 3, 'ema');
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('getVolumeAnalysis', () => {
  it('returns analysis for valid data', () => {
    const data = makeData([100, 200, 300, 400, 500]);
    const result = getVolumeAnalysis(data, 3, 4);
    expect(result).not.toBeNull();
    expect(result?.period).toBe(3);
    expect(result?.current).toBe(500);
    expect(result?.ratio).toBeGreaterThan(0);
  });

  it('returns null for empty data', () => {
    expect(getVolumeAnalysis([], 3)).toBeNull();
  });

  it('returns null for invalid index', () => {
    const data = makeData([100, 200, 300]);
    expect(getVolumeAnalysis(data, 3, -1)).toBeNull();
    expect(getVolumeAnalysis(data, 3, 10)).toBeNull();
  });

  it('returns null when data length < period', () => {
    const data = makeData([100, 200]);
    expect(getVolumeAnalysis(data, 3)).toBeNull();
  });
});

describe('checkVolumeCondition', () => {
  it('returns true when short MA > long MA * threshold', () => {
    // 20 data points with increasing volumes at the end
    // Short period (3) at index 14: average of [500, 500, 500] = 500
    // Long period (10) at index 14: average of [50, 50, 50, 50, 50, 500, 500, 500, 500, 500] = 275
    // 500 > 275 * 1.0 → true
    const data = makeData([50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 500, 500, 500, 500, 500]);
    const result = checkVolumeCondition(data, 3, 10, 1.0, 14);
    expect(result).toBe(true);
  });

  it('returns false when condition not met', () => {
    const data = makeData([100, 100, 100, 100, 100, 100, 100, 100, 100, 100]);
    const result = checkVolumeCondition(data, 3, 5, 2.0, data.length - 1);
    expect(result).toBe(false);
  });

  it('returns false when insufficient data', () => {
    const data = makeData([100]);
    expect(checkVolumeCondition(data, 2, 3, 1.0)).toBe(false);
  });
});

describe('checkVolumeConditionAtIndex', () => {
  it('returns matched=true with details when condition met', () => {
    const data = makeData([50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 500, 500, 500, 500, 500]);
    const result = checkVolumeConditionAtIndex(data, 3, 10, 1.0, 14);
    expect(result.matched).toBe(true);
    expect(result.details).toBeDefined();
    expect(result.details?.short.period).toBe(3);
  });

  it('returns matched=false without details', () => {
    const data = makeData([100, 100, 100, 100, 100, 100, 100, 100, 100, 100]);
    const result = checkVolumeConditionAtIndex(data, 3, 5, 5.0, data.length - 1);
    expect(result.matched).toBe(false);
    expect(result.details).toBeUndefined();
  });

  it('returns not matched for insufficient data', () => {
    const data = makeData([100]);
    const result = checkVolumeConditionAtIndex(data, 2, 3, 1.0, 0);
    expect(result.matched).toBe(false);
  });
});

describe('checkVolumeConditionInRange', () => {
  it('finds match in range', () => {
    const data = makeData([100, 100, 100, 100, 100, 500, 500, 500, 500, 500]);
    const result = checkVolumeConditionInRange(data, 3, 5, 1.0, 5);
    expect(result.matched).toBe(true);
    expect(result.matchedIndex).toBeDefined();
    expect(result.details).toBeDefined();
  });

  it('returns not matched when no match in range', () => {
    const data = makeData([100, 100, 100, 100, 100]);
    const result = checkVolumeConditionInRange(data, 2, 3, 5.0, 3);
    expect(result.matched).toBe(false);
  });
});

describe('getVolumeDataInRange', () => {
  it('returns volumes for valid range', () => {
    const data = makeData([100, 200, 300, 400, 500]);
    expect(getVolumeDataInRange(data, 1, 3)).toEqual([200, 300, 400]);
  });

  it('returns empty for invalid range', () => {
    const data = makeData([100, 200, 300]);
    expect(getVolumeDataInRange(data, -1, 2)).toEqual([]);
    expect(getVolumeDataInRange(data, 0, 10)).toEqual([]);
    expect(getVolumeDataInRange(data, 2, 1)).toEqual([]);
  });
});

describe('calculateVolumeStats', () => {
  it('calculates stats correctly', () => {
    const stats = calculateVolumeStats([10, 20, 30, 40, 50]);
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(50);
    expect(stats.average).toBe(30);
    expect(stats.median).toBe(30);
  });

  it('calculates median for even count', () => {
    const stats = calculateVolumeStats([10, 20, 30, 40]);
    expect(stats.median).toBe(25);
  });

  it('returns zeros for empty array', () => {
    const stats = calculateVolumeStats([]);
    expect(stats.min).toBe(0);
    expect(stats.max).toBe(0);
    expect(stats.average).toBe(0);
    expect(stats.median).toBe(0);
  });
});
