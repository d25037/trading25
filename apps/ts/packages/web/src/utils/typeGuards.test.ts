import { describe, expect, it } from 'vitest';
import {
  hasVolumeData,
  isArrayOf,
  isBollingerBandsData,
  isBollingerBandsDataArray,
  isIndicatorValue,
  isIndicatorValueArray,
  isMACDIndicatorData,
  isPPOIndicatorData,
  isPPOIndicatorDataArray,
  isStockDataPoint,
  isTradingValueMAData,
  isTradingValueMADataArray,
  isValidIndicatorValue,
  isVolumeComparisonData,
  isVolumeComparisonDataArray,
} from './typeGuards';

describe('isStockDataPoint', () => {
  it('returns true for valid stock data point', () => {
    expect(
      isStockDataPoint({
        time: '2024-01-01',
        open: 100,
        high: 110,
        low: 90,
        close: 105,
      })
    ).toBe(true);
  });

  it('returns false for null', () => {
    expect(isStockDataPoint(null)).toBe(false);
  });

  it('returns false for missing fields', () => {
    expect(isStockDataPoint({ time: '2024-01-01', open: 100 })).toBe(false);
  });

  it('returns false for non-object', () => {
    expect(isStockDataPoint('string')).toBe(false);
  });
});

describe('hasVolumeData', () => {
  it('returns true when volume is a positive number', () => {
    const point = { time: '2024-01-01', open: 100, high: 110, low: 90, close: 105, volume: 1000 };
    expect(hasVolumeData(point)).toBe(true);
  });

  it('returns false when volume is 0', () => {
    const point = { time: '2024-01-01', open: 100, high: 110, low: 90, close: 105, volume: 0 };
    expect(hasVolumeData(point)).toBe(false);
  });

  it('returns false when volume is undefined', () => {
    const point = { time: '2024-01-01', open: 100, high: 110, low: 90, close: 105 };
    expect(hasVolumeData(point as never)).toBe(false);
  });
});

describe('isValidIndicatorValue', () => {
  it('returns true for finite number', () => {
    expect(isValidIndicatorValue(42)).toBe(true);
  });

  it('returns false for NaN', () => {
    expect(isValidIndicatorValue(Number.NaN)).toBe(false);
  });

  it('returns false for Infinity', () => {
    expect(isValidIndicatorValue(Number.POSITIVE_INFINITY)).toBe(false);
  });

  it('returns false for string', () => {
    expect(isValidIndicatorValue('42')).toBe(false);
  });
});

describe('isIndicatorValue', () => {
  it('returns true for valid indicator value', () => {
    expect(isIndicatorValue({ time: '2024-01-01', value: 100 })).toBe(true);
  });

  it('returns false for null', () => {
    expect(isIndicatorValue(null)).toBe(false);
  });

  it('returns false for missing value field', () => {
    expect(isIndicatorValue({ time: '2024-01-01' })).toBe(false);
  });
});

describe('isMACDIndicatorData', () => {
  it('returns true for valid MACD data', () => {
    expect(isMACDIndicatorData({ time: '2024-01-01', macd: 1, signal: 0.5, histogram: 0.5 })).toBe(true);
  });

  it('returns false for missing histogram', () => {
    expect(isMACDIndicatorData({ time: '2024-01-01', macd: 1, signal: 0.5 })).toBe(false);
  });

  it('returns false for null', () => {
    expect(isMACDIndicatorData(null)).toBe(false);
  });
});

describe('isPPOIndicatorData', () => {
  it('returns true for valid PPO data', () => {
    expect(isPPOIndicatorData({ time: '2024-01-01', ppo: 1, signal: 0.5, histogram: 0.5 })).toBe(true);
  });

  it('returns false for missing fields', () => {
    expect(isPPOIndicatorData({ time: '2024-01-01', ppo: 1 })).toBe(false);
  });
});

describe('isBollingerBandsData', () => {
  it('returns true for valid bollinger data', () => {
    expect(isBollingerBandsData({ time: '2024-01-01', upper: 110, middle: 100, lower: 90 })).toBe(true);
  });

  it('returns false for missing lower', () => {
    expect(isBollingerBandsData({ time: '2024-01-01', upper: 110, middle: 100 })).toBe(false);
  });
});

describe('isVolumeComparisonData', () => {
  it('returns true for valid volume comparison data', () => {
    expect(
      isVolumeComparisonData({
        time: '2024-01-01',
        shortMA: 100,
        longThresholdLower: 80,
        longThresholdHigher: 120,
      })
    ).toBe(true);
  });

  it('returns false for missing fields', () => {
    expect(isVolumeComparisonData({ time: '2024-01-01' })).toBe(false);
  });
});

describe('isTradingValueMAData', () => {
  it('returns true for valid data', () => {
    expect(isTradingValueMAData({ time: '2024-01-01', value: 1000 })).toBe(true);
  });

  it('returns false for null', () => {
    expect(isTradingValueMAData(null)).toBe(false);
  });
});

describe('isArrayOf', () => {
  it('returns true for valid typed array', () => {
    const data = [
      { time: '2024-01-01', value: 100 },
      { time: '2024-01-02', value: 200 },
    ];
    expect(isArrayOf(data, isIndicatorValue)).toBe(true);
  });

  it('returns false for non-array', () => {
    expect(isArrayOf('not array', isIndicatorValue)).toBe(false);
  });

  it('returns true for empty array', () => {
    expect(isArrayOf([], isIndicatorValue)).toBe(true);
  });

  it('returns false when one element fails guard', () => {
    const data = [{ time: '2024-01-01', value: 100 }, { time: '2024-01-02' }];
    expect(isArrayOf(data, isIndicatorValue)).toBe(false);
  });
});

describe('array guard wrappers', () => {
  it('isIndicatorValueArray validates correctly', () => {
    expect(isIndicatorValueArray([{ time: '2024-01-01', value: 100 }])).toBe(true);
    expect(isIndicatorValueArray([{ time: '2024-01-01' }])).toBe(false);
  });

  it('isPPOIndicatorDataArray validates correctly', () => {
    expect(isPPOIndicatorDataArray([{ time: '2024-01-01', ppo: 1, signal: 0.5, histogram: 0.5 }])).toBe(true);
    expect(isPPOIndicatorDataArray([{ time: '2024-01-01' }])).toBe(false);
  });

  it('isBollingerBandsDataArray validates correctly', () => {
    expect(isBollingerBandsDataArray([{ time: '2024-01-01', upper: 110, middle: 100, lower: 90 }])).toBe(true);
  });

  it('isVolumeComparisonDataArray validates correctly', () => {
    expect(
      isVolumeComparisonDataArray([
        {
          time: '2024-01-01',
          shortMA: 100,
          longThresholdLower: 80,
          longThresholdHigher: 120,
        },
      ])
    ).toBe(true);
  });

  it('isTradingValueMADataArray validates correctly', () => {
    expect(isTradingValueMADataArray([{ time: '2024-01-01', value: 1000 }])).toBe(true);
  });
});
