import { describe, expect, it } from 'vitest';
import type { DisplayTimeframe } from '@/stores/chartStore';
import type { ChartData } from '@/types/chart';
import {
  getAtrSupportData,
  getBollingerBandsData,
  getIndicatorData,
  getNBarSupportData,
  getPpoData,
  getTradingValueMAData,
  getVolumeComparisonData,
} from './chartDataHelpers';
import { isIndicatorValueArray } from './typeGuards';

const timeframe: DisplayTimeframe = 'daily';

function makeChartDataMap(data: Partial<ChartData>): Record<DisplayTimeframe, ChartData | undefined> {
  return {
    daily: {
      candlestickData: [],
      indicators: {},
      ...data,
    } as ChartData,
    weekly: undefined,
    monthly: undefined,
  };
}

describe('getIndicatorData', () => {
  it('returns data when indicator exists and passes validation', () => {
    const chartData = makeChartDataMap({
      indicators: {
        test: [{ time: '2024-01-01', value: 100 }],
      },
    });
    const result = getIndicatorData(chartData, timeframe, 'test', isIndicatorValueArray);
    expect(result).toEqual([{ time: '2024-01-01', value: 100 }]);
  });

  it('returns undefined when indicator does not exist', () => {
    const chartData = makeChartDataMap({ indicators: {} });
    const result = getIndicatorData(chartData, timeframe, 'missing', isIndicatorValueArray);
    expect(result).toBeUndefined();
  });

  it('returns undefined when chartData is undefined', () => {
    const result = getIndicatorData(undefined, timeframe, 'test', isIndicatorValueArray);
    expect(result).toBeUndefined();
  });

  it('returns undefined when validation fails', () => {
    const chartData = makeChartDataMap({
      indicators: { test: [{ time: '2024-01-01' }] as never },
    });
    const result = getIndicatorData(chartData, timeframe, 'test', isIndicatorValueArray);
    expect(result).toBeUndefined();
  });
});

describe('getAtrSupportData', () => {
  it('returns atrSupport indicator data', () => {
    const chartData = makeChartDataMap({
      indicators: { atrSupport: [{ time: '2024-01-01', value: 50 }] },
    });
    expect(getAtrSupportData(chartData, timeframe)).toEqual([{ time: '2024-01-01', value: 50 }]);
  });
});

describe('getNBarSupportData', () => {
  it('returns nBarSupport indicator data', () => {
    const chartData = makeChartDataMap({
      indicators: { nBarSupport: [{ time: '2024-01-01', value: 60 }] },
    });
    expect(getNBarSupportData(chartData, timeframe)).toEqual([{ time: '2024-01-01', value: 60 }]);
  });
});

describe('getPpoData', () => {
  it('returns ppo indicator data', () => {
    const chartData = makeChartDataMap({
      indicators: {
        ppo: [{ time: '2024-01-01', ppo: 1, signal: 0.5, histogram: 0.5 }],
      },
    });
    expect(getPpoData(chartData, timeframe)).toEqual([{ time: '2024-01-01', ppo: 1, signal: 0.5, histogram: 0.5 }]);
  });
});

describe('getBollingerBandsData', () => {
  it('returns bollinger bands data', () => {
    const chartData = makeChartDataMap({
      bollingerBands: [{ time: '2024-01-01', upper: 110, middle: 100, lower: 90 }],
    });
    expect(getBollingerBandsData(chartData, timeframe)).toEqual([
      { time: '2024-01-01', upper: 110, middle: 100, lower: 90 },
    ]);
  });

  it('returns undefined when data is missing', () => {
    const chartData = makeChartDataMap({});
    expect(getBollingerBandsData(chartData, timeframe)).toBeUndefined();
  });
});

describe('getVolumeComparisonData', () => {
  it('returns volume comparison data', () => {
    const chartData = makeChartDataMap({
      volumeComparison: [
        {
          time: '2024-01-01',
          shortMA: 100,
          longThresholdLower: 80,
          longThresholdHigher: 120,
        },
      ],
    });
    expect(getVolumeComparisonData(chartData, timeframe)).toEqual([
      {
        time: '2024-01-01',
        shortMA: 100,
        longThresholdLower: 80,
        longThresholdHigher: 120,
      },
    ]);
  });
});

describe('getTradingValueMAData', () => {
  it('returns trading value MA data', () => {
    const chartData = makeChartDataMap({
      tradingValueMA: [{ time: '2024-01-01', value: 1000 }],
    });
    expect(getTradingValueMAData(chartData, timeframe)).toEqual([{ time: '2024-01-01', value: 1000 }]);
  });

  it('returns undefined when data is missing', () => {
    const chartData = makeChartDataMap({});
    expect(getTradingValueMAData(chartData, timeframe)).toBeUndefined();
  });
});
