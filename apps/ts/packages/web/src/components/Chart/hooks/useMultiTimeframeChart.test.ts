import { describe, expect, it } from 'vitest';
import type { ChartData } from '@/types/chart';
import { applyShikihoChartOverlay, type WorkbenchDailyChartOverlay } from './useMultiTimeframeChart';

const daily: ChartData = {
  candlestickData: [{ time: '2026-07-10', open: 100, high: 110, low: 95, close: 108, volume: 10 }],
  indicators: { sma: [{ time: '2026-07-10', value: 105 }] },
};
const weekly: ChartData = {
  candlestickData: [{ time: '2026-07-06', open: 90, high: 110, low: 85, close: 108, volume: 50 }],
  indicators: { sma: [{ time: '2026-07-06', value: 100 }] },
};
const overlay: WorkbenchDailyChartOverlay = {
  dailyBars: [
    ...daily.candlestickData,
    { time: '2026-07-13', open: 112, high: 125, low: 110, close: 120, volume: 123 },
  ],
  chartSmaPoint: { time: '2026-07-13', value: 109.2 },
  provenance: {
    provisional: true,
    tradingDate: '2026-07-13',
    observedAt: '2026-07-13T01:30:00Z',
    delayMinutes: 15,
    sourceLabel: '会社四季報オンライン',
  },
};

describe('applyShikihoChartOverlay', () => {
  it('replaces only daily bars and appends a local SMA5 point', () => {
    const chartData = { daily, weekly, monthly: weekly };
    const result = applyShikihoChartOverlay(chartData, overlay, false);
    expect(result.daily?.candlestickData).toEqual(overlay.dailyBars);
    expect(result.daily?.indicators.sma).toEqual([
      { time: '2026-07-10', value: 105 },
      { time: '2026-07-13', value: 109.2 },
    ]);
    expect(result.weekly).toBe(weekly);
    expect(result.monthly).toBe(weekly);
    expect(result.provenance).toBe(overlay.provenance);
    expect(chartData.daily).toBe(daily);
  });

  it('keeps every timeframe official in relative mode', () => {
    const chartData = { daily, weekly, monthly: weekly };
    const result = applyShikihoChartOverlay(chartData, overlay, true);
    expect(result).toEqual({ ...chartData, provenance: null });
    expect(result.daily).toBe(daily);
  });

  it('does not append a point when the configured SMA value is unavailable', () => {
    const chartData = { daily, weekly, monthly: weekly };
    const result = applyShikihoChartOverlay(chartData, { ...overlay, chartSmaPoint: null }, false);
    expect(result.daily?.indicators.sma).toBe(daily.indicators.sma);
  });
});
