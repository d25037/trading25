import { renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
} from '@/constants/fundamentalMetrics';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
} from '@/constants/fundamentalsHistoryMetrics';
import { apiPost } from '@/lib/api-client';
import type { ChartSettings } from '@/stores/chartStore';
import { createTestWrapper } from '@/test-utils';
import { btIndicatorKeys, buildIndicatorSpecs, mapBtResponseToChartData, useBtIndicators } from './useBtIndicators';

vi.mock('@/lib/api-client', () => ({
  apiPost: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

const mockApiPost = apiPost as ReturnType<typeof vi.fn>;

// ===== buildIndicatorSpecs Tests =====

describe('buildIndicatorSpecs', () => {
  const baseSettings = {
    timeframe: '1D' as const,
    displayTimeframe: 'daily' as const,
    indicators: {
      sma: { enabled: false, period: 20 },
      ema: { enabled: false, period: 12 },
      macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
      ppo: { enabled: false, fast: 12, slow: 26, signal: 9 },
      atrSupport: { enabled: false, period: 20, multiplier: 2.0 },
      nBarSupport: { enabled: false, period: 20 },
      bollinger: { enabled: false, period: 20, deviation: 2.0 },
    },
    volumeComparison: { shortPeriod: 20, longPeriod: 100, lowerMultiplier: 1.0, higherMultiplier: 1.5 },
    tradingValueMA: { period: 20 },
    riskAdjustedReturn: {
      lookbackPeriod: 60,
      ratioType: 'sortino' as const,
      threshold: 1.0,
      condition: 'above' as const,
    },
    chartType: 'candlestick' as const,
    showVolume: true,
    showPPOChart: false,
    showVolumeComparison: false,
    showTradingValueMA: false,
    showRiskAdjustedReturnChart: false,
    showFundamentalsPanel: true,
    showFundamentalsHistoryPanel: true,
    showMarginPressurePanel: true,
    showFactorRegressionPanel: true,
    fundamentalsPanelOrder: [
      'fundamentals',
      'fundamentalsHistory',
      'marginPressure',
      'factorRegression',
    ] as ChartSettings['fundamentalsPanelOrder'],
    fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
    fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
    fundamentalsHistoryMetricOrder: [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER],
    fundamentalsHistoryMetricVisibility: { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY },
    visibleBars: 200,
    relativeMode: false,
    signalOverlay: { enabled: false, signals: [] },
  };

  it('should return empty array when no indicators enabled', () => {
    const specs = buildIndicatorSpecs(baseSettings);
    expect(specs).toEqual([]);
  });

  it('should include SMA when enabled', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, sma: { enabled: true, period: 25 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({ type: 'sma', params: { period: 25 } });
  });

  it('should include EMA when enabled', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, ema: { enabled: true, period: 50 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({ type: 'ema', params: { period: 50 } });
  });

  it('should map MACD params correctly', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, macd: { enabled: true, fast: 8, slow: 21, signal: 5 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'macd',
      params: { fast_period: 8, slow_period: 21, signal_period: 5 },
    });
  });

  it('should map PPO params correctly', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, ppo: { enabled: true, fast: 10, slow: 30, signal: 7 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'ppo',
      params: { fast_period: 10, slow_period: 30, signal_period: 7 },
    });
  });

  it('should map Bollinger params correctly', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, bollinger: { enabled: true, period: 30, deviation: 2.5 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'bollinger',
      params: { period: 30, std_dev: 2.5 },
    });
  });

  it('should map ATR Support params correctly', () => {
    const settings = {
      ...baseSettings,
      indicators: { ...baseSettings.indicators, atrSupport: { enabled: true, period: 14, multiplier: 3.0 } },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'atr_support',
      params: { lookback_period: 14, atr_multiplier: 3.0 },
    });
  });

  it('should include volume_comparison with lower/higher multiplier', () => {
    const settings = {
      ...baseSettings,
      showVolumeComparison: true,
      volumeComparison: { shortPeriod: 10, longPeriod: 50, lowerMultiplier: 0.8, higherMultiplier: 2.0 },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'volume_comparison',
      params: { short_period: 10, long_period: 50, lower_multiplier: 0.8, higher_multiplier: 2.0 },
    });
  });

  it('should include trading_value_ma when showTradingValueMA is true', () => {
    const settings = { ...baseSettings, showTradingValueMA: true, tradingValueMA: { period: 30 } };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({ type: 'trading_value_ma', params: { period: 30 } });
  });

  it('should include risk_adjusted_return when showRiskAdjustedReturnChart is true', () => {
    const settings = {
      ...baseSettings,
      showRiskAdjustedReturnChart: true,
      riskAdjustedReturn: {
        lookbackPeriod: 80,
        ratioType: 'sharpe' as const,
        threshold: 1.2,
        condition: 'below' as const,
      },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toContainEqual({
      type: 'risk_adjusted_return',
      params: { lookback_period: 80, ratio_type: 'sharpe' },
    });
  });

  it('should build multiple specs when multiple indicators enabled', () => {
    const settings = {
      ...baseSettings,
      indicators: {
        ...baseSettings.indicators,
        sma: { enabled: true, period: 20 },
        macd: { enabled: true, fast: 12, slow: 26, signal: 9 },
      },
    };
    const specs = buildIndicatorSpecs(settings);
    expect(specs).toHaveLength(2);
  });
});

// ===== mapBtResponseToChartData Tests =====

describe('mapBtResponseToChartData', () => {
  it('should transform SMA records with date→time', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [
          { date: '2024-01-01', value: 100.5 },
          { date: '2024-01-02', value: 101.0 },
        ],
      },
    };
    const result = mapBtResponseToChartData(response);
    expect(result.indicators.sma).toHaveLength(2);
    const smaData = result.indicators.sma as Array<{ time: string; value: number }>;
    expect(smaData[0]).toEqual({ time: '2024-01-01', value: 100.5 });
  });

  it('should transform MACD records', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        macd_12_26_9: [{ date: '2024-01-01', macd: 1.5, signal: 1.0, histogram: 0.5 }],
      },
    };
    const result = mapBtResponseToChartData(response);
    const macdData = result.indicators.macd as Array<{ time: string; macd: number; signal: number; histogram: number }>;
    expect(macdData[0]).toEqual({ time: '2024-01-01', macd: 1.5, signal: 1.0, histogram: 0.5 });
  });

  it('should transform PPO records', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        ppo_12_26_9: [{ date: '2024-01-01', ppo: 2.0, signal: 1.5, histogram: 0.5 }],
      },
    };
    const result = mapBtResponseToChartData(response);
    const ppoData = result.indicators.ppo as Array<{ time: string; ppo: number; signal: number; histogram: number }>;
    expect(ppoData[0]).toEqual({ time: '2024-01-01', ppo: 2.0, signal: 1.5, histogram: 0.5 });
  });

  it('should transform Bollinger records into bollingerBands', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        'bollinger_20_2.0': [{ date: '2024-01-01', upper: 110, middle: 100, lower: 90 }],
      },
    };
    const result = mapBtResponseToChartData(response);
    expect(result.bollingerBands).toHaveLength(1);
    expect(result.bollingerBands?.[0]).toEqual({ time: '2024-01-01', upper: 110, middle: 100, lower: 90 });
  });

  it('should transform volume_comparison records', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        'volume_comparison_20_100_1.0_1.5_sma': [
          { date: '2024-01-01', shortMA: 50000, longThresholdLower: 40000, longThresholdHigher: 60000 },
        ],
      },
    };
    const result = mapBtResponseToChartData(response);
    expect(result.volumeComparison).toHaveLength(1);
    expect(result.volumeComparison?.[0]).toEqual({
      time: '2024-01-01',
      shortMA: 50000,
      longThresholdLower: 40000,
      longThresholdHigher: 60000,
    });
  });

  it('should filter out records with null values', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [
          { date: '2024-01-01', value: null },
          { date: '2024-01-02', value: 100 },
        ],
      },
    };
    const result = mapBtResponseToChartData(response);
    expect(result.indicators.sma).toHaveLength(1);
  });

  it('should return empty indicators when response is empty', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 0 },
      indicators: {},
    };
    const result = mapBtResponseToChartData(response);
    expect(result.indicators).toEqual({});
    expect(result.bollingerBands).toBeUndefined();
  });

  it('should transform risk_adjusted_return records', () => {
    const response = {
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        risk_adjusted_return_60_sortino: [{ date: '2024-01-01', value: 1.25 }],
      },
    };
    const result = mapBtResponseToChartData(response);
    expect(result.indicators.riskAdjustedReturn).toEqual([{ time: '2024-01-01', value: 1.25 }]);
  });
});

// ===== btIndicatorKeys Tests =====

describe('btIndicatorKeys', () => {
  it('should generate correct compute key', () => {
    const key = btIndicatorKeys.compute('7203', 'daily', '[{"type":"sma","params":{"period":20}}]');
    expect(key).toEqual(['bt-indicators', 'compute', '7203', 'daily', '[{"type":"sma","params":{"period":20}}]']);
  });
});

// ===== useBtIndicators Hook Tests =====

describe('useBtIndicators', () => {
  const baseSettings = {
    timeframe: '1D' as const,
    displayTimeframe: 'daily' as const,
    indicators: {
      sma: { enabled: true, period: 20 },
      ema: { enabled: false, period: 12 },
      macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
      ppo: { enabled: false, fast: 12, slow: 26, signal: 9 },
      atrSupport: { enabled: false, period: 20, multiplier: 2.0 },
      nBarSupport: { enabled: false, period: 20 },
      bollinger: { enabled: false, period: 20, deviation: 2.0 },
    },
    volumeComparison: { shortPeriod: 20, longPeriod: 100, lowerMultiplier: 1.0, higherMultiplier: 1.5 },
    tradingValueMA: { period: 20 },
    riskAdjustedReturn: {
      lookbackPeriod: 60,
      ratioType: 'sortino' as const,
      threshold: 1.0,
      condition: 'above' as const,
    },
    chartType: 'candlestick' as const,
    showVolume: true,
    showPPOChart: false,
    showVolumeComparison: false,
    showTradingValueMA: false,
    showRiskAdjustedReturnChart: false,
    showFundamentalsPanel: true,
    showFundamentalsHistoryPanel: true,
    showMarginPressurePanel: true,
    showFactorRegressionPanel: true,
    fundamentalsPanelOrder: [
      'fundamentals',
      'fundamentalsHistory',
      'marginPressure',
      'factorRegression',
    ] as ChartSettings['fundamentalsPanelOrder'],
    fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
    fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
    fundamentalsHistoryMetricOrder: [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER],
    fundamentalsHistoryMetricVisibility: { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY },
    visibleBars: 200,
    relativeMode: false,
    signalOverlay: { enabled: false, signals: [] },
  };

  it('should not call API when stockCode is null', () => {
    const { wrapper } = createTestWrapper();
    renderHook(() => useBtIndicators(null, 'daily', baseSettings), { wrapper });
    expect(mockApiPost).not.toHaveBeenCalled();
  });

  it('should call API with benchmark_code when relativeMode is true', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: { sma_20: [{ date: '2024-01-01', value: 100 }] },
    });
    const { wrapper } = createTestWrapper();
    const relativeModeSettings = { ...baseSettings, relativeMode: true };
    renderHook(() => useBtIndicators('7203', 'daily', relativeModeSettings), { wrapper });
    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledWith(
        '/api/indicators/compute',
        expect.objectContaining({
          benchmark_code: 'topix',
          relative_options: { align_dates: true, handle_zero_division: 'skip' },
        })
      );
    });
  });

  it('should call API with correct request body', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [{ date: '2024-01-01', value: 100 }],
      },
    });

    const { wrapper } = createTestWrapper();
    renderHook(() => useBtIndicators('7203', 'daily', baseSettings), { wrapper });

    await waitFor(() => {
      expect(mockApiPost).toHaveBeenCalledWith('/api/indicators/compute', {
        stock_code: '7203',
        source: 'market',
        timeframe: 'daily',
        indicators: [{ type: 'sma', params: { period: 20 } }],
      });
    });
  });

  it('should return transformed data on success', async () => {
    mockApiPost.mockResolvedValueOnce({
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [{ date: '2024-01-01', value: 100 }],
      },
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtIndicators('7203', 'daily', baseSettings), { wrapper });

    await waitFor(() => {
      const smaData = result.current.data.indicators.sma as Array<{ time: string; value: number }>;
      expect(smaData).toHaveLength(1);
      expect(smaData[0]).toEqual({ time: '2024-01-01', value: 100 });
    });
  });

  it('should return empty indicators when no specs', () => {
    const noIndicatorSettings = {
      ...baseSettings,
      indicators: {
        sma: { enabled: false, period: 20 },
        ema: { enabled: false, period: 12 },
        macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
        ppo: { enabled: false, fast: 12, slow: 26, signal: 9 },
        atrSupport: { enabled: false, period: 20, multiplier: 2.0 },
        nBarSupport: { enabled: false, period: 20 },
        bollinger: { enabled: false, period: 20, deviation: 2.0 },
      },
    };
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useBtIndicators('7203', 'daily', noIndicatorSettings), { wrapper });
    expect(result.current.data.indicators).toEqual({});
  });

  it('does not refetch when only threshold/condition changes', async () => {
    mockApiPost.mockClear();
    mockApiPost.mockResolvedValue({
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [{ date: '2024-01-01', value: 100 }],
        risk_adjusted_return_60_sortino: [{ date: '2024-01-01', value: 1.1 }],
      },
    });

    const settings: ChartSettings = {
      ...baseSettings,
      showRiskAdjustedReturnChart: true,
      riskAdjustedReturn: {
        lookbackPeriod: 60,
        ratioType: 'sortino' as const,
        threshold: 1.0,
        condition: 'above' as const,
      },
    };

    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(({ chartSettings }) => useBtIndicators('7203', 'daily', chartSettings), {
      wrapper,
      initialProps: { chartSettings: settings },
    });

    await waitFor(() => expect(mockApiPost).toHaveBeenCalledTimes(1));

    rerender({
      chartSettings: {
        ...settings,
        riskAdjustedReturn: {
          ...settings.riskAdjustedReturn,
          threshold: 2.0,
          condition: 'below',
        },
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(mockApiPost).toHaveBeenCalledTimes(1);
  });

  it('refetches when lookback/ratio changes', async () => {
    mockApiPost.mockClear();
    mockApiPost.mockResolvedValue({
      stock_code: '7203',
      timeframe: 'daily',
      meta: { bars: 100 },
      indicators: {
        sma_20: [{ date: '2024-01-01', value: 100 }],
        risk_adjusted_return_60_sortino: [{ date: '2024-01-01', value: 1.1 }],
      },
    });

    const settings: ChartSettings = {
      ...baseSettings,
      showRiskAdjustedReturnChart: true,
      riskAdjustedReturn: {
        lookbackPeriod: 60,
        ratioType: 'sortino' as const,
        threshold: 1.0,
        condition: 'above' as const,
      },
    };

    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(({ chartSettings }) => useBtIndicators('7203', 'daily', chartSettings), {
      wrapper,
      initialProps: { chartSettings: settings },
    });

    await waitFor(() => expect(mockApiPost).toHaveBeenCalledTimes(1));

    rerender({
      chartSettings: {
        ...settings,
        riskAdjustedReturn: {
          ...settings.riskAdjustedReturn,
          lookbackPeriod: 80,
          ratioType: 'sharpe',
        },
      },
    });

    await waitFor(() => expect(mockApiPost).toHaveBeenCalledTimes(2));
  });
});
