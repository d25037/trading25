import { act, render, screen } from '@testing-library/react';
import { createChart } from 'lightweight-charts';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
} from '@/constants/fundamentalMetrics';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
} from '@/constants/fundamentalsHistoryMetrics';
import { MarginPressureChart } from './MarginPressureChart';
import { PPOChart } from './PPOChart';
import { RiskAdjustedReturnChart } from './RiskAdjustedReturnChart';
import { TradingValueMAChart } from './TradingValueMAChart';
import { VolumeComparisonChart } from './VolumeComparisonChart';

const mockChartStore = {
  settings: {
    timeframe: '1D' as const,
    displayTimeframe: 'daily' as const,
    chartType: 'candlestick' as const,
    showVolume: true,
    showPPOChart: false,
    showVolumeComparison: false,
    showTradingValueMA: false,
    showFundamentalsPanel: true,
    showFundamentalsHistoryPanel: true,
    showMarginPressurePanel: true,
    showFactorRegressionPanel: true,
    fundamentalsPanelOrder: ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'],
    fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
    fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
    fundamentalsHistoryMetricOrder: [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER],
    fundamentalsHistoryMetricVisibility: { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY },
    visibleBars: 30,
    relativeMode: false,
    indicators: {
      sma: { enabled: false, period: 20 },
      ema: { enabled: false, period: 12 },
      vwema: { enabled: false, period: 20 },
      macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
      ppo: { enabled: false, fast: 12, slow: 26, signal: 9 },
      atrSupport: { enabled: false, period: 20, multiplier: 3.0 },
      nBarSupport: { enabled: false, period: 60 },
      bollinger: { enabled: false, period: 20, deviation: 2.0 },
    },
    volumeComparison: {
      shortPeriod: 20,
      longPeriod: 100,
      lowerMultiplier: 1.0,
      higherMultiplier: 1.5,
    },
    tradingValueMA: {
      period: 15,
    },
    riskAdjustedReturn: {
      lookbackPeriod: 60,
      ratioType: 'sortino' as const,
      threshold: 1.0,
      condition: 'above' as const,
    },
    showRiskAdjustedReturnChart: false,
    signalOverlay: {
      enabled: false,
      signals: [],
    },
  },
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

let createChartOptions: unknown[] = [];
let chartInstances: Array<{
  addSeries: ReturnType<typeof vi.fn>;
  timeScale: ReturnType<typeof vi.fn>;
  applyOptions: ReturnType<typeof vi.fn>;
  remove: ReturnType<typeof vi.fn>;
  removeSeries: ReturnType<typeof vi.fn>;
}> = [];
let resizeObserverCallbacks: Array<() => void> = [];

vi.mock('lightweight-charts', () => ({
  createChart: vi.fn((_container: unknown, options: unknown) => {
    createChartOptions.push(options);
    const chart = {
      addSeries: vi.fn(() => ({
        setData: vi.fn(),
        applyOptions: vi.fn(),
        priceScale: vi.fn(() => ({
          applyOptions: vi.fn(),
        })),
      })),
      timeScale: vi.fn(() => ({
        setVisibleRange: vi.fn(),
        setVisibleLogicalRange: vi.fn(),
      })),
      applyOptions: vi.fn(),
      remove: vi.fn(),
      removeSeries: vi.fn(),
    };
    chartInstances.push(chart);
    return chart;
  }),
  HistogramSeries: 'HistogramSeries',
  LineSeries: 'LineSeries',
}));

function expectLatestChartToUsePageScrollOptions(): void {
  expect(vi.mocked(createChart)).toHaveBeenCalled();
  expect(createChartOptions.at(-1)).toEqual(
    expect.objectContaining({
      handleScroll: expect.objectContaining({
        mouseWheel: false,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      }),
      handleScale: expect.objectContaining({
        mouseWheel: false,
        pinch: true,
      }),
    })
  );
}

function getLastChartInstance() {
  const chart = chartInstances.at(-1);
  expect(chart).toBeDefined();
  return chart!;
}

function triggerLastResizeObserver(): void {
  const callback = resizeObserverCallbacks.at(-1);
  expect(callback).toBeDefined();
  act(() => {
    callback?.();
  });
}

describe('page scroll chart interaction options', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    createChartOptions = [];
    chartInstances = [];
    resizeObserverCallbacks = [];

    class ResizeObserverMock {
      observe = vi.fn();
      disconnect = vi.fn();

      constructor(callback: () => void) {
        resizeObserverCallbacks.push(callback);
      }
    }

    vi.stubGlobal('ResizeObserver', ResizeObserverMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('applies page-scroll interaction options to PPOChart', () => {
    render(
      <PPOChart
        data={[
          {
            time: '2024-01-01',
            ppo: 1,
            signal: 0.5,
            histogram: 0.5,
          },
        ]}
      />
    );

    expectLatestChartToUsePageScrollOptions();
  });

  it('keeps PPOChart mounted when data is empty and resizes on window resize', () => {
    const { container } = render(<PPOChart data={[]} title="PPO" />);

    expect(screen.getByText('PPO')).toBeInTheDocument();

    const chartContainer = container.querySelector('.flex-1.h-full') as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 400 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 180 });

    act(() => {
      window.dispatchEvent(new Event('resize'));
    });

    expect(getLastChartInstance().applyOptions).toHaveBeenCalledWith({ width: 400, height: 180 });
  });

  it('ignores NaN PPO values without breaking chart updates', () => {
    render(
      <PPOChart
        data={[
          {
            time: '2024-01-01',
            ppo: Number.NaN,
            signal: Number.NaN,
            histogram: Number.NaN,
          },
        ]}
      />
    );

    expectLatestChartToUsePageScrollOptions();
  });

  it('applies page-scroll interaction options to VolumeComparisonChart', () => {
    render(
      <VolumeComparisonChart
        data={[
          {
            time: '2024-01-01',
            shortMA: 100,
            longThresholdLower: 80,
            longThresholdHigher: 120,
          },
        ]}
      />
    );

    expectLatestChartToUsePageScrollOptions();
  });

  it('handles empty VolumeComparisonChart data and only resizes for valid dimensions', () => {
    const { container } = render(<VolumeComparisonChart data={[]} />);
    const chartContainer = container.querySelector('.flex-1.h-full') as HTMLDivElement;

    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 0 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 80 });
    triggerLastResizeObserver();
    expect(getLastChartInstance().applyOptions).not.toHaveBeenCalled();

    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 320 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 160 });
    triggerLastResizeObserver();
    expect(getLastChartInstance().applyOptions).toHaveBeenCalledWith({ width: 320, height: 160 });
  });

  it('applies page-scroll interaction options to TradingValueMAChart', () => {
    render(
      <TradingValueMAChart
        title="TVMA"
        data={[
          {
            time: '2024-01-01',
            value: 1000000,
          },
        ]}
      />
    );

    expectLatestChartToUsePageScrollOptions();
  });

  it('handles empty TradingValueMAChart data and only resizes for valid dimensions', () => {
    const { container } = render(<TradingValueMAChart data={[]} />);
    const chartContainer = container.querySelector('.flex-1.h-full') as HTMLDivElement;

    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 0 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 90 });
    triggerLastResizeObserver();
    expect(getLastChartInstance().applyOptions).not.toHaveBeenCalled();

    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 480 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 140 });
    triggerLastResizeObserver();
    expect(getLastChartInstance().applyOptions).toHaveBeenCalledWith({ width: 480, height: 140 });
  });

  it('does not resize TradingValueMAChart after unmount', () => {
    const { unmount } = render(<TradingValueMAChart data={[{ time: '2024-01-01', value: 10 }]} />);
    const chart = getLastChartInstance();

    unmount();
    triggerLastResizeObserver();

    expect(chart.applyOptions).not.toHaveBeenCalled();
  });

  it('applies page-scroll interaction options to RiskAdjustedReturnChart', () => {
    render(
      <RiskAdjustedReturnChart
        title="RAR"
        data={[
          {
            time: '2024-01-01',
            value: 1.25,
          },
        ]}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    expectLatestChartToUsePageScrollOptions();
    expect(screen.getByText('RAR')).toBeInTheDocument();
  });

  it('renders RiskAdjustedReturnChart without a latest value pill when data is empty', () => {
    render(
      <RiskAdjustedReturnChart
        data={[]}
        lookbackPeriod={60}
        ratioType="sortino"
        threshold={1}
        condition="above"
      />
    );

    expect(screen.queryByText('0.00')).not.toBeInTheDocument();
    expect(screen.getByText('Risk Adjusted Return')).toBeInTheDocument();
  });

  it('applies page-scroll interaction options to MarginPressureChart', () => {
    render(
      <MarginPressureChart
        type="longPressure"
        longPressureData={[
          {
            date: '2024-01-01',
            pressure: 1.2,
            longVol: 100,
            shortVol: 20,
            avgVolume: 50,
          },
        ]}
      />
    );

    expectLatestChartToUsePageScrollOptions();
  });

  it('renders empty-state MarginPressureChart when long pressure data is missing', () => {
    render(<MarginPressureChart type="longPressure" />);

    expect(screen.getByText('No data available')).toBeInTheDocument();
  });

  it('covers non-zero-line MarginPressureChart types and window resize handling', () => {
    const { rerender, container } = render(
      <MarginPressureChart
        type="flowPressure"
        flowPressureData={[
          {
            date: '2024-01-01',
            flowPressure: 0.5,
            currentNetMargin: 40,
            previousNetMargin: 35,
            avgVolume: 50,
          },
        ]}
      />
    );

    expect(screen.getByText('信用フロー圧力')).toBeInTheDocument();

    rerender(
      <MarginPressureChart
        type="turnoverDays"
        turnoverDaysData={[
          {
            date: '2024-01-01',
            turnoverDays: 2.5,
            longVol: 100,
            avgVolume: 40,
          },
        ]}
      />
    );

    expect(screen.getByText('信用回転日数')).toBeInTheDocument();

    const chartContainer = container.querySelectorAll('.h-full.w-full')[1] as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 360 });

    act(() => {
      window.dispatchEvent(new Event('resize'));
    });

    expect(getLastChartInstance().applyOptions).toHaveBeenCalledWith({ width: 360 });
  });
});
