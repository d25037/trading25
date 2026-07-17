import { act, render, screen } from '@testing-library/react';
import { createChart, createSeriesMarkers } from 'lightweight-charts';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
} from '@/constants/fundamentalMetrics';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
} from '@/constants/fundamentalsHistoryMetrics';
import { CHART_COLORS } from '@/lib/constants';
import type { StockDataPoint } from '@/types/chart';
import { calculateChangePct, StockChart, timeToDateString } from './StockChart';

// Mock the chart store
const mockChartStore = {
  selectedSymbol: null as string | null,
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
      smaAtrBands: { enabled: false, smaPeriod: 5, atrPeriod: 20, multiplier: 1.0 },
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
    accumulationFlow: {
      cmfPeriod: 20,
      chaikinFastPeriod: 3,
      chaikinSlowPeriod: 10,
      obvLookbackPeriod: 20,
    },
    riskAdjustedReturn: {
      lookbackPeriod: 60,
      ratioType: 'sortino' as const,
      threshold: 1.0,
      condition: 'above' as const,
    },
    showCMF: false,
    showChaikinOscillator: false,
    showOBVFlowScore: false,
    showRiskAdjustedReturnChart: false,
    signalOverlay: {
      enabled: false,
      signals: [],
    },
  },
  setSelectedSymbol: vi.fn(),
  updateSettings: vi.fn(),
  toggleIndicator: vi.fn(),
  toggleRelativeMode: vi.fn(),
  setDisplayTimeframe: vi.fn(),
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => mockChartStore,
}));

// Capture crosshairMove callback for OHLCOverlay tests
let crosshairCallback: ((param: unknown) => void) | null = null;
let mockCandlestickSeries: Record<string, unknown>;
let mockAddSeries: ReturnType<typeof vi.fn>;
let mockRemoveSeries: ReturnType<typeof vi.fn>;
let mockApplyOptions: ReturnType<typeof vi.fn>;
let mockSeriesInstances: Array<{ setData: ReturnType<typeof vi.fn>; priceScale: ReturnType<typeof vi.fn> }>;

vi.mock('lightweight-charts', () => ({
  createChart: vi.fn(() => {
    mockSeriesInstances = [];
    mockAddSeries = vi.fn(() => {
      const series = {
        setData: vi.fn(),
        priceScale: vi.fn(() => ({
          applyOptions: vi.fn(),
        })),
      };
      mockSeriesInstances.push(series);
      if (mockSeriesInstances.length === 1) {
        mockCandlestickSeries = series;
      }
      return series;
    });
    mockRemoveSeries = vi.fn();
    mockApplyOptions = vi.fn();
    return {
      addSeries: mockAddSeries,
      timeScale: vi.fn(() => ({
        setVisibleRange: vi.fn(),
        setVisibleLogicalRange: vi.fn(),
      })),
      applyOptions: mockApplyOptions,
      remove: vi.fn(),
      removeSeries: mockRemoveSeries,
      subscribeCrosshairMove: vi.fn((cb: (param: unknown) => void) => {
        crosshairCallback = cb;
      }),
    };
  }),
  createSeriesMarkers: vi.fn(() => ({
    setMarkers: vi.fn(),
  })),
  CandlestickSeries: 'CandlestickSeries',
  HistogramSeries: 'HistogramSeries',
  LineSeries: 'LineSeries',
}));

const mockStockData: StockDataPoint[] = [
  {
    time: '2024-01-01',
    open: 100,
    high: 110,
    low: 95,
    close: 105,
    volume: 1000,
  },
  {
    time: '2024-01-02',
    open: 105,
    high: 115,
    low: 100,
    close: 110,
    volume: 1200,
  },
];

function stubResizeObserver(): { trigger: () => void } {
  let resizeCallback: (() => void) | null = null;

  class ResizeObserverMock {
    observe = vi.fn();
    disconnect = vi.fn();

    constructor(callback: () => void) {
      resizeCallback = callback;
    }
  }

  vi.stubGlobal('ResizeObserver', ResizeObserverMock);
  return {
    trigger: () => resizeCallback?.(),
  };
}

describe('StockChart', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockChartStore.settings.showVolume = true;
    mockChartStore.settings.indicators.sma.enabled = false;
    mockChartStore.settings.indicators.ema.enabled = false;
    mockChartStore.settings.indicators.vwema.enabled = false;
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders chart container', () => {
    render(<StockChart data={mockStockData} />);

    // Chart container should be present
    const chartContainer = document.querySelector('div');
    expect(chartContainer).toBeInTheDocument();
  });

  it('shows no data message when data array is empty', () => {
    render(<StockChart data={[]} />);

    expect(screen.getByText('No chart data available')).toBeInTheDocument();
  });

  it('renders with default empty data when no props provided', () => {
    render(<StockChart />);

    expect(screen.getByText('No chart data available')).toBeInTheDocument();
  });

  it('creates chart when component mounts with data', () => {
    const { container } = render(<StockChart data={mockStockData} />);

    // Verify chart container is created and doesn't show "No chart data" message
    expect(screen.queryByText('No chart data available')).not.toBeInTheDocument();
    expect(container.querySelector('div')).toBeInTheDocument();
  });

  it('styles and labels only the active provisional daily bar', () => {
    const { rerender } = render(<StockChart data={mockStockData} provisionalDate="2024-01-02" />);

    expect(screen.getByText('四季報 15分遅延・当日暫定')).toHaveAttribute(
      'title',
      '2024-01-02 の日足は四季報の当日暫定値です'
    );
    expect(mockCandlestickSeries.setData).toHaveBeenLastCalledWith([
      { time: '2024-01-01', open: 100, high: 110, low: 95, close: 105 },
      {
        time: '2024-01-02',
        open: 105,
        high: 115,
        low: 100,
        close: 110,
        color: '#f59e0b',
        borderColor: '#d97706',
        wickColor: '#f59e0b',
      },
    ]);

    rerender(<StockChart data={mockStockData} provisionalDate={null} />);
    expect(screen.queryByText('四季報 15分遅延・当日暫定')).not.toBeInTheDocument();
    expect(mockCandlestickSeries.setData).toHaveBeenLastCalledWith([
      { time: '2024-01-01', open: 100, high: 110, low: 95, close: 105 },
      { time: '2024-01-02', open: 105, high: 115, low: 100, close: 110 },
    ]);
  });

  it('disables mouse wheel chart interactions so page scroll remains available', () => {
    render(<StockChart data={mockStockData} />);

    expect(vi.mocked(createChart)).toHaveBeenCalledWith(
      expect.anything(),
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
  });

  it('updates chart data when data prop changes', () => {
    const { rerender, container } = render(<StockChart data={mockStockData} />);

    const newData = [
      ...mockStockData,
      {
        time: '2024-01-03',
        open: 110,
        high: 120,
        low: 105,
        close: 115,
        volume: 1500,
      },
    ];

    rerender(<StockChart data={newData} />);

    // Chart should still be present after rerender
    expect(container.querySelector('div')).toBeInTheDocument();
    expect(screen.queryByText('No chart data available')).not.toBeInTheDocument();
  });

  it('handles volume toggle correctly', () => {
    const { rerender, container } = render(<StockChart data={mockStockData} />);

    // Mock store with volume disabled
    mockChartStore.settings.showVolume = false;

    rerender(<StockChart data={mockStockData} />);

    // Chart should still be present after volume toggle
    expect(container.querySelector('div')).toBeInTheDocument();
  });

  it('cleans up chart when component unmounts', () => {
    const { unmount, container } = render(<StockChart data={mockStockData} />);

    // Component should be in the document before unmount
    expect(container.querySelector('div')).toBeInTheDocument();

    unmount();

    // After unmount, container should be empty
    expect(container.children.length).toBe(0);
  });

  it('renders and removes VWEMA overlay when toggled', () => {
    mockChartStore.settings.indicators.vwema.enabled = true;
    const vwemaData = mockStockData.map((item, index) => ({
      time: item.time,
      value: 102 + index,
    }));

    const { rerender } = render(<StockChart data={mockStockData} vwema={vwemaData} />);

    const vwemaCallIndex = mockAddSeries.mock.calls.findIndex(
      ([seriesType, options]) =>
        seriesType === 'LineSeries' &&
        typeof options === 'object' &&
        options !== null &&
        'color' in options &&
        options.color === CHART_COLORS.VWEMA
    );
    expect(vwemaCallIndex).toBeGreaterThanOrEqual(0);
    const vwemaSeries = mockSeriesInstances[vwemaCallIndex];
    expect(vwemaSeries?.setData).toHaveBeenCalledWith(vwemaData);

    mockChartStore.settings.indicators.vwema.enabled = false;
    rerender(<StockChart data={mockStockData} vwema={vwemaData} />);

    expect(mockRemoveSeries).toHaveBeenCalledWith(vwemaSeries);
  });

  it.each([
    ['SMA', 'sma', CHART_COLORS.SMA],
    ['EMA', 'ema', CHART_COLORS.EMA],
  ] as const)('renders and removes %s overlay when toggled', (_label, indicatorKey, color) => {
    mockChartStore.settings.indicators[indicatorKey].enabled = true;
    const maData = mockStockData.map((item, index) => ({
      time: item.time,
      value: 100 + index,
    }));

    const props = indicatorKey === 'sma' ? { sma: maData } : { ema: maData };
    const { rerender } = render(<StockChart data={mockStockData} {...props} />);

    const maCallIndex = mockAddSeries.mock.calls.findIndex(
      ([seriesType, options]) =>
        seriesType === 'LineSeries' &&
        typeof options === 'object' &&
        options !== null &&
        'color' in options &&
        options.color === color
    );
    expect(maCallIndex).toBeGreaterThanOrEqual(0);
    const maSeries = mockSeriesInstances[maCallIndex];
    expect(maSeries?.setData).toHaveBeenCalledWith(maData);

    mockChartStore.settings.indicators[indicatorKey].enabled = false;
    rerender(<StockChart data={mockStockData} {...props} />);

    expect(mockRemoveSeries).toHaveBeenCalledWith(maSeries);
  });

  it('renders and removes N-bar support overlay when toggled', () => {
    mockChartStore.settings.indicators.nBarSupport.enabled = true;
    const nBarData = mockStockData.map((item, index) => ({
      time: item.time,
      value: 95 + index,
    }));

    const { rerender } = render(<StockChart data={mockStockData} nBarSupport={nBarData} />);

    const nBarCallIndex = mockAddSeries.mock.calls.findIndex(
      ([seriesType, options]) =>
        seriesType === 'LineSeries' &&
        typeof options === 'object' &&
        options !== null &&
        'color' in options &&
        options.color === CHART_COLORS.N_BAR_SUPPORT
    );
    expect(nBarCallIndex).toBeGreaterThanOrEqual(0);
    const nBarSeries = mockSeriesInstances[nBarCallIndex];
    expect(nBarSeries?.setData).toHaveBeenCalledWith(nBarData);

    mockChartStore.settings.indicators.nBarSupport.enabled = false;
    rerender(<StockChart data={mockStockData} nBarSupport={nBarData} />);

    expect(mockRemoveSeries).toHaveBeenCalledWith(nBarSeries);
  });

  it('renders and removes ATR support overlay when toggled', () => {
    mockChartStore.settings.indicators.atrSupport.enabled = true;
    const atrData = mockStockData.map((item, index) => ({
      time: item.time,
      value: 90 + index,
    }));

    const { rerender } = render(<StockChart data={mockStockData} atrSupport={atrData} />);

    const atrCallIndex = mockAddSeries.mock.calls.findIndex(
      ([seriesType, options]) =>
        seriesType === 'LineSeries' &&
        typeof options === 'object' &&
        options !== null &&
        'color' in options &&
        options.color === CHART_COLORS.ATR_SUPPORT
    );
    expect(atrCallIndex).toBeGreaterThanOrEqual(0);
    const atrSeries = mockSeriesInstances[atrCallIndex];
    expect(atrSeries?.setData).toHaveBeenCalledWith(atrData);

    mockChartStore.settings.indicators.atrSupport.enabled = false;
    rerender(<StockChart data={mockStockData} atrSupport={atrData} />);

    expect(mockRemoveSeries).toHaveBeenCalledWith(atrSeries);
  });

  it('renders and removes SMA ATR position bands when toggled', () => {
    mockChartStore.settings.indicators.smaAtrBands.enabled = true;
    const bandData = [
      { time: '2024-01-01', upper: 110, middle: 100, lower: 90, deviation: 0.5 },
      { time: '2024-01-02', upper: 111, middle: 101, lower: 91, deviation: -0.5 },
    ];

    const { rerender } = render(<StockChart data={mockStockData} smaAtrBands={bandData} />);

    for (const [color, field] of [
      [CHART_COLORS.SMA_ATR_UPPER, 'upper'],
      [CHART_COLORS.SMA_ATR_MIDDLE, 'middle'],
      [CHART_COLORS.SMA_ATR_LOWER, 'lower'],
    ] as const) {
      const callIndex = mockAddSeries.mock.calls.findIndex(
        ([seriesType, options]) => seriesType === 'LineSeries' && options?.color === color
      );
      expect(callIndex).toBeGreaterThanOrEqual(0);
      expect(mockSeriesInstances[callIndex]?.setData).toHaveBeenCalledWith(
        bandData.map((item) => ({ time: item.time, value: item[field] }))
      );
    }

    mockChartStore.settings.indicators.smaAtrBands.enabled = false;
    rerender(<StockChart data={mockStockData} smaAtrBands={bandData} />);
    expect(mockRemoveSeries).toHaveBeenCalledTimes(3);
  });

  it('uses orange for SMA, green above, and red below', () => {
    expect(CHART_COLORS.SMA_ATR_MIDDLE).toBe('#F59E0B');
    expect(CHART_COLORS.SMA_ATR_UPPER).toBe('#26A69A');
    expect(CHART_COLORS.SMA_ATR_LOWER).toBe('#EF4444');
  });

  it('renders and removes Bollinger overlay when toggled', () => {
    mockChartStore.settings.indicators.bollinger.enabled = true;
    const bollingerData = mockStockData.map((item, index) => ({
      time: item.time,
      upper: 110 + index,
      middle: 100 + index,
      lower: 90 + index,
    }));

    const { rerender } = render(<StockChart data={mockStockData} bollingerBands={bollingerData} />);

    const bollingerCallIndex = mockAddSeries.mock.calls.findIndex(
      ([seriesType, options]) =>
        seriesType === 'LineSeries' &&
        typeof options === 'object' &&
        options !== null &&
        'color' in options &&
        options.color === CHART_COLORS.BOLLINGER
    );
    expect(bollingerCallIndex).toBeGreaterThanOrEqual(0);
    const bollingerSeries = mockSeriesInstances[bollingerCallIndex];
    expect(bollingerSeries?.setData).toHaveBeenCalledWith([
      { time: '2024-01-01', value: 110 },
      { time: '2024-01-02', value: 111 },
    ]);

    mockChartStore.settings.indicators.bollinger.enabled = false;
    rerender(<StockChart data={mockStockData} bollingerBands={bollingerData} />);

    expect(mockRemoveSeries).toHaveBeenCalledWith(bollingerSeries);
  });

  it('updates signal markers after the initial marker plugin is created', () => {
    const initialMarkers = [
      {
        time: '2024-01-01',
        position: 'belowBar' as const,
        color: '#00ff00',
        shape: 'arrowUp' as const,
        text: 'BUY',
        size: 1,
      },
    ];
    const nextMarkers = [
      {
        time: '2024-01-02',
        position: 'aboveBar' as const,
        color: '#ff0000',
        shape: 'arrowDown' as const,
        text: 'SELL',
        size: 1,
      },
    ];

    const { rerender } = render(<StockChart data={mockStockData} signalMarkers={initialMarkers} />);
    rerender(<StockChart data={mockStockData} signalMarkers={nextMarkers} />);

    const markersApi = vi.mocked(createSeriesMarkers).mock.results[0]?.value;
    expect(markersApi?.setMarkers).toHaveBeenCalledWith([
      {
        time: '2024-01-02',
        position: 'aboveBar',
        color: '#ff0000',
        shape: 'arrowDown',
        text: 'SELL',
        size: 1,
      },
    ]);
  });

  it('gracefully skips data updates when candlestick series creation fails', () => {
    mockChartStore.settings.showVolume = false;
    vi.mocked(createChart).mockImplementationOnce(
      () =>
        ({
          addSeries: vi.fn(() => null),
          timeScale: vi.fn(() => ({
            setVisibleRange: vi.fn(),
            setVisibleLogicalRange: vi.fn(),
          })),
          applyOptions: vi.fn(),
          remove: vi.fn(),
          removeSeries: vi.fn(),
          subscribeCrosshairMove: vi.fn(),
        }) as never
    );

    expect(() => render(<StockChart data={mockStockData} />)).not.toThrow();
  });

  it('resizes chart when observer reports valid dimensions', () => {
    const resizeObserver = stubResizeObserver();
    const { container } = render(<StockChart data={mockStockData} />);
    const chartContainer = container.querySelector('.absolute.inset-0') as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 900 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 420 });

    act(() => {
      resizeObserver.trigger();
    });

    expect(mockApplyOptions).toHaveBeenCalledWith({ width: 900, height: 420 });
  });

  it('skips resize when observer reports too-small dimensions', () => {
    const resizeObserver = stubResizeObserver();
    const { container } = render(<StockChart data={mockStockData} />);
    const chartContainer = container.querySelector('.absolute.inset-0') as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', { configurable: true, value: 0 });
    Object.defineProperty(chartContainer, 'clientHeight', { configurable: true, value: 120 });

    act(() => {
      resizeObserver.trigger();
    });

    expect(mockApplyOptions).not.toHaveBeenCalled();
  });
});

describe('calculateChangePct', () => {
  const testData: StockDataPoint[] = [
    { time: '2024-01-01', open: 100, high: 110, low: 95, close: 100, volume: 1000 },
    { time: '2024-01-02', open: 100, high: 115, low: 98, close: 110, volume: 1200 },
    { time: '2024-01-03', open: 110, high: 120, low: 105, close: 105, volume: 1100 },
  ];

  it('returns undefined for first data point (no previous day)', () => {
    expect(calculateChangePct('2024-01-01', 100, testData)).toBeUndefined();
  });

  it('returns undefined when time is not found', () => {
    expect(calculateChangePct('2024-01-99', 100, testData)).toBeUndefined();
  });

  it('calculates positive change correctly', () => {
    const result = calculateChangePct('2024-01-02', 110, testData);
    expect(result).toBeCloseTo(10, 2); // (110 - 100) / 100 * 100 = 10%
  });

  it('calculates negative change correctly', () => {
    const result = calculateChangePct('2024-01-03', 105, testData);
    expect(result).toBeCloseTo(-4.545, 2); // (105 - 110) / 110 * 100 = -4.545%
  });

  it('returns undefined for empty data array', () => {
    expect(calculateChangePct('2024-01-01', 100, [])).toBeUndefined();
  });
});

describe('timeToDateString', () => {
  it('returns string time as-is', () => {
    expect(timeToDateString('2024-01-15')).toBe('2024-01-15');
  });

  it('converts UTC timestamp (seconds) to date string', () => {
    // 1704067200 = 2024-01-01T00:00:00Z
    expect(timeToDateString(1704067200)).toBe('2024-01-01');
  });

  it('converts BusinessDay object to date string', () => {
    const businessDay = { year: 2024, month: 3, day: 5 };
    expect(timeToDateString(businessDay)).toBe('2024-03-05');
  });

  it('pads single-digit month and day', () => {
    const businessDay = { year: 2024, month: 1, day: 9 };
    expect(timeToDateString(businessDay)).toBe('2024-01-09');
  });
});

describe('OHLCOverlay (via crosshair)', () => {
  const twoPointData: StockDataPoint[] = [
    { time: '2024-01-01', open: 100, high: 110, low: 95, close: 100, volume: 1000 },
    { time: '2024-01-02', open: 100, high: 115, low: 98, close: 110, volume: 1200 },
  ];

  function buildSeriesData(ohlc: { open: number; high: number; low: number; close: number }) {
    const seriesData = new Map();
    seriesData.set(mockCandlestickSeries, ohlc);
    return seriesData;
  }

  function simulateCrosshair(time: string, ohlc: { open: number; high: number; low: number; close: number }) {
    act(() => {
      crosshairCallback?.({ time, seriesData: buildSeriesData(ohlc) });
    });
  }

  const bullishOHLC = { open: 100, high: 115, low: 98, close: 110 };
  const bearishOHLC = { open: 110, high: 115, low: 95, close: 100 };

  beforeEach(() => {
    vi.clearAllMocks();
    crosshairCallback = null;
    mockChartStore.settings.showVolume = true;
  });

  it('renders OHLC overlay with correct inline colors for positive close', () => {
    render(<StockChart data={twoPointData} />);
    expect(crosshairCallback).not.toBeNull();

    simulateCrosshair('2024-01-02', bullishOHLC);

    for (const label of ['O', 'H', 'L', 'C']) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }

    const openLabel = screen.getByText('O');
    expect(openLabel.closest('span[style]')).toHaveStyle({ color: '#26a69a' });
  });

  it('renders OHLC overlay with correct inline colors for negative close', () => {
    render(<StockChart data={twoPointData} />);

    simulateCrosshair('2024-01-02', bearishOHLC);

    const openLabel = screen.getByText('O');
    expect(openLabel.closest('span[style]')).toHaveStyle({ color: '#ef5350' });
  });

  it('shows change percentage with correct color', () => {
    render(<StockChart data={twoPointData} />);

    simulateCrosshair('2024-01-02', bullishOHLC);

    // changePct = (110 - 100) / 100 * 100 = 10%
    expect(screen.getByText(/\+10\.00%/)).toBeInTheDocument();
  });

  it('clears overlay when crosshair moves away', () => {
    render(<StockChart data={twoPointData} />);

    simulateCrosshair('2024-01-02', bullishOHLC);
    expect(screen.getByText('O')).toBeInTheDocument();

    act(() => {
      crosshairCallback?.({ time: undefined, seriesData: new Map() });
    });
    expect(screen.queryByText('O')).not.toBeInTheDocument();
  });
});
