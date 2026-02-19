import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  countVisibleFundamentalMetrics,
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
  resolveFundamentalsPanelHeightPx,
} from '@/constants/fundamentalMetrics';
import { ChartsPage } from './ChartsPage';

const mockUseMultiTimeframeChart = vi.fn();
const mockUseBtMarginIndicators = vi.fn();
const mockUseStockData = vi.fn();
const mockUseFundamentals = vi.fn();
const mockWindowOpen = vi.fn();
const mockFundamentalsPanelProps = vi.fn<[unknown], void>();

vi.mock('@/components/Chart/hooks/useMultiTimeframeChart', () => ({
  useMultiTimeframeChart: () => mockUseMultiTimeframeChart(),
}));

vi.mock('@/hooks/useBtMarginIndicators', () => ({
  useBtMarginIndicators: (...args: unknown[]) => mockUseBtMarginIndicators(...args),
}));

vi.mock('@/hooks/useStockData', () => ({
  useStockData: (...args: unknown[]) => mockUseStockData(...args),
}));

vi.mock('@/hooks/useFundamentals', () => ({
  useFundamentals: (...args: unknown[]) => mockUseFundamentals(...args),
}));

const mockSettings = {
  timeframe: '1D' as const,
  displayTimeframe: 'daily' as const,
  indicators: {
    sma: { enabled: false, period: 20 },
    ema: { enabled: false, period: 12 },
    macd: { enabled: false, fast: 12, slow: 26, signal: 9 },
    ppo: { enabled: true, fast: 12, slow: 26, signal: 9 },
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
  chartType: 'candlestick' as const,
  showVolume: true,
  showPPOChart: true,
  showVolumeComparison: true,
  showTradingValueMA: true,
  showRiskAdjustedReturnChart: true,
  showFundamentalsPanel: true,
  showFundamentalsHistoryPanel: true,
  showMarginPressurePanel: true,
  showFactorRegressionPanel: true,
  fundamentalsPanelOrder: ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'],
  fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
  fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
  visibleBars: 120,
  relativeMode: true,
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => ({ settings: mockSettings }),
}));

vi.mock('@/components/Chart/ChartControls', () => ({
  ChartControls: () => <div>Chart Controls</div>,
}));

vi.mock('@/components/Chart/StockChart', () => ({
  StockChart: () => <div>Stock Chart</div>,
}));

vi.mock('@/components/Chart/PPOChart', () => ({
  PPOChart: () => <div>PPO Chart</div>,
}));

vi.mock('@/components/Chart/RiskAdjustedReturnChart', () => ({
  RiskAdjustedReturnChart: () => <div>Risk Adjusted Return Chart</div>,
}));

vi.mock('@/components/Chart/VolumeComparisonChart', () => ({
  VolumeComparisonChart: () => <div>Volume Comparison</div>,
}));

vi.mock('@/components/Chart/TradingValueMAChart', () => ({
  TradingValueMAChart: () => <div>Trading Value MA</div>,
}));

vi.mock('@/components/Chart/MarginPressureChart', () => ({
  MarginPressureChart: () => <div>Margin Pressure Chart</div>,
}));

vi.mock('@/components/Chart/FundamentalsPanel', () => ({
  FundamentalsPanel: (props: unknown) => {
    mockFundamentalsPanelProps(props);
    return <div>Fundamentals Panel</div>;
  },
}));

vi.mock('@/components/Chart/FundamentalsHistoryPanel', () => ({
  FundamentalsHistoryPanel: () => <div>FY History Panel</div>,
}));

const mockFactorRegressionPanelProps = vi.fn<[unknown], void>();
vi.mock('@/components/Chart/FactorRegressionPanel', () => ({
  FactorRegressionPanel: (props: unknown) => {
    mockFactorRegressionPanelProps(props);
    return <div>Factor Regression Panel</div>;
  },
}));

vi.mock('@/components/Chart/TimeframeSelector', () => ({
  TimeframeSelector: () => <div>Timeframe Selector</div>,
}));

vi.mock('@/components/ErrorBoundary', () => ({
  ErrorBoundary: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

describe('ChartsPage', () => {
  class MockIntersectionObserver {
    static instances: MockIntersectionObserver[] = [];

    private readonly callback: IntersectionObserverCallback;
    private readonly targets: Element[] = [];

    constructor(callback: IntersectionObserverCallback) {
      this.callback = callback;
      MockIntersectionObserver.instances.push(this);
    }

    observe = (target: Element) => {
      this.targets.push(target);
    };

    disconnect = vi.fn();
    unobserve = vi.fn();
    takeRecords = () => [];

    trigger(isIntersecting = true) {
      const entries = this.targets.map(
        (target) =>
          ({
            target,
            isIntersecting,
            intersectionRatio: isIntersecting ? 1 : 0,
            time: 0,
            boundingClientRect: {} as DOMRectReadOnly,
            intersectionRect: {} as DOMRectReadOnly,
            rootBounds: null,
          }) as IntersectionObserverEntry
      );
      this.callback(entries, this as unknown as IntersectionObserver);
    }

    static reset() {
      MockIntersectionObserver.instances = [];
    }

    static triggerAll(isIntersecting = true) {
      for (const instance of MockIntersectionObserver.instances) {
        instance.trigger(isIntersecting);
      }
    }
  }

  beforeEach(() => {
    vi.restoreAllMocks();
    MockIntersectionObserver.reset();
    vi.stubGlobal('IntersectionObserver', MockIntersectionObserver as unknown as typeof IntersectionObserver);
    mockWindowOpen.mockReset();
    vi.spyOn(window, 'open').mockImplementation(mockWindowOpen as typeof window.open);

    mockUseBtMarginIndicators.mockReset();
    mockUseStockData.mockReset();
    mockUseFundamentals.mockReset();
    mockFundamentalsPanelProps.mockReset();
    mockFactorRegressionPanelProps.mockReset();

    mockSettings.showPPOChart = true;
    mockSettings.showVolumeComparison = true;
    mockSettings.showTradingValueMA = true;
    mockSettings.showRiskAdjustedReturnChart = true;
    mockSettings.showFundamentalsPanel = true;
    mockSettings.showFundamentalsHistoryPanel = true;
    mockSettings.showMarginPressurePanel = true;
    mockSettings.showFactorRegressionPanel = true;
    mockSettings.fundamentalsPanelOrder = ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'];
    mockSettings.fundamentalsMetricOrder = [...DEFAULT_FUNDAMENTAL_METRIC_ORDER];
    mockSettings.fundamentalsMetricVisibility = { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY };
    mockSettings.tradingValueMA.period = 15;
    mockSettings.relativeMode = true;

    mockUseFundamentals.mockReturnValue({ data: null });
  });

  it('renders loading state', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: true,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: null });

    render(<ChartsPage />);
    expect(screen.getByText(/Loading chart data/i)).toBeInTheDocument();
  });

  it('renders error state', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: new Error('Boom'),
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: null });

    render(<ChartsPage />);
    expect(screen.getByText(/Unable to load chart data/i)).toBeInTheDocument();
    expect(screen.getByText('Boom')).toBeInTheDocument();
  });

  it('renders empty state when no symbol selected', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: null,
      selectedSymbol: null,
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: null });

    render(<ChartsPage />);
    expect(screen.getByText(/Start Trading Analysis/i)).toBeInTheDocument();
  });

  it('renders generic error message when error is not an Error instance', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: 'string error',
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: null });

    render(<ChartsPage />);
    expect(screen.getByText('An unexpected error occurred while fetching market data')).toBeInTheDocument();
  });

  it('renders chart panels when data is available', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    expect(screen.getByText('Stock Chart')).toBeInTheDocument();
    expect(screen.getByText('PPO Chart')).toBeInTheDocument();
    expect(screen.getByText('Risk Adjusted Return Chart')).toBeInTheDocument();
    expect(screen.getByText('Volume Comparison')).toBeInTheDocument();
    expect(screen.getByText('Trading Value MA')).toBeInTheDocument();
    expect(screen.getByText('Fundamentals Panel')).toBeInTheDocument();
    expect(screen.getByText('FY History Panel')).toBeInTheDocument();
    expect(screen.getByText('Factor Regression Panel')).toBeInTheDocument();
    expect(screen.getAllByText('Margin Pressure Chart')).toHaveLength(3);
    expect(screen.getByText('Test Co')).toBeInTheDocument();
    expect(screen.getByText(/7203/)).toBeInTheDocument();
    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: false, tradingValuePeriod: 15 });
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
      tradingValuePeriod: 15,
    });
  });

  it('hides panel sections and disables related queries when panel flags are off', () => {
    mockSettings.showFundamentalsPanel = false;
    mockSettings.showFundamentalsHistoryPanel = false;
    mockSettings.showMarginPressurePanel = false;
    mockSettings.showFactorRegressionPanel = false;

    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    expect(screen.queryByText('Fundamentals Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('FY History Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('Factor Regression Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('信用圧力指標')).not.toBeInTheDocument();
    expect(screen.queryByText(/時価総額/)).not.toBeInTheDocument();

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: false, tradingValuePeriod: 15 });
    expect(mockUseBtMarginIndicators).toHaveBeenCalledWith('7203', { enabled: false });
    expect(mockFactorRegressionPanelProps).not.toHaveBeenCalled();
  });

  it('renders FY, margin pressure, and factor sections in the expected order', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    const fyHeading = screen.getByRole('heading', { name: 'FY推移' });
    const marginHeading = screen.getByRole('heading', { name: /^信用圧力指標/ });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });

    expect(fyHeading.compareDocumentPosition(marginHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(marginHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(screen.queryByRole('heading', { name: 'FY推移（過去5期）' })).not.toBeInTheDocument();
  });

  it('renders panel sections based on configured order', () => {
    mockSettings.fundamentalsPanelOrder = ['marginPressure', 'fundamentalsHistory', 'factorRegression', 'fundamentals'];
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    const marginHeading = screen.getByRole('heading', { name: /^信用圧力指標/ });
    const fyHeading = screen.getByRole('heading', { name: 'FY推移' });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });
    const fundamentalsHeading = screen.getByRole('heading', { name: 'Fundamental Analysis' });

    expect(marginHeading.compareDocumentPosition(fyHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(fyHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(factorHeading.compareDocumentPosition(fundamentalsHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
  });

  it('normalizes trading value period before passing to fundamentals hooks/components', () => {
    mockSettings.tradingValueMA.period = 0;
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: false, tradingValuePeriod: 1 });
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      tradingValuePeriod: 1,
    });
  });

  it('adjusts fundamentals panel height based on visible metric count', () => {
    mockSettings.fundamentalsMetricVisibility = {
      ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
      pbr: false,
      roe: false,
      roa: false,
      eps: false,
      bps: false,
      dividendPerShare: false,
      operatingMargin: false,
      netMargin: false,
      cashFlowOperating: false,
      cashFlowInvesting: false,
      cashFlowFinancing: false,
      cashAndEquivalents: false,
      fcf: false,
      fcfYield: false,
      fcfMargin: false,
      cfoToNetProfitRatio: false,
      tradingValueToMarketCapRatio: false,
    };
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    render(<ChartsPage />);

    const section = screen.getByTestId('fundamentals-panel-section');
    const visibleCount = countVisibleFundamentalMetrics(
      mockSettings.fundamentalsMetricOrder,
      mockSettings.fundamentalsMetricVisibility
    );
    expect(section).toHaveStyle(`height: ${resolveFundamentalsPanelHeightPx(visibleCount)}px`);
  });

  it('renders margin loading and error states in indicator section', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });

    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });
    const { rerender } = render(<ChartsPage />);
    expect(screen.getByText('Loading...')).toBeInTheDocument();

    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('boom'),
    });
    rerender(<ChartsPage />);
    expect(screen.getByText('Failed to load margin pressure data')).toBeInTheDocument();
  });

  it('renders market cap and opens external links when available', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockImplementation(
      (_symbol: string, options?: { enabled?: boolean; tradingValuePeriod?: number }) => ({
        data: options?.enabled ? { dailyValuation: [{ marketCap: 1000000000 }] } : null,
      })
    );

    render(<ChartsPage />);

    act(() => {
      MockIntersectionObserver.triggerAll(true);
    });

    expect(screen.getByText(/時価総額/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /四季報/i }));
    fireEvent.click(screen.getByRole('button', { name: /B\.C\./i }));

    expect(mockWindowOpen).toHaveBeenCalledWith(
      'https://shikiho.toyokeizai.net/stocks/7203',
      '_blank',
      'noopener,noreferrer'
    );
    expect(mockWindowOpen).toHaveBeenCalledWith(
      'https://www.buffett-code.com/company/7203/',
      '_blank',
      'noopener,noreferrer'
    );
  });

  it('falls back to immediate visibility when IntersectionObserver is unavailable', async () => {
    vi.stubGlobal('IntersectionObserver', undefined);

    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          indicators: {},
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({ data: null, isLoading: false, error: null });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    render(<ChartsPage />);

    await waitFor(() => {
      expect(mockUseBtMarginIndicators).toHaveBeenLastCalledWith('7203', { enabled: true });
    });
    await waitFor(() => {
      expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    });
  });

  it('defers panel queries until sections become visible', async () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseBtMarginIndicators.mockReturnValue({ data: null, isLoading: false, error: null });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    render(<ChartsPage />);

    expect(mockUseBtMarginIndicators).toHaveBeenCalledWith('7203', { enabled: false });
    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: false, tradingValuePeriod: 15 });
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
      tradingValuePeriod: 15,
    });
    expect(mockFactorRegressionPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
    });

    act(() => {
      MockIntersectionObserver.triggerAll(true);
    });

    await waitFor(() => {
      expect(mockUseBtMarginIndicators).toHaveBeenLastCalledWith('7203', { enabled: true });
    });
    await waitFor(() => {
      expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    });
    await waitFor(() => {
      expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
        symbol: '7203',
        enabled: true,
        tradingValuePeriod: 15,
      });
    });
    await waitFor(() => {
      expect(mockFactorRegressionPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
        symbol: '7203',
        enabled: true,
      });
    });
  });

  it('starts deferred queries when chart panels mount after initial loading', async () => {
    let chartState: {
      chartData: unknown;
      isLoading: boolean;
      error: unknown;
      selectedSymbol: string | null;
    } = {
      chartData: null,
      isLoading: true,
      error: null,
      selectedSymbol: '7203',
    };
    mockUseMultiTimeframeChart.mockImplementation(() => chartState);
    mockUseBtMarginIndicators.mockReturnValue({ data: null, isLoading: false, error: null });
    mockUseStockData.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    const { rerender } = render(<ChartsPage />);

    expect(mockUseBtMarginIndicators).toHaveBeenLastCalledWith('7203', { enabled: false });
    expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: false, tradingValuePeriod: 15 });

    chartState = {
      chartData: {
        daily: {
          candlestickData: [{ time: '2024-01-01', open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 }],
          indicators: { atrSupport: [], nBarSupport: [], ppo: [] },
          bollingerBands: [],
          volumeComparison: [],
          tradingValueMA: [],
        },
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    };

    rerender(<ChartsPage />);
    expect(MockIntersectionObserver.instances.length).toBeGreaterThan(0);

    act(() => {
      MockIntersectionObserver.triggerAll(true);
    });

    await waitFor(() => {
      expect(mockUseBtMarginIndicators).toHaveBeenLastCalledWith('7203', { enabled: true });
    });
    await waitFor(() => {
      expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    });
  });
});
