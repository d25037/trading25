import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  countVisibleFundamentalMetrics,
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
  resolveFundamentalsPanelHeightPx,
} from '@/constants/fundamentalMetrics';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
} from '@/constants/fundamentalsHistoryMetrics';
import { ApiError } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { SymbolWorkbenchPage } from './SymbolWorkbenchPage';

const mockUseMultiTimeframeChart = vi.fn();
const mockUseBtMarginIndicators = vi.fn();
const mockUseStockInfo = vi.fn();
const mockUseRefreshStocks = vi.fn();
const mockUseFundamentals = vi.fn();
const mockWindowOpen = vi.fn();
const mockFundamentalsPanelProps = vi.fn<(props: unknown) => void>();
const mockFundamentalsHistoryPanelProps = vi.fn<(props: unknown) => void>();
const mockCostStructurePanelProps = vi.fn<(props: unknown) => void>();
const mockSymbolWorkbenchRouteState = {
  selectedSymbol: '7203' as string | null,
  strategyName: null as string | null,
  matchedDate: null as string | null,
  setSelectedSymbol: vi.fn(),
};

vi.mock('@/components/Chart/hooks/useMultiTimeframeChart', () => ({
  useMultiTimeframeChart: (...args: unknown[]) => mockUseMultiTimeframeChart(...args),
}));

vi.mock('@/hooks/usePageRouteState', () => ({
  useSymbolWorkbenchRouteState: () => mockSymbolWorkbenchRouteState,
  useMigrateSymbolWorkbenchRouteState: () => {},
}));

vi.mock('@/hooks/useBtMarginIndicators', () => ({
  useBtMarginIndicators: (...args: unknown[]) => mockUseBtMarginIndicators(...args),
}));

vi.mock('@/hooks/useStockInfo', () => ({
  useStockInfo: (...args: unknown[]) => mockUseStockInfo(...args),
  stockInfoKeys: {
    detail: (symbol: string) => ['stock-info', symbol],
  },
}));

vi.mock('@/hooks/useDbSync', () => ({
  useRefreshStocks: () => mockUseRefreshStocks(),
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
    vwema: { enabled: false, period: 20 },
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
  recentReturn: {
    shortPeriod: 20,
    longPeriod: 60,
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
  chartType: 'candlestick' as const,
  showVolume: true,
  showPPOChart: true,
  showVolumeComparison: true,
  showTradingValueMA: true,
  showRecentReturnChart: true,
  showCMF: true,
  showChaikinOscillator: true,
  showOBVFlowScore: true,
  showRiskAdjustedReturnChart: true,
  showFundamentalsPanel: true,
  showFundamentalsHistoryPanel: true,
  showCostStructurePanel: true,
  showMarginPressurePanel: true,
  showFactorRegressionPanel: true,
  fundamentalsPanelOrder: [
    'fundamentals',
    'fundamentalsHistory',
    'costStructure',
    'marginPressure',
    'factorRegression',
  ],
  workbenchPanelOrder: [
    'ppo',
    'riskAdjustedReturn',
    'recentReturn',
    'volumeComparison',
    'cmf',
    'chaikinOscillator',
    'obvFlowScore',
    'tradingValueMA',
    'fundamentals',
    'fundamentalsHistory',
    'costStructure',
    'marginPressure',
    'factorRegression',
  ],
  fundamentalsMetricOrder: [...DEFAULT_FUNDAMENTAL_METRIC_ORDER],
  fundamentalsMetricVisibility: { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY },
  fundamentalsHistoryMetricOrder: [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER],
  fundamentalsHistoryMetricVisibility: { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY },
  visibleBars: 120,
  relativeMode: true,
};

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => ({ settings: mockSettings }),
}));

vi.mock('@/components/Chart/ChartControls', () => ({
  ChartControls: ({ onSelectSymbol }: { onSelectSymbol: (symbol: string) => void }) => (
    <div>
      <div>Chart Controls</div>
      <button type="button" onClick={() => onSelectSymbol('6758')}>
        Select 6758
      </button>
    </div>
  ),
}));

vi.mock('@/components/Chart/StockChart', () => ({
  StockChart: () => <div>Stock Chart</div>,
}));

vi.mock('@/components/Chart/PPOChart', () => ({
  PPOChart: () => <div>PPO Chart</div>,
}));

vi.mock('@/components/Chart/RecentReturnChart', () => ({
  RecentReturnChart: () => <div>Recent Return Chart</div>,
}));

vi.mock('@/components/Chart/RiskAdjustedReturnChart', () => ({
  RiskAdjustedReturnChart: () => <div>Risk Adjusted Return Chart</div>,
}));

vi.mock('@/components/Chart/SingleValueIndicatorChart', () => ({
  SingleValueIndicatorChart: ({ title }: { title: string }) => <div>{title}</div>,
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

vi.mock('@/components/Chart/ValueCompositeScoreStrip', () => ({
  ValueCompositeScoreStrip: () => <div>Value Score Strip</div>,
}));

vi.mock('@/components/Chart/FundamentalsHistoryPanel', () => ({
  FundamentalsHistoryPanel: (props: unknown) => {
    mockFundamentalsHistoryPanelProps(props);
    return <div>FY History Panel</div>;
  },
}));

vi.mock('@/components/Chart/CostStructurePanel', () => ({
  CostStructurePanel: (props: unknown) => {
    mockCostStructurePanelProps(props);
    return <div>Cost Structure Panel</div>;
  },
}));

const mockFactorRegressionPanelProps = vi.fn<(props: unknown) => void>();
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

function renderSymbolWorkbenchPage() {
  const { queryClient, wrapper } = createTestWrapper();
  return {
    queryClient,
    ...render(<SymbolWorkbenchPage />, { wrapper }),
  };
}

describe('SymbolWorkbenchPage', () => {
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

    mockUseMultiTimeframeChart.mockReset();
    mockUseBtMarginIndicators.mockReset();
    mockUseStockInfo.mockReset();
    mockUseRefreshStocks.mockReset();
    mockUseFundamentals.mockReset();
    mockFundamentalsPanelProps.mockReset();
    mockFundamentalsHistoryPanelProps.mockReset();
    mockCostStructurePanelProps.mockReset();
    mockFactorRegressionPanelProps.mockReset();
    mockSymbolWorkbenchRouteState.selectedSymbol = '7203';
    mockSymbolWorkbenchRouteState.strategyName = null;
    mockSymbolWorkbenchRouteState.matchedDate = null;
    mockSymbolWorkbenchRouteState.setSelectedSymbol.mockReset();

    mockSettings.showPPOChart = true;
    mockSettings.showVolumeComparison = true;
    mockSettings.showTradingValueMA = true;
    mockSettings.showRecentReturnChart = true;
    mockSettings.showRiskAdjustedReturnChart = true;
    mockSettings.showFundamentalsPanel = true;
    mockSettings.showFundamentalsHistoryPanel = true;
    mockSettings.showCostStructurePanel = true;
    mockSettings.showMarginPressurePanel = true;
    mockSettings.showFactorRegressionPanel = true;
    mockSettings.fundamentalsPanelOrder = [
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ];
    mockSettings.workbenchPanelOrder = [
      'ppo',
      'riskAdjustedReturn',
      'recentReturn',
      'volumeComparison',
      'cmf',
      'chaikinOscillator',
      'obvFlowScore',
      'tradingValueMA',
      'fundamentals',
      'fundamentalsHistory',
      'costStructure',
      'marginPressure',
      'factorRegression',
    ];
    mockSettings.fundamentalsMetricOrder = [...DEFAULT_FUNDAMENTAL_METRIC_ORDER];
    mockSettings.fundamentalsMetricVisibility = { ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY };
    mockSettings.fundamentalsHistoryMetricOrder = [...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER];
    mockSettings.fundamentalsHistoryMetricVisibility = { ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY };
    mockSettings.tradingValueMA.period = 15;
    mockSettings.relativeMode = true;

    mockUseFundamentals.mockReturnValue({ data: null });
    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });
    mockUseStockInfo.mockReturnValue({ data: null });
    mockUseRefreshStocks.mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
      error: null,
    });
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
    mockUseStockInfo.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();
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
    mockUseStockInfo.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();
    expect(screen.getByText(/Unable to load chart data/i)).toBeInTheDocument();
    expect(screen.getByText('Boom')).toBeInTheDocument();
  });

  it('shows stock refresh guidance when local stock data is missing', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: new ApiError('Local data missing', 404, {
        message: 'Local stock data is missing.',
        details: [
          { field: 'reason', message: 'local_stock_data_missing' },
          { field: 'recovery', message: 'stock_refresh' },
        ],
      }),
      selectedSymbol: '7203',
    });
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(screen.getByRole('button', { name: /Stock Refresh/i })).toBeInTheDocument();
    expect(screen.getByText(/Use Stock Refresh above to restore the DuckDB snapshot/i)).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /Open Market DB/i })).not.toBeInTheDocument();
  });

  it('does not show market db guidance for local stock data missing even if recovery suggests sync', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: new ApiError('Local snapshot unavailable', 404, {
        message: 'Local stock data is missing.',
        details: [
          { field: 'reason', message: 'local_stock_data_missing' },
          { field: 'recovery', message: 'market_db_sync' },
        ],
      }),
      selectedSymbol: '7203',
    });
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(screen.getByText(/Use Stock Refresh above to restore the DuckDB snapshot/i)).toBeInTheDocument();
    expect(screen.queryByText(/Relative mode requires local TOPIX data/i)).not.toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /Open Market DB/i })).not.toBeInTheDocument();
  });

  it('shows market db guidance when TOPIX data is missing', () => {
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      isLoading: false,
      error: new ApiError('TOPIX missing', 404, {
        message: 'TOPIX data is missing.',
        details: [
          { field: 'reason', message: 'topix_data_missing' },
          { field: 'recovery', message: 'market_db_sync' },
        ],
      }),
      selectedSymbol: '7203',
    });
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(screen.getByRole('button', { name: /Stock Refresh/i })).toBeInTheDocument();
    expect(screen.getByText(/Relative mode requires local TOPIX data/i)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Open Market DB/i })).toHaveAttribute('href', '/market-db');
  });

  it('renders empty state when no symbol selected', () => {
    mockSymbolWorkbenchRouteState.selectedSymbol = null;
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
    mockUseStockInfo.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();
    expect(screen.getByText(/Start Trading Analysis/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '7203' }));
    expect(mockSymbolWorkbenchRouteState.setSelectedSymbol).toHaveBeenCalledWith('7203');
  });

  it('renders generic error message when error is not an Error instance', () => {
    const reloadSpy = vi.spyOn(window.location, 'reload').mockImplementation(() => {});
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
    mockUseStockInfo.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();
    expect(screen.getByText('An unexpected error occurred while fetching market data')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Try Again/i }));
    expect(reloadSpy).toHaveBeenCalledOnce();
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(screen.getByRole('button', { name: /Stock Refresh/i })).toBeInTheDocument();
    expect(screen.getAllByText('Stock Chart').length).toBeGreaterThan(0);
    expect(screen.getByText('PPO Chart')).toBeInTheDocument();
    expect(screen.getByText('Recent Return Chart')).toBeInTheDocument();
    expect(screen.getByText('Risk Adjusted Return Chart')).toBeInTheDocument();
    expect(screen.getByText('Volume Comparison')).toBeInTheDocument();
    expect(screen.getByText('Trading Value MA')).toBeInTheDocument();
    expect(screen.getByText('Fundamentals Panel')).toBeInTheDocument();
    expect(screen.getByText('FY History Panel')).toBeInTheDocument();
    expect(screen.getByText('Cost Structure Panel')).toBeInTheDocument();
    expect(screen.getByText('Factor Regression Panel')).toBeInTheDocument();
    expect(screen.getAllByText('Margin Pressure Chart')).toHaveLength(3);
    expect(screen.getByText('Test Co')).toBeInTheDocument();
    expect(screen.getByText(/7203/)).toBeInTheDocument();
    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
      tradingValuePeriod: 15,
    });
    expect(mockFundamentalsHistoryPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
      metricOrder: DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
      metricVisibility: DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
    });
    expect(mockCostStructurePanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
    });
  });

  it('opens chart controls from the mobile settings action', () => {
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

    renderSymbolWorkbenchPage();

    fireEvent.click(screen.getByRole('button', { name: '設定' }));

    expect(screen.getByRole('dialog', { name: 'Symbol Workbench Settings' })).toBeInTheDocument();
    expect(screen.getAllByText('Chart Controls').length).toBeGreaterThan(1);
  });

  it('passes screening verification context into chart rendering and symbol changes', async () => {
    const user = userEvent.setup();
    mockSymbolWorkbenchRouteState.strategyName = 'production/demo';
    mockSymbolWorkbenchRouteState.matchedDate = '2026-03-14';
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
      signalMarkers: { daily: [], weekly: [], monthly: [] },
      signalResponse: {
        provenance: {
          source_kind: 'market',
          strategy_name: 'production/demo',
          loaded_domains: ['stock_data', 'statements'],
        },
        diagnostics: {},
      },
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(mockUseMultiTimeframeChart).toHaveBeenCalledWith('7203', 'production/demo');
    expect(screen.getAllByText('production/demo (strategy)').length).toBeGreaterThan(0);
    expect(screen.getAllByText('2026-03-14').length).toBeGreaterThan(0);

    await user.click(screen.getByRole('button', { name: 'Select 6758' }));

    expect(mockSymbolWorkbenchRouteState.setSelectedSymbol).toHaveBeenCalledWith('6758');
  });

  it('refreshes the selected symbol and invalidates related chart queries', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn((_request, options) => {
      options?.onSuccess?.({
        totalStocks: 1,
        successCount: 1,
        failedCount: 0,
        totalApiCalls: 1,
        totalRecordsStored: 60,
        results: [{ code: '7203', success: true, recordsFetched: 60, recordsStored: 60 }],
        errors: [],
        lastUpdated: '2026-03-12T00:00:00Z',
      });
    });
    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    const { queryClient } = renderSymbolWorkbenchPage();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    await user.click(screen.getByRole('button', { name: /Stock Refresh/i }));

    expect(mutate).toHaveBeenCalledWith(
      { codes: ['7203'] },
      expect.objectContaining({
        onSuccess: expect.any(Function),
        onError: expect.any(Function),
      })
    );
    expect(await screen.findByText('7203 refreshed: fetched 60, stored 60.')).toBeInTheDocument();

    await waitFor(() => {
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['bt-ohlcv', 'resample', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['bt-indicators', 'compute', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['bt-signals', 'compute', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['fundamentals', 'v2', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['cost-structure', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['bt-margin', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['stock-info', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-stats'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-validation'] });
    });
  });

  it('shows refresh error feedback when stock refresh fails', async () => {
    const user = userEvent.setup();
    const mutate = vi.fn((_request, options) => {
      options?.onError?.(new Error('refresh failed'));
    });
    mockUseRefreshStocks.mockReturnValue({
      mutate,
      isPending: false,
      error: null,
    });
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();
    await user.click(screen.getByRole('button', { name: /Stock Refresh/i }));

    expect(mutate).toHaveBeenCalledWith(
      { codes: ['7203'] },
      expect.objectContaining({
        onSuccess: expect.any(Function),
        onError: expect.any(Function),
      })
    );
    expect(await screen.findByText('refresh failed')).toBeInTheDocument();
  });

  it('hides panel sections and disables related queries when panel flags are off', () => {
    mockSettings.showFundamentalsPanel = false;
    mockSettings.showFundamentalsHistoryPanel = false;
    mockSettings.showCostStructurePanel = false;
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(screen.queryByText('Fundamentals Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('FY History Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('Cost Structure Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('Factor Regression Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('信用圧力指標')).not.toBeInTheDocument();
    expect(screen.getByText('時価総額 (Free Float)')).toBeInTheDocument();
    expect(screen.getByText('時価総額 (発行済み株式数)')).toBeInTheDocument();

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    expect(mockUseBtMarginIndicators).toHaveBeenCalledWith('7203', { enabled: false });
    expect(mockFactorRegressionPanelProps).not.toHaveBeenCalled();
  });

  it('renders FY, cost structure, margin pressure, and factor sections in the expected order', () => {
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    const fyHeading = screen.getByRole('heading', { name: 'FY推移' });
    const costStructureHeading = screen.getByRole('heading', { name: 'Cost Structure Analysis' });
    const marginHeading = screen.getByRole('heading', { name: /^信用圧力指標/ });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });

    expect(fyHeading.compareDocumentPosition(costStructureHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(costStructureHeading.compareDocumentPosition(marginHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(marginHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(screen.queryByRole('heading', { name: 'FY推移（過去5期）' })).not.toBeInTheDocument();
  });

  it('renders panel sections based on configured order', () => {
    mockSettings.fundamentalsPanelOrder = [
      'marginPressure',
      'fundamentalsHistory',
      'factorRegression',
      'costStructure',
      'fundamentals',
    ];
    mockSettings.workbenchPanelOrder = [
      'ppo',
      'riskAdjustedReturn',
      'recentReturn',
      'volumeComparison',
      'cmf',
      'chaikinOscillator',
      'obvFlowScore',
      'tradingValueMA',
      'marginPressure',
      'fundamentalsHistory',
      'factorRegression',
      'costStructure',
      'fundamentals',
    ];
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    const marginHeading = screen.getByRole('heading', { name: /^信用圧力指標/ });
    const fyHeading = screen.getByRole('heading', { name: 'FY推移' });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });
    const costStructureHeading = screen.getByRole('heading', { name: 'Cost Structure Analysis' });
    const fundamentalsHeading = screen.getByRole('heading', { name: 'Fundamental Analysis' });

    expect(marginHeading.compareDocumentPosition(fyHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(fyHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(factorHeading.compareDocumentPosition(costStructureHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(
      costStructureHeading.compareDocumentPosition(fundamentalsHeading) & Node.DOCUMENT_POSITION_FOLLOWING
    ).not.toBe(0);
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: true, tradingValuePeriod: 1 });
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
      cfoYield: false,
      cfoMargin: false,
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    renderSymbolWorkbenchPage();

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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });

    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });
    const { rerender } = renderSymbolWorkbenchPage();
    expect(screen.getByText('Loading...')).toBeInTheDocument();

    mockUseBtMarginIndicators.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('boom'),
    });
    rerender(<SymbolWorkbenchPage />);
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
    mockUseStockInfo.mockReturnValue({
      data: {
        companyName: 'Test Co',
        marketCode: '0111',
        marketName: 'プライム',
        scaleCategory: 'TOPIX Core30',
        sector17Name: '自動車・輸送機',
        sector33Name: '輸送用機器',
      },
    });
    mockUseFundamentals.mockImplementation(
      (_symbol: string, options?: { enabled?: boolean; tradingValuePeriod?: number }) => ({
        data: options?.enabled ? { dailyValuation: [{ marketCap: 1000000000, freeFloatMarketCap: 800000000 }] } : null,
      })
    );

    renderSymbolWorkbenchPage();

    act(() => {
      MockIntersectionObserver.triggerAll(true);
    });

    expect(screen.getByText('市場')).toBeInTheDocument();
    expect(screen.getByText('Prime')).toBeInTheDocument();
    expect(screen.getByText('指数採用')).toBeInTheDocument();
    expect(screen.getByText('Core30')).toBeInTheDocument();
    expect(screen.getByText('セクター17')).toBeInTheDocument();
    expect(screen.getByText('自動車・輸送機')).toBeInTheDocument();
    expect(screen.getByText('セクター33')).toBeInTheDocument();
    expect(screen.getByText('輸送用機器')).toBeInTheDocument();
    expect(screen.getByText('時価総額 (Free Float)')).toBeInTheDocument();
    expect(screen.getByText('時価総額 (発行済み株式数)')).toBeInTheDocument();

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

  it('prefers the market name when the market code has no canonical label mapping', () => {
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
      selectedSymbol: '2510',
    });
    mockUseBtMarginIndicators.mockReturnValue({
      data: { longPressure: [], flowPressure: [], turnoverDays: [], averagePeriod: 20 },
      isLoading: false,
      error: null,
    });
    mockUseStockInfo.mockReturnValue({
      data: {
        companyName: 'ETF Test',
        marketCode: '9999',
        marketName: 'ETF/ETN',
      },
    });

    renderSymbolWorkbenchPage();

    expect(screen.getByText('市場')).toBeInTheDocument();
    expect(screen.getByText('ETF/ETN')).toBeInTheDocument();
    expect(screen.queryByText('9999')).not.toBeInTheDocument();
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();

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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    renderSymbolWorkbenchPage();

    expect(mockUseBtMarginIndicators).toHaveBeenCalledWith('7203', { enabled: false });
    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
      tradingValuePeriod: 15,
    });
    expect(mockCostStructurePanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      symbol: '7203',
      enabled: false,
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
      expect(mockCostStructurePanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
        symbol: '7203',
        enabled: true,
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'Test Co' } });
    mockUseFundamentals.mockReturnValue({ data: null });

    const { rerender } = renderSymbolWorkbenchPage();

    expect(mockUseBtMarginIndicators).toHaveBeenLastCalledWith('7203', { enabled: false });
    expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: true, tradingValuePeriod: 15 });

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

    rerender(<SymbolWorkbenchPage />);
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
