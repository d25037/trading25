import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    hash,
    children,
    ...props
  }: {
    to: string;
    hash?: string;
    children: ReactNode;
    [key: string]: unknown;
  }) => (
    <a href={`${to}${hash ? `#${hash}` : ''}`} {...props}>
      {children}
    </a>
  ),
}));

const mockUseMultiTimeframeChart = vi.fn();
const mockUseBtMarginIndicators = vi.fn();
const mockUseStockInfo = vi.fn();
const mockUseRefreshStocks = vi.fn();
const mockUseFundamentals = vi.fn();
const mockUseRankingSymbolSnapshot = vi.fn();
const mockUseShikihoSnapshot = vi.fn();
const mockUseWatchlists = vi.fn();
const mockUseAddWatchlistItem = vi.fn();
const mockStockChartProps = vi.fn<(props: unknown) => void>();
const mockFundamentalsPanelProps = vi.fn<(props: unknown) => void>();
const mockFundamentalsHistoryPanelProps = vi.fn<(props: unknown) => void>();
let mockEmbeddedFundamentalsPanelError: Error | null = null;
let mockEmbeddedFundamentalsHistoryError: Error | null = null;
const mockSymbolWorkbenchRouteState = {
  selectedSymbol: '7203' as string | null,
  strategyName: null as string | null,
  matchedDate: null as string | null,
  setSelectedSymbol: vi.fn(),
};

vi.mock('@/components/Chart/hooks/useMultiTimeframeChart', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/components/Chart/hooks/useMultiTimeframeChart')>()),
  useMultiTimeframeChart: (...args: unknown[]) => mockUseMultiTimeframeChart(...args),
}));

vi.mock('@/hooks/usePageRouteState', () => ({
  useSymbolWorkbenchRouteState: () => mockSymbolWorkbenchRouteState,
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

vi.mock('@/hooks/useRankingSymbolSnapshot', () => ({
  useRankingSymbolSnapshot: (...args: unknown[]) => mockUseRankingSymbolSnapshot(...args),
  rankingSymbolSnapshotKeys: {
    detail: (symbol: string) => ['ranking', 'symbol', symbol],
  },
}));

vi.mock('@/hooks/useShikihoSnapshot', () => ({
  useShikihoSnapshot: (...args: unknown[]) => mockUseShikihoSnapshot(...args),
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: (...args: unknown[]) => mockUseWatchlists(...args),
  useAddWatchlistItem: (...args: unknown[]) => mockUseAddWatchlistItem(...args),
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
  showMarginPressurePanel: true,
  showFactorRegressionPanel: true,
  fundamentalsPanelOrder: ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'],
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
  StockChart: (props: unknown) => {
    mockStockChartProps(props);
    return <div>Stock Chart</div>;
  },
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
  FundamentalsPanel: (props: { suppressError?: boolean }) => {
    mockFundamentalsPanelProps(props);
    if (mockEmbeddedFundamentalsPanelError) {
      return props.suppressError ? null : <div role="alert">{mockEmbeddedFundamentalsPanelError.message}</div>;
    }
    return <div>Fundamentals Panel</div>;
  },
}));

vi.mock('@/components/Chart/ValueCompositeScoreStrip', () => ({
  ValueCompositeScoreStrip: () => <div>Value Score Strip</div>,
}));

vi.mock('@/components/Chart/FundamentalsHistoryPanel', () => ({
  FundamentalsHistoryPanel: (props: { suppressError?: boolean }) => {
    mockFundamentalsHistoryPanelProps(props);
    if (mockEmbeddedFundamentalsHistoryError) {
      return props.suppressError ? null : <div role="alert">{mockEmbeddedFundamentalsHistoryError.message}</div>;
    }
    return <div>FY History Panel</div>;
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

function stubMobileWorkbenchLayout() {
  const mediaQueryList = {
    matches: true,
    media: '(max-width: 1023px)',
    onchange: null,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    addListener: vi.fn(),
    removeListener: vi.fn(),
    dispatchEvent: vi.fn(),
  } satisfies MediaQueryList;
  vi.stubGlobal('matchMedia', vi.fn().mockReturnValue(mediaQueryList));
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
    mockStockChartProps.mockReset();

    mockUseMultiTimeframeChart.mockReset();
    mockUseBtMarginIndicators.mockReset();
    mockUseStockInfo.mockReset();
    mockUseRefreshStocks.mockReset();
    mockUseFundamentals.mockReset();
    mockUseRankingSymbolSnapshot.mockReset();
    mockUseShikihoSnapshot.mockReset();
    mockFundamentalsPanelProps.mockReset();
    mockFundamentalsHistoryPanelProps.mockReset();
    mockEmbeddedFundamentalsPanelError = null;
    mockEmbeddedFundamentalsHistoryError = null;
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
    mockSettings.showMarginPressurePanel = true;
    mockSettings.showFactorRegressionPanel = true;
    mockSettings.fundamentalsPanelOrder = ['fundamentals', 'fundamentalsHistory', 'marginPressure', 'factorRegression'];
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
    mockUseRankingSymbolSnapshot.mockReturnValue({
      data: { date: '2026-07-09', item: null, lastUpdated: '2026-07-10T00:00:00Z' },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    mockUseShikihoSnapshot.mockReturnValue({
      bridgeStatus: 'available',
      snapshot: null,
      displaySnapshot: null,
      candidate: null,
      trace: null,
      diagnostic: null,
      captureState: 'not_captured',
      isRefreshing: false,
      refresh: vi.fn(),
    });
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
    mockUseWatchlists.mockReturnValue({
      data: {
        watchlists: [
          {
            id: 3,
            name: 'Long',
            description: null,
            stockCount: 8,
            createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-01-01T00:00:00Z',
          },
        ],
      },
      isLoading: false,
      error: null,
    });
    mockUseAddWatchlistItem.mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
      error: null,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('passes the Shikiho refresh API through to the panel', async () => {
    const refresh = vi.fn();
    mockUseShikihoSnapshot.mockReturnValue({
      bridgeStatus: 'available',
      snapshot: null,
      displaySnapshot: null,
      candidate: null,
      trace: null,
      diagnostic: null,
      captureState: 'not_captured',
      isRefreshing: false,
      refresh,
    });
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      signalMarkers: [],
      signalResponse: null,
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });

    renderSymbolWorkbenchPage();

    await userEvent.click(screen.getByRole('button', { name: '会社四季報を更新' }));
    expect(refresh).toHaveBeenCalledOnce();
  });

  it('keeps the canonical chart overlay separate from the latest quote shown in the Shikiho panel', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-13T01:40:00.000Z'));
    mockSettings.relativeMode = false;
    const observedAt = '2026-07-13T01:35:00.000Z';
    const officialRanking = {
      date: '2026-07-10',
      lastUpdated: '2026-07-10T08:00:00Z',
      item: null,
    };
    mockUseRankingSymbolSnapshot.mockReturnValue({
      data: officialRanking,
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    const canonicalShikihoSnapshot = {
      schemaVersion: 1,
      extractorVersion: 'test',
      code: '7203',
      companyName: 'Toyota',
      sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
      capturedAt: observedAt,
      pageUpdatedAt: null,
      editionLabel: null,
      contentHash: 'test',
      status: 'captured',
      features: null,
      consolidatedBusinesses: null,
      commentary: [],
      score: {
        overall: null,
        growth: null,
        profitability: null,
        safety: null,
        scale: null,
        value: null,
        priceMomentum: null,
      },
      comparisonCompanies: [],
      industries: [],
      marketThemes: [],
      profile: [],
      quote: {
        tradingDate: '2026-07-13',
        observedAt,
        delayMinutes: 15,
        currentPrice: 120,
        open: 112,
        high: 125,
        low: 110,
        previousClose: 108,
        volume: 123_000,
        openTime: null,
        highTime: null,
        lowTime: null,
        sourceLabel: '会社四季報オンライン',
      },
      missingFields: [],
    };
    const candidateShikihoSnapshot = {
      ...canonicalShikihoSnapshot,
      status: 'partial' as const,
      features: 'candidate panel content',
      quote: { ...canonicalShikihoSnapshot.quote, currentPrice: 999 },
    };
    mockUseShikihoSnapshot.mockReturnValue({
      bridgeStatus: 'available',
      snapshot: canonicalShikihoSnapshot,
      displaySnapshot: candidateShikihoSnapshot,
      candidate: candidateShikihoSnapshot,
      trace: null,
      diagnostic: null,
      captureState: 'captured',
      isRefreshing: false,
      refresh: vi.fn(),
    });
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: {
        daily: {
          candlestickData: Array.from({ length: 9 }, (_, index) => ({
            time: `2026-07-${String(index + 2).padStart(2, '0')}`,
            open: 99 + index,
            high: 101 + index,
            low: 98 + index,
            close: 100 + index,
            volume: 10_000,
          })),
          indicators: { sma: [] },
        },
        weekly: { candlestickData: [], indicators: {} },
        monthly: { candlestickData: [], indicators: {} },
      },
      signalMarkers: { daily: [], weekly: [], monthly: [] },
      signalResponse: null,
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    const cachedLatestMetrics = {
      per: 10,
      forwardPer: 9,
      pbr: 2,
      stockPrice: 108,
      eps: 10,
      forwardEps: 12,
      bps: 50,
      sales: 900,
      operatingProfit: 80,
    };
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [{ date: '2026-03-31', periodType: 'FY', eps: 10, bps: 50 }],
        latestMetrics: cachedLatestMetrics,
        dailyValuation: [{ date: '2026-07-10', close: 108, eps: 10, forwardEps: 12, bps: 50, marketCap: 1_080_000 }],
      },
    });

    renderSymbolWorkbenchPage();
    act(() => MockIntersectionObserver.triggerAll(true));

    const stockChartProps = mockStockChartProps.mock.calls.at(-1)?.[0] as {
      data: Array<{ time: string; close: number }>;
      provisionalDate?: string | null;
    };
    expect(stockChartProps.data.at(-1)).toMatchObject({ time: '2026-07-13', close: 120 });
    expect(stockChartProps.provisionalDate).toBe('2026-07-13');
    expect(screen.getByText('￥999')).toBeInTheDocument();
    expect(screen.getAllByText('四季報 15分遅延・当日暫定').length).toBeGreaterThan(0);
    expect(mockFundamentalsPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      latestMetricsOverride: {
        per: 12,
        forwardPer: 10,
        pbr: 2.4,
        stockPrice: 120,
      },
      provisionalLabel: '四季報 15分遅延・当日暫定',
    });
    expect(cachedLatestMetrics).toEqual({
      per: 10,
      forwardPer: 9,
      pbr: 2,
      stockPrice: 108,
      eps: 10,
      forwardEps: 12,
      bps: 50,
      sales: 900,
      operatingProfit: 80,
    });
    expect(officialRanking.item).toBeNull();
    expect(mockUseMultiTimeframeChart).toHaveBeenCalledWith('7203', null);
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

  it('shows normal sync guidance for a typed provider-vintage recovery without starting a mutation', () => {
    const fundamentalsError = new HttpRequestError('Fundamentals PIT snapshot is inconsistent.', 'http', {
      status: 409,
      correlationId: 'corr-1',
      details: [
        { field: 'reason', message: 'pit_snapshot_inconsistent' },
        { field: 'recovery', message: 'market_db_sync' },
      ],
      reason: 'pit_snapshot_inconsistent',
      recovery: 'market_db_sync',
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
      signalMarkers: [],
      signalResponse: null,
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseFundamentals.mockReturnValue({ data: null, error: fundamentalsError });
    mockEmbeddedFundamentalsPanelError = fundamentalsError;
    mockEmbeddedFundamentalsHistoryError = fundamentalsError;

    renderSymbolWorkbenchPage();
    act(() => MockIntersectionObserver.triggerAll(true));

    expect(screen.getAllByText('Fundamentals PIT snapshot is inconsistent.')).toHaveLength(1);
    expect(screen.getAllByRole('alert')).toHaveLength(1);
    expect(screen.getByText(/corr-1/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open Market DB sync' })).toHaveAttribute('href', '/market-db');
    expect(screen.queryByText(/adjusted metrics recovery/i)).not.toBeInTheDocument();
    expect(mockUseRefreshStocks().mutate).not.toHaveBeenCalled();
  });

  it('keeps a panel-specific history error visible when the configurable page query succeeds', () => {
    mockSettings.tradingValueMA.period = 30;
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
      signalMarkers: [],
      signalResponse: null,
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseFundamentals.mockReturnValue({ data: { data: [] }, error: null });
    mockEmbeddedFundamentalsHistoryError = new Error('History query failed at period 15');

    renderSymbolWorkbenchPage();
    act(() => MockIntersectionObserver.triggerAll(true));

    expect(screen.getByRole('alert')).toHaveTextContent('History query failed at period 15');
    expect(mockFundamentalsHistoryPanelProps.mock.calls.at(-1)?.[0]).toMatchObject({
      enabled: true,
      suppressError: false,
    });
    expect(mockUseFundamentals).toHaveBeenLastCalledWith('7203', { enabled: true, tradingValuePeriod: 30 });
  });

  it('does not infer fundamentals recovery from backend message text', () => {
    const fundamentalsError = new HttpRequestError('Run the maintenance action to recover.', 'http', {
      status: 409,
      correlationId: 'corr-2',
    });
    mockUseMultiTimeframeChart.mockReturnValue({
      chartData: null,
      signalMarkers: [],
      signalResponse: null,
      isLoading: false,
      error: null,
      selectedSymbol: '7203',
    });
    mockUseFundamentals.mockReturnValue({ data: null, error: fundamentalsError });

    renderSymbolWorkbenchPage();

    expect(screen.getByText('Run the maintenance action to recover.')).toBeInTheDocument();
    expect(screen.getByText(/corr-2/)).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Open adjusted metrics recovery' })).not.toBeInTheDocument();
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

  it('closes mobile settings after selecting a symbol from the panel', async () => {
    const user = userEvent.setup();
    stubMobileWorkbenchLayout();
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

    await user.click(screen.getByRole('button', { name: '設定' }));
    expect(screen.getByRole('dialog', { name: 'Symbol Workbench Settings' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Select 6758' }));

    expect(mockSymbolWorkbenchRouteState.setSelectedSymbol).toHaveBeenCalledWith('6758');
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Symbol Workbench Settings' })).not.toBeInTheDocument()
    );
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
    expect(mockUseRankingSymbolSnapshot).toHaveBeenCalledWith('7203');
    expect(screen.getAllByText('production/demo (strategy)').length).toBeGreaterThan(0);
    expect(screen.getAllByText('2026-03-14').length).toBeGreaterThan(0);
    expect(screen.getByRole('region', { name: 'Daily Ranking Snapshot' })).toBeInTheDocument();
    expect(screen.getByText('2026-07-09')).toBeInTheDocument();
    expect(screen.getByTestId('daily-ranking-snapshot')).not.toHaveTextContent('2026-03-14');

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
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['bt-margin', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['stock-info', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['ranking', 'symbol', '7203'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-stats'] });
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-validation'] });
    });
  });

  it('adds the displayed symbol to the selected watchlist with the resolved company name', async () => {
    const user = userEvent.setup();
    const addWatchlistItem = vi.fn((_request, options) => {
      options?.onSuccess?.({
        id: 42,
        watchlistId: 3,
        code: '7203',
        companyName: 'トヨタ自動車',
        memo: null,
        createdAt: '2026-01-02T00:00:00Z',
      });
    });
    mockUseAddWatchlistItem.mockReturnValue({
      mutate: addWatchlistItem,
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
    mockUseStockInfo.mockReturnValue({ data: { companyName: 'トヨタ自動車' } });

    renderSymbolWorkbenchPage();

    await user.click(screen.getByRole('button', { name: /Add to Watchlist/i }));
    await user.click(screen.getByRole('button', { name: /^Add$/i }));

    expect(addWatchlistItem).toHaveBeenCalledWith(
      {
        watchlistId: 3,
        data: {
          code: '7203',
          companyName: 'トヨタ自動車',
          memo: undefined,
        },
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
      })
    );
    expect(await screen.findByText('Added 7203 to Long.')).toBeInTheDocument();
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
    expect(screen.queryByText('Factor Regression Panel')).not.toBeInTheDocument();
    expect(screen.queryByText('信用圧力指標')).not.toBeInTheDocument();
    expect(screen.getByText('FF MCap')).toBeInTheDocument();
    expect(screen.getByText('MCap')).toBeInTheDocument();

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

    const fyHeading = screen.getByRole('heading', { name: '業績履歴' });
    const marginHeading = screen.getByRole('heading', { name: /^信用圧力指標/ });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });

    expect(fyHeading.compareDocumentPosition(marginHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(marginHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(screen.queryByRole('heading', { name: 'Cost Structure Analysis' })).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'FY推移（過去5期）' })).not.toBeInTheDocument();
  });

  it('renders panel sections based on configured order', () => {
    mockSettings.fundamentalsPanelOrder = ['marginPressure', 'fundamentalsHistory', 'factorRegression', 'fundamentals'];
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
    const fyHeading = screen.getByRole('heading', { name: '業績履歴' });
    const factorHeading = screen.getByRole('heading', { name: 'Factor Regression Analysis' });
    const fundamentalsHeading = screen.getByRole('heading', { name: 'Fundamental Analysis' });

    expect(marginHeading.compareDocumentPosition(fyHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(fyHeading.compareDocumentPosition(factorHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(factorHeading.compareDocumentPosition(fundamentalsHeading) & Node.DOCUMENT_POSITION_FOLLOWING).not.toBe(0);
    expect(screen.queryByRole('heading', { name: 'Cost Structure Analysis' })).not.toBeInTheDocument();
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

  it('renders market cap and keeps Shikiho navigation local to the panel', () => {
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
        data: options?.enabled
          ? {
              dailyValuation: [{ marketCap: 1000000000, freeFloatMarketCap: 800000000 }],
              liquidityProfile: {
                supported: true,
                modelScope: 'prime',
                date: '2026-05-08',
                recentReturn20dPct: 8.45,
                recentReturn60dPct: 18.32,
                windows: [
                  {
                    advWindow: 20,
                    averageTradingValue: 300000000,
                    freeFloatTradingValueRatioPct: 3.75,
                    liquidityResidualZ: 1.4,
                    liquidityImpliedPrice: 1234,
                    liquidityImpliedPriceGapPct: 23.4,
                    liquidityRegime: 'crowded_rerating',
                  },
                  {
                    advWindow: 60,
                    averageTradingValue: 250000000,
                    freeFloatTradingValueRatioPct: 3.12,
                    liquidityResidualZ: 1.2,
                    liquidityImpliedPrice: 1180,
                    liquidityImpliedPriceGapPct: 18.0,
                    liquidityRegime: 'crowded_rerating',
                  },
                ],
              },
            }
          : null,
      })
    );
    mockUseRankingSymbolSnapshot.mockReturnValue({
      data: {
        date: '2026-07-09',
        item: {
          rank: 1,
          code: '72030',
          companyName: 'Test Co',
          marketCode: 'prime',
          sector33Name: '輸送用機器',
          sectorStrengthScore: 0.9,
          currentPrice: 3000,
          volume: 1_000_000,
          marketCap: 1_200_000_000,
          tradingValue: 1_500_000_000,
          liquidityResidualZ: 1.2,
          liquidityRegime: 'crowded_rerating',
        },
        lastUpdated: '2026-07-10T00:00:00Z',
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderSymbolWorkbenchPage();

    act(() => {
      MockIntersectionObserver.triggerAll(true);
    });

    expect(screen.getByRole('region', { name: 'Daily Ranking Snapshot' })).toBeInTheDocument();
    expect(screen.getByText('2026-07-09')).toBeInTheDocument();
    expect(screen.getByText('Mkt')).toBeInTheDocument();
    expect(screen.getByText('Prime')).toBeInTheDocument();
    expect(screen.getByText('Index')).toBeInTheDocument();
    expect(screen.getByText('Core30')).toBeInTheDocument();
    expect(screen.getByText('S17')).toBeInTheDocument();
    expect(screen.getByText('自動車・輸送機')).toBeInTheDocument();
    expect(screen.getByText('S33')).toBeInTheDocument();
    expect(screen.getByText('輸送用機器')).toBeInTheDocument();
    expect(screen.getByText('MCap')).toBeInTheDocument();
    expect(screen.getByText('FF MCap')).toBeInTheDocument();
    expect(screen.queryByText('Prime Liquidity')).not.toBeInTheDocument();
    expect(screen.queryByText('Med ADV60 / Free Float')).not.toBeInTheDocument();
    expect(screen.queryByText(/流動性等価株価/)).not.toBeInTheDocument();

    expect(screen.queryByRole('button', { name: /^四季報$/ })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toHaveAttribute(
      'href',
      'https://shikiho.toyokeizai.net/stocks/7203'
    );
    expect(screen.queryByRole('button', { name: /B\.C\./i })).not.toBeInTheDocument();
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

    expect(screen.getByText('Mkt')).toBeInTheDocument();
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
