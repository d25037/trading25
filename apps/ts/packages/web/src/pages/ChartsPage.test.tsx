import { render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ChartsPage } from './ChartsPage';

const mockUseMultiTimeframeChart = vi.fn();
const mockUseBtMarginIndicators = vi.fn();
const mockUseStockData = vi.fn();
const mockUseFundamentals = vi.fn();

vi.mock('@/components/Chart/hooks/useMultiTimeframeChart', () => ({
  useMultiTimeframeChart: () => mockUseMultiTimeframeChart(),
}));

vi.mock('@/hooks/useBtMarginIndicators', () => ({
  useBtMarginIndicators: () => mockUseBtMarginIndicators(),
}));

vi.mock('@/hooks/useStockData', () => ({
  useStockData: () => mockUseStockData(),
}));

vi.mock('@/hooks/useFundamentals', () => ({
  useFundamentals: () => mockUseFundamentals(),
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
  chartType: 'candlestick' as const,
  showVolume: true,
  showPPOChart: true,
  showVolumeComparison: true,
  showTradingValueMA: true,
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
  FundamentalsPanel: () => <div>Fundamentals Panel</div>,
}));

vi.mock('@/components/Chart/FundamentalsHistoryPanel', () => ({
  FundamentalsHistoryPanel: () => <div>FY History Panel</div>,
}));

vi.mock('@/components/Chart/FactorRegressionPanel', () => ({
  FactorRegressionPanel: () => <div>Factor Regression Panel</div>,
}));

vi.mock('@/components/Chart/TimeframeSelector', () => ({
  TimeframeSelector: () => <div>Timeframe Selector</div>,
}));

vi.mock('@/components/ErrorBoundary', () => ({
  ErrorBoundary: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

describe('ChartsPage', () => {
  beforeEach(() => {
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
    expect(screen.getByText('Volume Comparison')).toBeInTheDocument();
    expect(screen.getByText('Trading Value MA')).toBeInTheDocument();
    expect(screen.getByText('Fundamentals Panel')).toBeInTheDocument();
    expect(screen.getByText('FY History Panel')).toBeInTheDocument();
    expect(screen.getByText('Factor Regression Panel')).toBeInTheDocument();
    expect(screen.getAllByText('Margin Pressure Chart')).toHaveLength(3);
    expect(screen.getByText('Test Co')).toBeInTheDocument();
    expect(screen.getByText(/7203/)).toBeInTheDocument();
  });
});
