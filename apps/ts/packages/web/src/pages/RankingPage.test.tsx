import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_FUNDAMENTAL_RANKING_PARAMS, DEFAULT_RANKING_PARAMS } from '@/stores/screeningStore';
import type { RankingDailyView, RankingPageTab } from '@/types/ranking';
import { RankingPage } from './RankingPage';

const mockNavigate = vi.fn();
const mockSetActiveSubTab = vi.fn((tab: RankingPageTab) => {
  mockRouteState.activeSubTab = tab;
});
const mockSetActiveDailyView = vi.fn((view: RankingDailyView) => {
  mockRouteState.activeDailyView = view;
});
const mockSetRankingParams = vi.fn((params: typeof DEFAULT_RANKING_PARAMS) => {
  mockRouteState.rankingParams = params;
});
const mockSetFundamentalRankingParams = vi.fn((params: typeof DEFAULT_FUNDAMENTAL_RANKING_PARAMS) => {
  mockRouteState.fundamentalRankingParams = params;
});
const mockRouteState = {
  activeSubTab: 'ranking' as 'ranking' | 'fundamentalRanking',
  activeDailyView: 'stocks' as RankingDailyView,
  setActiveSubTab: mockSetActiveSubTab,
  setActiveDailyView: mockSetActiveDailyView,
  rankingParams: { ...DEFAULT_RANKING_PARAMS },
  setRankingParams: mockSetRankingParams,
  fundamentalRankingParams: { ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS },
  setFundamentalRankingParams: mockSetFundamentalRankingParams,
};

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/usePageRouteState', () => ({
  useRankingRouteState: () => mockRouteState,
}));

vi.mock('@/hooks/useRanking', () => ({
  useRanking: () => ({
    data: {
      rankings: {
        tradingValue: [],
        gainers: [],
        losers: [],
        periodHigh: [],
        periodLow: [],
      },
      indexPerformance: [],
    },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/hooks/useFundamentalRanking', () => ({
  useFundamentalRanking: () => ({
    data: { metricKey: 'eps_forecast_to_actual', rankings: { ratioHigh: [], ratioLow: [] } },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/hooks/useTopix100Ranking', () => ({
  useTopix100Ranking: () => ({
    data: {
      date: '2026-03-25',
      rankingMetric: 'price_vs_sma_gap',
      smaWindow: 50,
      shortWindowStreaks: 3,
      longWindowStreaks: 53,
      longScoreHorizonDays: 5,
      shortScoreHorizonDays: 1,
      scoreSourceRunId: '20260406_180623_c0eb7f87',
      itemCount: 0,
      items: [],
    },
    isLoading: false,
    error: null,
  }),
}));

vi.mock('@/components/Ranking', () => ({
  RANKING_LOOKBACK_OPTIONS: [
    { value: 1, label: '1 day' },
    { value: 5, label: '5 days' },
  ],
  IndexPerformanceTable: ({ onIndexClick }: { onIndexClick: (code: string) => void }) => (
    <button type="button" onClick={() => onIndexClick('TOPIX')}>
      Index Performance
    </button>
  ),
  RankingFilters: () => <div>Ranking Filters</div>,
  RankingSummary: () => <div>Ranking Summary</div>,
  RankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('6758')}>
      Ranking Row
    </button>
  ),
  Topix100RankingFilters: () => <div>TOPIX100 Ranking Filters</div>,
  Topix100RankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('7203')}>
      TOPIX100 Ranking Table
    </button>
  ),
}));

vi.mock('@/components/FundamentalRanking', () => ({
  FundamentalRankingFilters: () => <div>Fundamental Ranking Filters</div>,
  FundamentalRankingSummary: () => <div>Fundamental Ranking Summary</div>,
  FundamentalRankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('9432')}>
      Fundamental Ranking Row
    </button>
  ),
}));

describe('RankingPage', () => {
  beforeEach(() => {
    mockRouteState.activeSubTab = 'ranking';
    mockRouteState.activeDailyView = 'stocks';
    mockRouteState.rankingParams = { ...DEFAULT_RANKING_PARAMS };
    mockRouteState.fundamentalRankingParams = { ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS };
    mockNavigate.mockReset();
    mockSetActiveSubTab.mockClear();
    mockSetActiveDailyView.mockClear();
    mockSetRankingParams.mockClear();
    mockSetFundamentalRankingParams.mockClear();
  });

  it('renders daily ranking by default', () => {
    render(<RankingPage />);

    expect(screen.getByRole('heading', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByText('Daily market ranking')).toBeInTheDocument();
    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Individual Stocks' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Indices' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'TOPIX100 Study' })).toBeInTheDocument();
    expect(screen.queryByText('Ranking Summary')).not.toBeInTheDocument();
    expect(screen.queryByText('Index Performance')).not.toBeInTheDocument();
  });

  it('switches to fundamental ranking tab', async () => {
    const user = userEvent.setup();
    const view = render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'Fundamental Ranking' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('Forecast / actual EPS')).toBeInTheDocument();
    expect(screen.getByText('Fundamental Ranking Filters')).toBeInTheDocument();
    expect(screen.getByText('Fundamental Ranking Summary')).toBeInTheDocument();
  });

  it('navigates to charts when a ranking row is selected', async () => {
    const user = userEvent.setup();
    render(<RankingPage />);

    await user.click(screen.getByText('Ranking Row'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts', search: { symbol: '6758' } });
  });

  it('switches daily ranking to indices view', async () => {
    const user = userEvent.setup();
    const view = render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'Indices' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('Indices Filters')).toBeInTheDocument();
    expect(screen.getAllByText('Lookback Days')).toHaveLength(1);
    expect(
      screen.getByText('Index performance compares each latest close with the selected trading sessions earlier.')
    ).toBeInTheDocument();
    expect(screen.getByText('Index Performance')).toBeInTheDocument();
    expect(screen.queryByText('Ranking Filters')).not.toBeInTheDocument();
    expect(screen.queryByText('Ranking Summary')).not.toBeInTheDocument();
  });

  it('switches daily ranking to topix100 view', async () => {
    const user = userEvent.setup();
    const view = render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'TOPIX100 Study' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('Price / SMA50 Gap')).toBeInTheDocument();
    expect(screen.getByText('TOPIX100 Ranking Filters')).toBeInTheDocument();
    expect(screen.getByText('TOPIX100 Ranking Table')).toBeInTheDocument();
    expect(screen.queryByText('Ranking Filters')).not.toBeInTheDocument();
  });

  it('navigates to indices when an index row is selected', async () => {
    const user = userEvent.setup();
    mockRouteState.activeDailyView = 'indices';
    render(<RankingPage />);

    await user.click(screen.getByText('Index Performance'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/indices', search: { code: 'TOPIX' } });
  });

  it('navigates to charts when a TOPIX100 row is selected', async () => {
    const user = userEvent.setup();
    mockRouteState.activeDailyView = 'topix100';
    render(<RankingPage />);

    await user.click(screen.getByText('TOPIX100 Ranking Table'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts', search: { symbol: '7203' } });
  });
});
