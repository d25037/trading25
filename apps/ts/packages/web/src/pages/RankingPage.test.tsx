import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
  DEFAULT_RANKING_PARAMS,
  DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS,
} from '@/stores/screeningStore';
import type { RankingPageTab } from '@/types/ranking';
import { RankingPage } from './RankingPage';

const mockNavigate = vi.fn();
const mockSetActiveSubTab = vi.fn((tab: RankingPageTab) => {
  mockRouteState.activeSubTab = tab;
});
const mockSetActiveDailyView = vi.fn((view: 'stocks' | 'indices') => {
  mockRouteState.activeDailyView = view;
});
const mockSetRankingParams = vi.fn((params: typeof DEFAULT_RANKING_PARAMS) => {
  mockRouteState.rankingParams = params;
});
const mockSetFundamentalRankingParams = vi.fn((params: typeof DEFAULT_FUNDAMENTAL_RANKING_PARAMS) => {
  mockRouteState.fundamentalRankingParams = params;
});
const mockSetValueCompositeRankingParams = vi.fn((params: typeof DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS) => {
  mockRouteState.valueCompositeRankingParams = params;
});
const mockRouteState = {
  activeSubTab: 'ranking' as RankingPageTab,
  activeDailyView: 'stocks' as 'stocks' | 'indices',
  setActiveSubTab: mockSetActiveSubTab,
  setActiveDailyView: mockSetActiveDailyView,
  rankingParams: { ...DEFAULT_RANKING_PARAMS },
  setRankingParams: mockSetRankingParams,
  fundamentalRankingParams: { ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS },
  setFundamentalRankingParams: mockSetFundamentalRankingParams,
  valueCompositeRankingParams: { ...DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS },
  setValueCompositeRankingParams: mockSetValueCompositeRankingParams,
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

vi.mock('@/hooks/useValueCompositeRanking', () => ({
  useValueCompositeRanking: () => ({
    data: {
      date: '2026-04-24',
      markets: ['standard'],
      metricKey: 'standard_value_composite',
      scoreMethod: 'standard_pbr_tilt',
      scorePolicy: '35% small market cap + 40% low PBR + 25% low forward PER; no ADV60 floor',
      weights: { smallMarketCap: 0.35, lowPbr: 0.4, lowForwardPer: 0.25 },
      itemCount: 0,
      items: [],
      lastUpdated: '2026-04-24T00:00:00Z',
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

vi.mock('@/components/ValueCompositeRanking', () => ({
  ValueCompositeRankingFilters: () => <div>Value Score Filters</div>,
  ValueCompositeRankingSummary: () => <div>Value Score Summary</div>,
  ValueCompositeRankingTable: ({ onStockClick }: { onStockClick: (code: string) => void }) => (
    <button type="button" onClick={() => onStockClick('9984')}>
      Value Score Row
    </button>
  ),
}));

describe('RankingPage', () => {
  beforeEach(() => {
    mockRouteState.activeSubTab = 'ranking';
    mockRouteState.activeDailyView = 'stocks';
    mockRouteState.rankingParams = { ...DEFAULT_RANKING_PARAMS };
    mockRouteState.fundamentalRankingParams = { ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS };
    mockRouteState.valueCompositeRankingParams = { ...DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS };
    mockNavigate.mockReset();
    mockSetActiveSubTab.mockClear();
    mockSetActiveDailyView.mockClear();
    mockSetRankingParams.mockClear();
    mockSetFundamentalRankingParams.mockClear();
    mockSetValueCompositeRankingParams.mockClear();
  });

  it('renders daily ranking by default', () => {
    render(<RankingPage />);

    expect(screen.getByRole('heading', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByText('Daily market ranking')).toBeInTheDocument();
    expect(screen.getByText('Ranking Filters')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Individual Stocks' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Indices' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'TOPIX100 Study' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Value Scores' })).toBeInTheDocument();
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

  it('switches to value scores tab', async () => {
    const user = userEvent.setup();
    const view = render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'Value Scores' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('Standard PBR tilt score')).toBeInTheDocument();
    expect(screen.getByText('Value Score Filters')).toBeInTheDocument();
    expect(screen.getByText('Value Score Summary')).toBeInTheDocument();
  });

  it('navigates to the symbol workbench when a ranking row is selected', async () => {
    const user = userEvent.setup();
    render(<RankingPage />);

    await user.click(screen.getByText('Ranking Row'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/symbol-workbench', search: { symbol: '6758' } });
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

  it('navigates to indices when an index row is selected', async () => {
    const user = userEvent.setup();
    mockRouteState.activeDailyView = 'indices';
    render(<RankingPage />);

    await user.click(screen.getByText('Index Performance'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/indices', search: { code: 'TOPIX' } });
  });

  it('navigates to the symbol workbench when a value score row is selected', async () => {
    const user = userEvent.setup();
    mockRouteState.activeSubTab = 'valueComposite';
    render(<RankingPage />);

    await user.click(screen.getByText('Value Score Row'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/symbol-workbench', search: { symbol: '9984' } });
  });

});
