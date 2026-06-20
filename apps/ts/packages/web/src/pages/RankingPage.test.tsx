import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_RANKING_PARAMS } from '@/stores/screeningStore';
import type { RankingDailyView } from '@/types/ranking';
import { RankingPage } from './RankingPage';

const mockNavigate = vi.fn();
const mockSetActiveDailyView = vi.fn((view: RankingDailyView) => {
  mockRouteState.activeDailyView = view;
});
const mockSetRankingParams = vi.fn((params: typeof DEFAULT_RANKING_PARAMS) => {
  mockRouteState.rankingParams = params;
});
const mockSetRankingTableFilters = vi.fn((filters: Record<string, unknown>) => {
  mockRouteState.rankingTableFilters = filters;
});
const mockUseRanking = vi.fn();
const mockUseMarketBubbleFootprint = vi.fn();
const mockUseWatchlists = vi.fn();
const mockUseWatchlistWithItems = vi.fn();
const mockRouteState = {
  activeDailyView: 'stocks' as RankingDailyView,
  setActiveDailyView: mockSetActiveDailyView,
  rankingParams: { ...DEFAULT_RANKING_PARAMS },
  setRankingParams: mockSetRankingParams,
  rankingTableFilters: {},
  setRankingTableFilters: mockSetRankingTableFilters,
};

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    children,
    search,
    to,
    ...props
  }: {
    children: ReactNode;
    search?: { experimentId?: string };
    to: string;
  }) => (
    <a href={`${to}?experimentId=${search?.experimentId ?? ''}`} {...props}>
      {children}
    </a>
  ),
  useNavigate: () => mockNavigate,
}));

vi.mock('@/hooks/usePageRouteState', () => ({
  useRankingRouteState: () => mockRouteState,
}));

vi.mock('@/hooks/useRanking', () => ({
  useRanking: (...args: unknown[]) => mockUseRanking(...args),
}));

vi.mock('@/hooks/useMarketBubbleFootprint', () => ({
  useMarketBubbleFootprint: (...args: unknown[]) => mockUseMarketBubbleFootprint(...args),
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: (...args: unknown[]) => mockUseWatchlists(...args),
  useWatchlistWithItems: (...args: unknown[]) => mockUseWatchlistWithItems(...args),
}));

vi.mock('@/components/Ranking', () => ({
  FORWARD_EPS_DISCLOSURE_OPTIONS: [
    { value: 0, label: 'All' },
    { value: 126, label: '126 days' },
  ],
  PERIOD_OPTIONS: [
    { value: 60, label: '60 days' },
    { value: 250, label: '250 days (1Y)' },
  ],
  RANKING_LOOKBACK_OPTIONS: [
    { value: 1, label: '1 day' },
    { value: 5, label: '5 days' },
  ],
  RANKING_MARKET_OPTIONS: [
    { value: 'prime', label: 'Prime' },
    { value: 'standard', label: 'Standard' },
  ],
  SECTOR_STRENGTH_FAMILY_OPTIONS: [
    { value: 'balanced_sector_strength', label: 'Balanced Sector Strength' },
    { value: 'long_hybrid_leadership', label: 'Long Hybrid Leadership' },
  ],
  IndexPerformanceTable: ({
    headerActions,
    items,
    onIndexClick,
    title,
  }: {
    headerActions?: ReactNode;
    items?: { code: string }[];
    onIndexClick: (code: string) => void;
    title?: string;
  }) => (
    <div>
      <span>{title ?? 'Index Performance'}</span>
      <div>{headerActions}</div>
      <span>index-items:{items?.length ?? 0}</span>
      <button type="button" onClick={() => onIndexClick(items?.[0]?.code ?? 'TOPIX')}>
        Index Performance
      </button>
    </div>
  ),
  RankingFilters: () => <div>Ranking Filters</div>,
  TechnicalEventFilters: () => <div>Technical Event Filters</div>,
  RankingSummary: () => <div>Ranking Summary</div>,
  RankingTable: ({
    items,
    onStockClick,
    title,
    headerActions,
    showValuation,
    showChangeForTradingValue,
    enableColumnSort,
    enableTableFilters,
    filterState,
    filterWatchlists,
    filterWatchlistCodes,
    sortState,
    onSortChange,
  }: {
    items?: unknown[];
    onStockClick: (code: string) => void;
    title?: string;
    showValuation?: boolean;
    showChangeForTradingValue?: boolean;
    enableColumnSort?: boolean;
    enableTableFilters?: boolean;
    headerActions?: ReactNode;
    filterState?: Record<string, unknown>;
    filterWatchlists?: { id: number; name: string }[];
    filterWatchlistCodes?: Set<string>;
    sortState?: { field: string; order: 'asc' | 'desc' };
    onSortChange?: (state: { field: 'forwardPer'; order: 'asc' }) => void;
  }) => (
    <div>
      <span>{title ?? 'Market Rankings'}</span>
      <div>{headerActions}</div>
      <span>items:{items?.length ?? 0}</span>
      <span>{showValuation ? 'valuation columns enabled' : 'valuation columns disabled'}</span>
      <span>{showChangeForTradingValue ? 'trading value change enabled' : 'trading value change disabled'}</span>
      <span>{enableColumnSort ? 'column sort enabled' : 'column sort disabled'}</span>
      <span>{enableTableFilters ? 'table filters enabled' : 'table filters disabled'}</span>
      <span>filter-text:{String(filterState?.text ?? 'none')}</span>
      <span>filter-watchlists:{filterWatchlists?.length ?? 0}</span>
      <span>filter-watchlist-codes:{filterWatchlistCodes?.size ?? 0}</span>
      <span>
        sort:{sortState?.field ?? 'none'}:{sortState?.order ?? 'none'}
      </span>
      <button type="button" onClick={() => onStockClick('6758')}>
        Ranking Row
      </button>
      <button type="button" onClick={() => onSortChange?.({ field: 'forwardPer', order: 'asc' })}>
        Sort Forward PER
      </button>
    </div>
  ),
}));

describe('RankingPage', () => {
  beforeEach(() => {
    mockRouteState.activeDailyView = 'stocks';
    mockRouteState.rankingParams = { ...DEFAULT_RANKING_PARAMS };
    mockRouteState.rankingTableFilters = {};
    mockNavigate.mockReset();
    mockSetActiveDailyView.mockClear();
    mockSetRankingParams.mockClear();
    mockSetRankingTableFilters.mockClear();
    mockUseRanking.mockReset();
    mockUseMarketBubbleFootprint.mockReset();
    mockUseWatchlists.mockReset();
    mockUseWatchlistWithItems.mockReset();
    mockUseRanking.mockReturnValue({
      data: {
        rankings: {
          tradingValue: [{ code: '6758' }],
          gainers: [],
          losers: [],
          periodHigh: [{ code: '8035' }, { code: '9984' }],
          periodLow: [{ code: '7203' }],
        },
        indexPerformance: [
          { code: 'TOPIX', category: 'topix' },
          { code: '004F', category: 'sector33' },
        ],
      },
      isLoading: false,
      error: null,
    });
    mockUseMarketBubbleFootprint.mockReturnValue({
      data: {
        date: '2026-05-29',
        markets: ['prime', 'standard', 'growth'],
        overallRegime: 'blowoff_watch',
        overallScore: 4,
        nearBlowoff: true,
        researchExperimentId: 'market-behavior/market-bubble-footprint',
        reratingExperimentId: 'market-behavior/rerating-bubble-regime-forward-response',
        horizons: [
          {
            horizon: 60,
            score: 3,
            regime: 'crowded',
            nearBlowoff: true,
            intensityLabel: 'Near blowoff',
            activeFlags: [],
          },
        ],
      },
      isLoading: false,
      error: null,
    });
    mockUseWatchlists.mockReturnValue({
      data: { watchlists: [{ id: 12, name: 'Breakout Watch' }] },
      isLoading: false,
      error: null,
    });
    mockUseWatchlistWithItems.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: null,
    });
  });

  it('renders daily ranking by default', () => {
    render(<RankingPage />);

    expect(screen.getByRole('heading', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByText('Daily market ranking')).toBeInTheDocument();
    expect(screen.queryByText('Ranking Filters')).not.toBeInTheDocument();
    expect(screen.getByText('Market Regime')).toBeInTheDocument();
    expect(screen.getByText(/score 3/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Individual Stocks' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Technical Events' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Indices' })).toBeInTheDocument();
    expect(screen.getByLabelText('Preset')).toBeInTheDocument();
    expect(screen.getByText('More')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Fundamental Ranking' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Value Scores' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'TOPIX100 Study' })).not.toBeInTheDocument();
    expect(screen.queryByText('Ranking Summary')).not.toBeInTheDocument();
    expect(screen.queryByText('Index Performance')).not.toBeInTheDocument();
    expect(screen.getByText('items:1')).toBeInTheDocument();
    expect(screen.getByText('valuation columns enabled')).toBeInTheDocument();
    expect(screen.getByText('trading value change enabled')).toBeInTheDocument();
    expect(screen.getByText('column sort enabled')).toBeInTheDocument();
    expect(screen.getByText('table filters enabled')).toBeInTheDocument();
    expect(screen.getByText('filter-text:none')).toBeInTheDocument();
    expect(screen.getByText('filter-watchlists:1')).toBeInTheDocument();
    expect(screen.getByText('filter-watchlist-codes:0')).toBeInTheDocument();
    expect(screen.getByText('sort:tradingValue:desc')).toBeInTheDocument();
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ includeValuation: true }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ includeSectorStrength: true }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(
      expect.objectContaining({ sectorStrengthFamily: 'balanced_sector_strength' }),
      true
    );
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ limit: 0 }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ forwardEpsDisclosedWithinDays: 0 }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ liquidityState: undefined }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ regimeState: undefined }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ riskState: undefined }), true);
    expect(mockUseRanking).toHaveBeenCalledWith(expect.objectContaining({ technicalState: undefined }), true);
    expect(mockUseRanking).not.toHaveBeenCalledWith(expect.objectContaining({ sortBy: 'tradingValue' }), true);
    expect(mockUseMarketBubbleFootprint).toHaveBeenCalledWith(
      expect.objectContaining({ markets: DEFAULT_RANKING_PARAMS.markets, date: DEFAULT_RANKING_PARAMS.date })
    );
  });

  it('passes daily ranking state filters only to individual stocks', async () => {
    const user = userEvent.setup();
    mockRouteState.rankingParams = {
      ...DEFAULT_RANKING_PARAMS,
      regimeState: 'neutral_rerating_good',
      riskState: 'overheat',
      technicalState: 'atr20_acceleration',
    };
    const view = render(<RankingPage />);

    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({ regimeState: 'neutral_rerating_good' }),
      true
    );
    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ riskState: 'overheat' }), true);
    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({ technicalState: 'atr20_acceleration' }),
      true
    );

    await user.click(screen.getByRole('button', { name: 'Technical Events' }));
    view.rerender(<RankingPage />);

    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ liquidityState: undefined }), true);
    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ regimeState: undefined }), true);
    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ riskState: undefined }), true);
    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ technicalState: undefined }), true);
  });

  it('persists daily ranking table sort in route state', async () => {
    const user = userEvent.setup();
    render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'Sort Forward PER' }));

    expect(mockSetRankingParams).toHaveBeenCalledWith({
      ...DEFAULT_RANKING_PARAMS,
      sortBy: 'forwardPer',
      order: 'asc',
    });
  });

  it('passes table filters only to individual stocks ranking table', async () => {
    const user = userEvent.setup();
    mockRouteState.rankingTableFilters = { text: 'sony' };
    const view = render(<RankingPage />);

    expect(screen.getByText('table filters enabled')).toBeInTheDocument();
    expect(screen.getByText('filter-text:sony')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Technical Events' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('table filters disabled')).toBeInTheDocument();
    expect(screen.getByText('filter-text:none')).toBeInTheDocument();
  });

  it('passes selected watchlist codes to individual stocks ranking table', () => {
    mockRouteState.rankingTableFilters = { watchlistId: 12 };
    mockUseWatchlistWithItems.mockReturnValue({
      data: {
        id: 12,
        name: 'Breakout Watch',
        createdAt: '2026-06-20T00:00:00Z',
        updatedAt: '2026-06-20T00:00:00Z',
        items: [
          {
            id: 1,
            watchlistId: 12,
            code: '6758',
            companyName: 'Sony Group',
            createdAt: '2026-06-20T00:00:00Z',
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<RankingPage />);

    expect(mockUseWatchlistWithItems).toHaveBeenCalledWith(12);
    expect(screen.getByText('filter-watchlist-codes:1')).toBeInTheDocument();
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

    expect(screen.queryByText('Indices Filters')).not.toBeInTheDocument();
    expect(screen.getByText('Index Performance')).toBeInTheDocument();
    expect(screen.getByText('33業種指数')).toBeInTheDocument();
    expect(screen.getByText('index-items:1')).toBeInTheDocument();
    expect(screen.queryByText('Ranking Filters')).not.toBeInTheDocument();
    expect(screen.queryByText('Ranking Summary')).not.toBeInTheDocument();
    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({ includeValuation: false, limit: 20, forwardEpsDisclosedWithinDays: 0 }),
      true
    );
    expect(mockUseRanking).toHaveBeenLastCalledWith(expect.objectContaining({ includeSectorStrength: true }), true);
    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({ sectorStrengthFamily: 'balanced_sector_strength' }),
      true
    );
  });

  it('switches daily ranking to technical events view', async () => {
    const user = userEvent.setup();
    const view = render(<RankingPage />);

    await user.click(screen.getByRole('button', { name: 'Technical Events' }));
    view.rerender(<RankingPage />);

    expect(screen.getByText('Technical events')).toBeInTheDocument();
    expect(screen.queryByText('Technical Event Filters')).not.toBeInTheDocument();
    expect(screen.getByText('250日高値')).toBeInTheDocument();
    expect(screen.getByText('items:2')).toBeInTheDocument();
    expect(screen.queryByText('Ranking Filters')).not.toBeInTheDocument();
    expect(mockUseRanking).toHaveBeenLastCalledWith(
      expect.objectContaining({
        includeValuation: true,
        includeSectorStrength: false,
        sectorStrengthFamily: undefined,
        limit: 50,
        forwardEpsDisclosedWithinDays: 0,
      }),
      true
    );
  });

  it('navigates to indices when an index row is selected', async () => {
    const user = userEvent.setup();
    mockRouteState.activeDailyView = 'indices';
    render(<RankingPage />);

    await user.click(screen.getByText('Index Performance'));
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/indices', search: { code: '004F' } });
  });
});
