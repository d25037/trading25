import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_RANKING_PARAMS } from '@/stores/analysisStore';
import {
  useAnalysisRouteState,
  useBacktestRouteState,
  useChartsRouteState,
  useIndicesRouteState,
  useMigrateAnalysisRouteState,
  useMigrateBacktestRouteState,
  useMigrateChartsRouteState,
  useMigrateIndicesRouteState,
  useMigratePortfolioRouteState,
  usePortfolioRouteState,
} from './usePageRouteState';

type RouteSearchState = Record<'charts' | 'portfolio' | 'indices' | 'analysis' | 'backtest', Record<string, unknown>>;

const routeSearchState: RouteSearchState = {
  charts: {},
  portfolio: {},
  indices: {},
  analysis: {},
  backtest: {},
};

const routeKeyByPath = {
  '/analysis': 'analysis',
  '/backtest': 'backtest',
  '/charts': 'charts',
  '/indices': 'indices',
  '/portfolio': 'portfolio',
} as const;

const mockNavigate = vi.fn(
  (options: {
    to: keyof typeof routeKeyByPath;
    replace?: boolean;
    search: Record<string, unknown> | ((current: Record<string, unknown>) => Record<string, unknown>);
  }) => {
    const routeKey = routeKeyByPath[options.to];
    const currentSearch = routeSearchState[routeKey];
    routeSearchState[routeKey] =
      typeof options.search === 'function' ? options.search(currentSearch) : options.search;
    return Promise.resolve();
  }
);

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/router', () => ({
  analysisRoute: { useSearch: () => routeSearchState.analysis },
  backtestRoute: { useSearch: () => routeSearchState.backtest },
  chartsRoute: { useSearch: () => routeSearchState.charts },
  indicesRoute: { useSearch: () => routeSearchState.indices },
  portfolioRoute: { useSearch: () => routeSearchState.portfolio },
}));

function resetRouteSearchState(): void {
  routeSearchState.charts = {};
  routeSearchState.portfolio = {};
  routeSearchState.indices = {};
  routeSearchState.analysis = {};
  routeSearchState.backtest = {};
}

describe('usePageRouteState', () => {
  beforeEach(() => {
    resetRouteSearchState();
    window.localStorage.clear();
    window.sessionStorage.clear();
    mockNavigate.mockClear();
  });
  it('reads and updates chart and indices route state', () => {
    routeSearchState.charts = { symbol: '7203' };
    routeSearchState.indices = { code: 'topix' };

    const chartsHook = renderHook(() => useChartsRouteState());
    const indicesHook = renderHook(() => useIndicesRouteState());

    expect(chartsHook.result.current.selectedSymbol).toBe('7203');
    expect(indicesHook.result.current.selectedIndexCode).toBe('topix');

    act(() => {
      chartsHook.result.current.setSelectedSymbol(' 6758 ', { replace: true });
      indicesHook.result.current.setSelectedIndexCode(' nk225 ', { replace: true });
    });

    expect(routeSearchState.charts).toEqual({ symbol: '6758' });
    expect(routeSearchState.indices).toEqual({ code: 'nk225' });
    expect(mockNavigate).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ to: '/charts', replace: true, search: expect.any(Function) })
    );
    expect(mockNavigate).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ to: '/indices', replace: true, search: { code: 'nk225' } })
    );
  });

  it('preserves strategy context and drops matched date when charts symbol changes', () => {
    routeSearchState.charts = {
      symbol: '7203',
      strategy: 'production/demo',
      matchedDate: '2026-03-14',
    };

    const { result } = renderHook(() => useChartsRouteState());

    act(() => {
      result.current.setSelectedSymbol('6758');
    });

    expect(routeSearchState.charts).toEqual({
      symbol: '6758',
      strategy: 'production/demo',
    });
  });

  it('clears strategy context when charts symbol is cleared', () => {
    routeSearchState.charts = {
      symbol: '7203',
      strategy: 'production/demo',
      matchedDate: '2026-03-14',
    };

    const { result } = renderHook(() => useChartsRouteState());

    act(() => {
      result.current.setSelectedSymbol(null);
    });

    expect(routeSearchState.charts).toEqual({});
  });

  it('preserves portfolio state across sequential updates', () => {
    routeSearchState.portfolio = { portfolioId: 7 };

    const { result } = renderHook(() => usePortfolioRouteState());

    expect(result.current.portfolioSubTab).toBe('portfolios');
    expect(result.current.selectedPortfolioId).toBe(7);
    expect(result.current.selectedWatchlistId).toBeNull();

    act(() => {
      result.current.setSelectedWatchlistId(12);
      result.current.setPortfolioSubTab('watchlists');
    });

    expect(routeSearchState.portfolio).toEqual({
      tab: 'watchlists',
      portfolioId: 7,
      watchlistId: 12,
    });
  });

  it('preserves analysis params across sequential updates', () => {
    const { result } = renderHook(() => useAnalysisRouteState());

    expect(result.current.activeSubTab).toBe('screening');
    expect(result.current.rankingParams).toEqual(DEFAULT_RANKING_PARAMS);

    act(() => {
      result.current.setActiveSubTab('ranking');
      result.current.setRankingParams({
        ...DEFAULT_RANKING_PARAMS,
        limit: 25,
        markets: 'growth',
      });
    });

    expect(routeSearchState.analysis).toEqual({
      tab: 'ranking',
      rankingLimit: 25,
      rankingMarkets: 'growth',
    });
  });

  it('preserves backtest params across sequential updates', () => {
    const { result } = renderHook(() => useBacktestRouteState());

    expect(result.current.activeSubTab).toBe('runner');
    expect(result.current.selectedResultJobId).toBeNull();

    act(() => {
      result.current.setSelectedResultJobId('job-1');
      result.current.setActiveSubTab('results');
      result.current.setSelectedStrategy('production/alpha');
      result.current.setActiveLabType('optimize');
    });

    expect(routeSearchState.backtest).toEqual({
      tab: 'results',
      strategy: 'production/alpha',
      resultJobId: 'job-1',
      labType: 'optimize',
    });
  });

  it('migrates charts and indices persisted state into route search', async () => {
    window.localStorage.setItem(
      'trading25-chart-store',
      JSON.stringify({
        state: {
          selectedSymbol: '7203',
          settings: { visibleBars: 180 },
        },
        version: 0,
      })
    );
    window.localStorage.setItem(
      'trading25-ui-store',
      JSON.stringify({
        state: {
          selectedIndexCode: 'topix',
        },
        version: 0,
      })
    );

    renderHook(() => useMigrateChartsRouteState());
    renderHook(() => useMigrateIndicesRouteState());

    await waitFor(() => {
      expect(routeSearchState.charts).toEqual({ symbol: '7203' });
      expect(routeSearchState.indices).toEqual({ code: 'topix' });
    });

    expect(JSON.parse(window.localStorage.getItem('trading25-chart-store') ?? '{}')).toEqual({
      state: {
        settings: { visibleBars: 180 },
      },
      version: 0,
    });
    expect(window.localStorage.getItem('trading25-ui-store')).toBeNull();
  });

  it('migrates analysis and backtest persisted state when route search is empty', async () => {
    window.sessionStorage.setItem(
      'trading25-analysis-store',
      JSON.stringify({
        state: {
          activeSubTab: 'fundamentalRanking',
          screeningParams: { strategies: 'production/a' },
          rankingParams: { limit: 15 },
          fundamentalRankingParams: { forecastLookbackFyCount: 5 },
        },
        version: 0,
      })
    );
    window.localStorage.setItem(
      'trading25-backtest-store',
      JSON.stringify({
        state: {
          activeSubTab: 'lab',
          selectedStrategy: 'production/beta',
          selectedResultJobId: 'job-9',
          selectedDatasetName: 'snapshot-b',
          activeLabType: 'evolve',
        },
        version: 0,
      })
    );

    renderHook(() => useMigrateAnalysisRouteState());
    renderHook(() => useMigrateBacktestRouteState());

    await waitFor(() => {
      expect(routeSearchState.analysis).toEqual({
        tab: 'fundamentalRanking',
        screeningStrategies: 'production/a',
        rankingLimit: 15,
        forecastLookbackFyCount: 5,
      });
      expect(routeSearchState.backtest).toEqual({
        tab: 'lab',
        strategy: 'production/beta',
        resultJobId: 'job-9',
        dataset: 'snapshot-b',
        labType: 'evolve',
      });
    });

    expect(window.sessionStorage.getItem('trading25-analysis-store')).toBeNull();
    expect(window.localStorage.getItem('trading25-backtest-store')).toBeNull();
  });

  it('skips navigation when route search already manages the migrated state', async () => {
    routeSearchState.portfolio = { tab: 'watchlists' };
    window.localStorage.setItem(
      'trading25-ui-store',
      JSON.stringify({
        state: {
          selectedPortfolioId: 11,
          selectedWatchlistId: 19,
          portfolioSubTab: 'watchlists',
        },
        version: 0,
      })
    );

    renderHook(() => useMigratePortfolioRouteState());

    await waitFor(() => {
      expect(window.localStorage.getItem('trading25-ui-store')).toBeNull();
    });

    expect(routeSearchState.portfolio).toEqual({ tab: 'watchlists' });
    expect(mockNavigate).not.toHaveBeenCalled();
  });
});
