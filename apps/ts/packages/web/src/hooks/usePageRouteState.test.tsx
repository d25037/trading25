import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_RANKING_PARAMS } from '@/stores/screeningStore';
import {
  useBacktestRouteState,
  useIndicesRouteState,
  useRankingRouteState,
  useScreeningRouteState,
  useSymbolWorkbenchRouteState,
  useWatchlistRouteState,
} from './usePageRouteState';

type RouteSearchState = Record<
  'symbolWorkbench' | 'watchlist' | 'indices' | 'screening' | 'ranking' | 'backtest',
  Record<string, unknown>
>;

const routeSearchState: RouteSearchState = {
  symbolWorkbench: {},
  watchlist: {},
  indices: {},
  screening: {},
  ranking: {},
  backtest: {},
};

const routeKeyByPath = {
  '/backtest': 'backtest',
  '/symbol-workbench': 'symbolWorkbench',
  '/indices': 'indices',
  '/watchlist': 'watchlist',
  '/ranking': 'ranking',
  '/screening': 'screening',
} as const;

const mockNavigate = vi.fn(
  (options: {
    to: keyof typeof routeKeyByPath;
    replace?: boolean;
    search: Record<string, unknown> | ((current: Record<string, unknown>) => Record<string, unknown>);
  }) => {
    const routeKey = routeKeyByPath[options.to];
    const currentSearch = routeSearchState[routeKey];
    routeSearchState[routeKey] = typeof options.search === 'function' ? options.search(currentSearch) : options.search;
    return Promise.resolve();
  }
);

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/router', () => ({
  backtestRoute: { useSearch: () => routeSearchState.backtest },
  symbolWorkbenchRoute: { useSearch: () => routeSearchState.symbolWorkbench },
  indicesRoute: { useSearch: () => routeSearchState.indices },
  rankingRoute: { useSearch: () => routeSearchState.ranking },
  screeningRoute: { useSearch: () => routeSearchState.screening },
  watchlistRoute: { useSearch: () => routeSearchState.watchlist },
}));

function resetRouteSearchState(): void {
  routeSearchState.symbolWorkbench = {};
  routeSearchState.watchlist = {};
  routeSearchState.indices = {};
  routeSearchState.screening = {};
  routeSearchState.ranking = {};
  routeSearchState.backtest = {};
}

describe('usePageRouteState', () => {
  beforeEach(() => {
    resetRouteSearchState();
    window.localStorage.clear();
    window.sessionStorage.clear();
    mockNavigate.mockClear();
  });
  it('reads and updates symbol workbench and indices route state', () => {
    routeSearchState.symbolWorkbench = { symbol: '7203' };
    routeSearchState.indices = { code: 'topix' };

    const symbolWorkbenchHook = renderHook(() => useSymbolWorkbenchRouteState());
    const indicesHook = renderHook(() => useIndicesRouteState());

    expect(symbolWorkbenchHook.result.current.selectedSymbol).toBe('7203');
    expect(indicesHook.result.current.selectedIndexCode).toBe('topix');

    act(() => {
      symbolWorkbenchHook.result.current.setSelectedSymbol(' 6758 ', { replace: true });
      indicesHook.result.current.setSelectedIndexCode(' nk225 ', { replace: true });
    });

    expect(routeSearchState.symbolWorkbench).toEqual({ symbol: '6758' });
    expect(routeSearchState.indices).toEqual({ code: 'nk225' });
    expect(mockNavigate).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ to: '/symbol-workbench', replace: true, search: expect.any(Function) })
    );
    expect(mockNavigate).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ to: '/indices', replace: true, search: expect.any(Function) })
    );
  });

  it('coerces numeric symbol workbench route values to strings', () => {
    routeSearchState.symbolWorkbench = { symbol: 7203, strategy: 1234, matchedDate: 20260324 };

    const { result } = renderHook(() => useSymbolWorkbenchRouteState());

    expect(result.current.selectedSymbol).toBe('7203');
    expect(result.current.strategyName).toBe('1234');
    expect(result.current.matchedDate).toBe('20260324');
  });

  it('preserves strategy context and drops matched date when symbol workbench symbol changes', () => {
    routeSearchState.symbolWorkbench = {
      symbol: '7203',
      strategy: 'production/demo',
      matchedDate: '2026-03-14',
    };

    const { result } = renderHook(() => useSymbolWorkbenchRouteState());

    act(() => {
      result.current.setSelectedSymbol('6758');
    });

    expect(routeSearchState.symbolWorkbench).toEqual({
      symbol: '6758',
      strategy: 'production/demo',
    });
  });

  it('clears strategy context when symbol workbench symbol is cleared', () => {
    routeSearchState.symbolWorkbench = {
      symbol: '7203',
      strategy: 'production/demo',
      matchedDate: '2026-03-14',
    };

    const { result } = renderHook(() => useSymbolWorkbenchRouteState());

    act(() => {
      result.current.setSelectedSymbol(null);
    });

    expect(routeSearchState.symbolWorkbench).toEqual({});
  });

  it('preserves watchlist state across sequential updates', () => {
    routeSearchState.watchlist = {};

    const { result } = renderHook(() => useWatchlistRouteState());

    expect(result.current.selectedWatchlistId).toBeNull();

    act(() => {
      result.current.setSelectedWatchlistId(12);
    });

    expect(routeSearchState.watchlist).toEqual({
      watchlistId: 12,
    });
  });

  it('preserves screening params across sequential updates', () => {
    const { result } = renderHook(() => useScreeningRouteState());

    expect(result.current.activeSubTab).toBe('preOpenScreening');
    expect(result.current.preOpenScreeningParams.entry_decidability).toBe('pre_open_decidable');

    act(() => {
      result.current.setActiveSubTab('inSessionScreening');
      result.current.setInSessionScreeningParams({
        ...result.current.inSessionScreeningParams,
        limit: 25,
        markets: 'growth',
      });
    });

    expect(routeSearchState.screening).toEqual({
      tab: 'inSessionScreening',
      inSessionLimit: 25,
      inSessionMarkets: 'growth',
    });
  });

  it('preserves ranking params across sequential updates', () => {
    const { result } = renderHook(() => useRankingRouteState());

    expect(result.current.activeDailyView).toBe('stocks');
    expect(result.current.activeSubTab).toBe('ranking');
    expect(result.current.fundamentalRankingParams).toEqual({
      limit: 20,
      markets: 'prime',
      metricKey: 'eps_forecast_to_actual',
      forecastAboveRecentFyActuals: false,
      forecastLookbackFyCount: 3,
    });
    expect(result.current.rankingParams).toEqual(DEFAULT_RANKING_PARAMS);
    expect(result.current.rankingTableFilters).toEqual({});

    act(() => {
      result.current.setActiveDailyView('indices');
      result.current.setActiveSubTab('fundamentalRanking');
      result.current.setRankingParams({
        ...DEFAULT_RANKING_PARAMS,
        limit: 25,
        markets: 'growth',
      });
      result.current.setRankingTableFilters({
        text: 'sony',
        minForwardPer: 0,
        maxForwardPer: 15,
        valuationSignal: 'undervalued',
      });
      result.current.setFundamentalRankingParams({
        ...result.current.fundamentalRankingParams,
        markets: 'standard',
        forecastAboveRecentFyActuals: true,
        forecastLookbackFyCount: 5,
      });
    });

    expect(routeSearchState.ranking).toEqual({
      dailyView: 'indices',
      tab: 'fundamentalRanking',
      rankingLimit: 25,
      rankingMarkets: 'growth',
      rankingFilterText: 'sony',
      rankingFilterMinForwardPer: 0,
      rankingFilterMaxForwardPer: 15,
      rankingFilterSignal: 'undervalued',
      fundamentalMarkets: 'standard',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 5,
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
});
