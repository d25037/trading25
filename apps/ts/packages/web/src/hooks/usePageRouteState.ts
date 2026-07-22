import { useNavigate } from '@tanstack/react-router';
import type { LabType } from '@trading25/api-clients/backtest';
import { useCallback } from 'react';
import {
  type BacktestSubTab,
  getIndicesStateFromSearch,
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
  serializeBacktestSearch,
  serializeIndicesSearchForNavigation,
  serializeRankingSearchForNavigation,
  serializeScreeningSearch,
  serializeSymbolWorkbenchSearch,
  serializeWatchlistSearch,
  validateBacktestSearch,
  validateIndicesSearch,
  validateRankingSearch,
  validateScreeningSearch,
  validateSymbolWorkbenchSearch,
  validateWatchlistSearch,
} from '@/lib/routeSearch';
import {
  backtestRoute,
  indicesRoute,
  rankingRoute,
  screeningRoute,
  symbolWorkbenchRoute,
  watchlistRoute,
} from '@/router';
import type { ScreeningSubTab } from '@/stores/screeningStore';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type {
  DailyRankingTableFilters,
  RankingDailyView,
  RankingPageTab,
  RankingParams,
  RankingSortField,
  RankingSortOrder,
} from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';

const SYMBOL_WORKBENCH_PATH = '/symbol-workbench';

function coerceRouteString(value: unknown): string | null {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

export function useSymbolWorkbenchRouteState(): {
  selectedSymbol: string | null;
  strategyName: string | null;
  matchedDate: string | null;
  setSelectedSymbol: (symbol: string | null, options?: { replace?: boolean }) => void;
} {
  const navigate = useNavigate();
  const search = symbolWorkbenchRoute.useSearch();
  const selectedSymbol = coerceRouteString(search.symbol);
  const strategyName = coerceRouteString(search.strategy);
  const matchedDate = coerceRouteString(search.matchedDate);

  const setSelectedSymbol = useCallback(
    (symbol: string | null, options?: { replace?: boolean }) => {
      void navigate({
        to: SYMBOL_WORKBENCH_PATH,
        replace: options?.replace ?? false,
        search: (current: Record<string, unknown>) => {
          const currentSearch = validateSymbolWorkbenchSearch(current);
          const nextSymbol = typeof symbol === 'string' ? symbol.trim() || null : null;

          if (nextSymbol == null) {
            return serializeSymbolWorkbenchSearch({ symbol: null, strategy: null, matchedDate: null });
          }

          const preserveMatchedDate = currentSearch.symbol === nextSymbol ? (currentSearch.matchedDate ?? null) : null;

          return serializeSymbolWorkbenchSearch({
            symbol: nextSymbol,
            strategy: currentSearch.strategy ?? null,
            matchedDate: preserveMatchedDate,
          });
        },
      });
    },
    [navigate]
  );

  return { selectedSymbol, strategyName, matchedDate, setSelectedSymbol };
}

export function useWatchlistRouteState(): {
  selectedWatchlistId: number | null;
  setSelectedWatchlistId: (id: number | null) => void;
} {
  const navigate = useNavigate();
  const search = watchlistRoute.useSearch();

  const selectedWatchlistId = search.watchlistId ?? null;

  const updateSearch = useCallback(
    (updater: (currentState: { watchlistId: number | null }) => { watchlistId: number | null }) => {
      void navigate({
        to: '/watchlist',
        search: (current: Record<string, unknown>) => {
          const currentSearch = validateWatchlistSearch(current);
          return serializeWatchlistSearch(updater({ watchlistId: currentSearch.watchlistId ?? null }));
        },
      });
    },
    [navigate]
  );

  return {
    selectedWatchlistId,
    setSelectedWatchlistId: (id) => updateSearch((currentState) => ({ ...currentState, watchlistId: id })),
  };
}

export function useIndicesRouteState(): {
  selectedIndexCode: string | null;
  setSelectedIndexCode: (code: string | null, options?: { replace?: boolean }) => void;
  sectorMarkets: string;
  setSectorMarkets: (markets: string) => void;
  sectorLookbackDays: number;
  setSectorLookbackDays: (lookbackDays: number) => void;
  sectorSortBy: RankingSortField;
  sectorOrder: RankingSortOrder;
  setSectorSortState: (sortBy: RankingSortField, order: RankingSortOrder) => void;
} {
  const navigate = useNavigate();
  const search = indicesRoute.useSearch();
  const state = getIndicesStateFromSearch(search);

  const updateSearch = useCallback(
    (
      updater: (currentState: ReturnType<typeof getIndicesStateFromSearch>) => ReturnType<typeof getIndicesStateFromSearch>,
      options?: { replace?: boolean }
    ) => {
      void navigate({
        to: '/indices',
        replace: options?.replace ?? false,
        search: (current: Record<string, unknown>) => {
          const currentState = getIndicesStateFromSearch(validateIndicesSearch(current));
          return serializeIndicesSearchForNavigation(current, updater(currentState));
        },
      });
    },
    [navigate]
  );

  const setSelectedIndexCode = useCallback(
    (code: string | null, options?: { replace?: boolean }) => {
      updateSearch((currentState) => ({ ...currentState, selectedIndexCode: code }), options);
    },
    [updateSearch]
  );

  return {
    ...state,
    setSelectedIndexCode,
    setSectorMarkets: (markets) => updateSearch((currentState) => ({ ...currentState, sectorMarkets: markets })),
    setSectorLookbackDays: (lookbackDays) =>
      updateSearch((currentState) => ({ ...currentState, sectorLookbackDays: lookbackDays })),
    setSectorSortState: (sortBy, order) =>
      updateSearch((currentState) => ({ ...currentState, sectorSortBy: sortBy, sectorOrder: order })),
  };
}

export function useScreeningRouteState(): {
  activeSubTab: ScreeningSubTab;
  setActiveSubTab: (tab: ScreeningSubTab) => void;
  preOpenScreeningParams: ScreeningParams;
  setPreOpenScreeningParams: (params: ScreeningParams) => void;
  inSessionScreeningParams: ScreeningParams;
  setInSessionScreeningParams: (params: ScreeningParams) => void;
} {
  const navigate = useNavigate();
  const search = screeningRoute.useSearch();
  const state = getScreeningStateFromSearch(search);

  const updateSearch = useCallback(
    (
      updater: (currentState: {
        activeSubTab: ScreeningSubTab;
        preOpenScreeningParams: ScreeningParams;
        inSessionScreeningParams: ScreeningParams;
      }) => {
        activeSubTab: ScreeningSubTab;
        preOpenScreeningParams: ScreeningParams;
        inSessionScreeningParams: ScreeningParams;
      }
    ) => {
      void navigate({
        to: '/screening',
        search: (current: Record<string, unknown>) => {
          const currentState = getScreeningStateFromSearch(validateScreeningSearch(current));
          return serializeScreeningSearch(updater(currentState));
        },
      });
    },
    [navigate]
  );

  return {
    ...state,
    setActiveSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, activeSubTab: tab })),
    setPreOpenScreeningParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, preOpenScreeningParams: params })),
    setInSessionScreeningParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, inSessionScreeningParams: params })),
  };
}

export function useRankingRouteState(): {
  activeSubTab: RankingPageTab;
  setActiveSubTab: (tab: RankingPageTab) => void;
  activeDailyView: RankingDailyView;
  setActiveDailyView: (view: RankingDailyView) => void;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
  rankingTableFilters: DailyRankingTableFilters;
  setRankingTableFilters: (filters: DailyRankingTableFilters) => void;
  fundamentalRankingParams: FundamentalRankingParams;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
} {
  const navigate = useNavigate();
  const search = rankingRoute.useSearch();
  const state = getRankingStateFromSearch(search);

  const updateSearch = useCallback(
    (
      updater: (currentState: {
        activeSubTab: RankingPageTab;
        activeDailyView: RankingDailyView;
        rankingParams: RankingParams;
        rankingTableFilters: DailyRankingTableFilters;
        fundamentalRankingParams: FundamentalRankingParams;
      }) => {
        activeSubTab: RankingPageTab;
        activeDailyView: RankingDailyView;
        rankingParams: RankingParams;
        rankingTableFilters: DailyRankingTableFilters;
        fundamentalRankingParams: FundamentalRankingParams;
      }
    ) => {
      void navigate({
        to: '/ranking',
        search: (current: Record<string, unknown>) => {
          const currentState = getRankingStateFromSearch(validateRankingSearch(current));
          return serializeRankingSearchForNavigation(current, updater(currentState));
        },
      });
    },
    [navigate]
  );

  return {
    ...state,
    setActiveSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, activeSubTab: tab })),
    setActiveDailyView: (view) => updateSearch((currentState) => ({ ...currentState, activeDailyView: view })),
    setRankingParams: (params) => updateSearch((currentState) => ({ ...currentState, rankingParams: params })),
    setRankingTableFilters: (filters) =>
      updateSearch((currentState) => ({ ...currentState, rankingTableFilters: filters })),
    setFundamentalRankingParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, fundamentalRankingParams: params })),
  };
}

export function useBacktestRouteState(): {
  activeSubTab: BacktestSubTab;
  setActiveSubTab: (tab: BacktestSubTab) => void;
  selectedStrategy: string | null;
  setSelectedStrategy: (strategy: string | null) => void;
  selectedResultJobId: string | null;
  setSelectedResultJobId: (jobId: string | null) => void;
  selectedDatasetName: string | null;
  setSelectedDatasetName: (datasetName: string | null) => void;
  activeLabType: LabType | null;
  setActiveLabType: (labType: LabType | null) => void;
} {
  const navigate = useNavigate();
  const search = backtestRoute.useSearch();
  const activeSubTab = search.tab ?? 'runner';
  const selectedStrategy = search.strategy ?? null;
  const selectedResultJobId = search.resultJobId ?? null;
  const selectedDatasetName = search.dataset ?? null;
  const activeLabType = search.labType ?? null;

  const updateSearch = useCallback(
    (
      updater: (currentState: {
        activeSubTab: BacktestSubTab;
        selectedStrategy: string | null;
        selectedResultJobId: string | null;
        selectedDatasetName: string | null;
        activeLabType: LabType | null;
      }) => {
        activeSubTab: BacktestSubTab;
        selectedStrategy: string | null;
        selectedResultJobId: string | null;
        selectedDatasetName: string | null;
        activeLabType: LabType | null;
      }
    ) => {
      void navigate({
        to: '/backtest',
        search: (current: Record<string, unknown>) => {
          const currentSearch = validateBacktestSearch(current);
          return serializeBacktestSearch(
            updater({
              activeSubTab: currentSearch.tab ?? 'runner',
              selectedStrategy: currentSearch.strategy ?? null,
              selectedResultJobId: currentSearch.resultJobId ?? null,
              selectedDatasetName: currentSearch.dataset ?? null,
              activeLabType: currentSearch.labType ?? null,
            })
          );
        },
      });
    },
    [navigate]
  );

  return {
    activeSubTab,
    setActiveSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, activeSubTab: tab })),
    selectedStrategy,
    setSelectedStrategy: (strategy) =>
      updateSearch((currentState) => ({ ...currentState, selectedStrategy: strategy })),
    selectedResultJobId,
    setSelectedResultJobId: (jobId) =>
      updateSearch((currentState) => ({ ...currentState, selectedResultJobId: jobId })),
    selectedDatasetName,
    setSelectedDatasetName: (datasetName) =>
      updateSearch((currentState) => ({ ...currentState, selectedDatasetName: datasetName })),
    activeLabType,
    setActiveLabType: (labType) => updateSearch((currentState) => ({ ...currentState, activeLabType: labType })),
  };
}
