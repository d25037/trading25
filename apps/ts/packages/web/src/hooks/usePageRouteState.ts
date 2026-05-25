import { useNavigate } from '@tanstack/react-router';
import type { LabType } from '@trading25/api-clients/backtest';
import { useCallback } from 'react';
import {
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
  type PortfolioSubTab,
  serializeBacktestSearch,
  serializeIndicesSearch,
  serializePortfolioSearch,
  serializeRankingSearch,
  serializeScreeningSearch,
  serializeSymbolWorkbenchSearch,
  validateBacktestSearch,
  validatePortfolioSearch,
  validateRankingSearch,
  validateScreeningSearch,
  validateSymbolWorkbenchSearch,
} from '@/lib/routeSearch';
import {
  backtestRoute,
  indicesRoute,
  portfolioRoute,
  rankingRoute,
  screeningRoute,
  symbolWorkbenchRoute,
} from '@/router';
import type { ScreeningSubTab } from '@/stores/screeningStore';
import type { BacktestSubTab } from '@/types/backtest';
import type { RankingDailyView, RankingParams } from '@/types/ranking';
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

export function usePortfolioRouteState(): {
  portfolioSubTab: PortfolioSubTab;
  setPortfolioSubTab: (tab: PortfolioSubTab) => void;
  selectedPortfolioId: number | null;
  setSelectedPortfolioId: (id: number | null) => void;
  selectedWatchlistId: number | null;
  setSelectedWatchlistId: (id: number | null) => void;
} {
  const navigate = useNavigate();
  const search = portfolioRoute.useSearch();

  const portfolioSubTab = search.tab ?? 'portfolios';
  const selectedPortfolioId = search.portfolioId ?? null;
  const selectedWatchlistId = search.watchlistId ?? null;

  const updateSearch = useCallback(
    (
      updater: (currentState: { tab: PortfolioSubTab; portfolioId: number | null; watchlistId: number | null }) => {
        tab: PortfolioSubTab;
        portfolioId: number | null;
        watchlistId: number | null;
      }
    ) => {
      void navigate({
        to: '/portfolio',
        search: (current: Record<string, unknown>) => {
          const currentSearch = validatePortfolioSearch(current);
          return serializePortfolioSearch(
            updater({
              tab: currentSearch.tab ?? 'portfolios',
              portfolioId: currentSearch.portfolioId ?? null,
              watchlistId: currentSearch.watchlistId ?? null,
            })
          );
        },
      });
    },
    [navigate]
  );

  return {
    portfolioSubTab,
    setPortfolioSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, tab })),
    selectedPortfolioId,
    setSelectedPortfolioId: (id) => updateSearch((currentState) => ({ ...currentState, portfolioId: id })),
    selectedWatchlistId,
    setSelectedWatchlistId: (id) => updateSearch((currentState) => ({ ...currentState, watchlistId: id })),
  };
}

export function useIndicesRouteState(): {
  selectedIndexCode: string | null;
  setSelectedIndexCode: (code: string | null, options?: { replace?: boolean }) => void;
} {
  const navigate = useNavigate();
  const search = indicesRoute.useSearch();
  const selectedIndexCode = search.code ?? null;

  const setSelectedIndexCode = useCallback(
    (code: string | null, options?: { replace?: boolean }) => {
      void navigate({
        to: '/indices',
        replace: options?.replace ?? false,
        search: serializeIndicesSearch(code),
      });
    },
    [navigate]
  );

  return { selectedIndexCode, setSelectedIndexCode };
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
  activeDailyView: RankingDailyView;
  setActiveDailyView: (view: RankingDailyView) => void;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
} {
  const navigate = useNavigate();
  const search = rankingRoute.useSearch();
  const state = getRankingStateFromSearch(search);

  const updateSearch = useCallback(
    (
      updater: (currentState: { activeDailyView: RankingDailyView; rankingParams: RankingParams }) => {
        activeDailyView: RankingDailyView;
        rankingParams: RankingParams;
      }
    ) => {
      void navigate({
        to: '/ranking',
        search: (current: Record<string, unknown>) => {
          const currentState = getRankingStateFromSearch(validateRankingSearch(current));
          return serializeRankingSearch(updater(currentState));
        },
      });
    },
    [navigate]
  );

  return {
    ...state,
    setActiveDailyView: (view) => updateSearch((currentState) => ({ ...currentState, activeDailyView: view })),
    setRankingParams: (params) => updateSearch((currentState) => ({ ...currentState, rankingParams: params })),
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
