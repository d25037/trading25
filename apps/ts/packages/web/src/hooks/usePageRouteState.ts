import { useNavigate } from '@tanstack/react-router';
import { useCallback, useEffect, useRef } from 'react';
import type { AnalysisSubTab } from '@/stores/analysisStore';
import { analysisRoute, backtestRoute, chartsRoute, indicesRoute, portfolioRoute } from '@/router';
import type { BacktestSubTab, LabType } from '@/types/backtest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';
import {
  extractLegacyAnalysisSearch,
  extractLegacyBacktestSearch,
  extractLegacyChartsSearch,
  extractLegacyIndicesSearch,
  extractLegacyPortfolioSearch,
  getAnalysisStateFromSearch,
  prunePersistedStoreFields,
  readPersistedStoreState,
  serializeAnalysisSearch,
  serializeBacktestSearch,
  serializeChartsSearch,
  serializeIndicesSearch,
  serializePortfolioSearch,
  validateAnalysisSearch,
  validateBacktestSearch,
  validateChartsSearch,
  validatePortfolioSearch,
  type AnalysisRouteSearch,
  type PortfolioSubTab,
} from '@/lib/routeSearch';

function useLegacySearchMigration<TSearch extends object>(params: {
  storageType: 'local' | 'session';
  storageKey: string;
  pruneFields: string[];
  hasManagedSearchValues: boolean;
  extractLegacySearch: (state: Record<string, unknown>) => TSearch;
  to: string;
}): void {
  const navigate = useNavigate();
  const hasRunRef = useRef(false);
  const { storageType, storageKey, pruneFields, hasManagedSearchValues, extractLegacySearch, to } = params;

  useEffect(() => {
    if (hasRunRef.current || typeof window === 'undefined') return;
    hasRunRef.current = true;

    const storage = storageType === 'local' ? window.localStorage : window.sessionStorage;
    const persistedState = readPersistedStoreState(storage, storageKey);
    if (!persistedState) return;

    const legacySearch = extractLegacySearch(persistedState);
    prunePersistedStoreFields(storage, storageKey, pruneFields);

    if (hasManagedSearchValues || Object.keys(legacySearch).length === 0) {
      return;
    }

    void navigate({
      to,
      replace: true,
      search: (current: Record<string, unknown>) => ({ ...current, ...legacySearch }),
    });
  }, [extractLegacySearch, hasManagedSearchValues, navigate, pruneFields, storageKey, storageType, to]);
}

export function useChartsRouteState(): {
  selectedSymbol: string | null;
  strategyName: string | null;
  matchedDate: string | null;
  setSelectedSymbol: (symbol: string | null, options?: { replace?: boolean }) => void;
} {
  const navigate = useNavigate();
  const search = chartsRoute.useSearch();
  const selectedSymbol = search.symbol ?? null;
  const strategyName = search.strategy ?? null;
  const matchedDate = search.matchedDate ?? null;

  const setSelectedSymbol = useCallback(
    (symbol: string | null, options?: { replace?: boolean }) => {
      void navigate({
        to: '/charts',
        replace: options?.replace ?? false,
        search: (current: Record<string, unknown>) => {
          const currentSearch = validateChartsSearch(current);
          const nextSymbol = typeof symbol === 'string' ? symbol.trim() || null : null;

          if (nextSymbol == null) {
            return serializeChartsSearch({ symbol: null, strategy: null, matchedDate: null });
          }

          const preserveMatchedDate = currentSearch.symbol === nextSymbol ? currentSearch.matchedDate ?? null : null;

          return serializeChartsSearch({
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

export function useMigrateChartsRouteState(): void {
  const search = chartsRoute.useSearch();
  useLegacySearchMigration({
    storageType: 'local',
    storageKey: 'trading25-chart-store',
    pruneFields: ['selectedSymbol'],
    hasManagedSearchValues: Boolean(search.symbol) || Boolean(search.strategy) || Boolean(search.matchedDate),
    extractLegacySearch: extractLegacyChartsSearch,
    to: '/charts',
  });
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
      updater: (currentState: {
        tab: PortfolioSubTab;
        portfolioId: number | null;
        watchlistId: number | null;
      }) => {
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

export function useMigratePortfolioRouteState(): void {
  const search = portfolioRoute.useSearch();
  useLegacySearchMigration({
    storageType: 'local',
    storageKey: 'trading25-ui-store',
    pruneFields: ['selectedPortfolioId', 'selectedWatchlistId', 'portfolioSubTab'],
    hasManagedSearchValues:
      Boolean(search.tab) || typeof search.portfolioId === 'number' || typeof search.watchlistId === 'number',
    extractLegacySearch: extractLegacyPortfolioSearch,
    to: '/portfolio',
  });
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

export function useMigrateIndicesRouteState(): void {
  const search = indicesRoute.useSearch();
  useLegacySearchMigration({
    storageType: 'local',
    storageKey: 'trading25-ui-store',
    pruneFields: ['selectedIndexCode'],
    hasManagedSearchValues: Boolean(search.code),
    extractLegacySearch: extractLegacyIndicesSearch,
    to: '/indices',
  });
}

export function useAnalysisRouteState(): {
  activeSubTab: AnalysisSubTab;
  setActiveSubTab: (tab: AnalysisSubTab) => void;
  screeningParams: ScreeningParams;
  setScreeningParams: (params: ScreeningParams) => void;
  sameDayScreeningParams: ScreeningParams;
  setSameDayScreeningParams: (params: ScreeningParams) => void;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
  fundamentalRankingParams: FundamentalRankingParams;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
} {
  const navigate = useNavigate();
  const search = analysisRoute.useSearch();
  const state = getAnalysisStateFromSearch(search);

  const updateSearch = useCallback(
    (
      updater: (currentState: {
        activeSubTab: AnalysisSubTab;
        screeningParams: ScreeningParams;
        sameDayScreeningParams: ScreeningParams;
        rankingParams: RankingParams;
        fundamentalRankingParams: FundamentalRankingParams;
      }) => {
        activeSubTab: AnalysisSubTab;
        screeningParams: ScreeningParams;
        sameDayScreeningParams: ScreeningParams;
        rankingParams: RankingParams;
        fundamentalRankingParams: FundamentalRankingParams;
      }
    ) => {
      void navigate({
        to: '/analysis',
        search: (current: Record<string, unknown>) => {
          const currentState = getAnalysisStateFromSearch(validateAnalysisSearch(current));
          return serializeAnalysisSearch(updater(currentState));
        },
      });
    },
    [navigate]
  );

  return {
    ...state,
    setActiveSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, activeSubTab: tab })),
    setScreeningParams: (params) => updateSearch((currentState) => ({ ...currentState, screeningParams: params })),
    setSameDayScreeningParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, sameDayScreeningParams: params })),
    setRankingParams: (params) => updateSearch((currentState) => ({ ...currentState, rankingParams: params })),
    setFundamentalRankingParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, fundamentalRankingParams: params })),
  };
}

export function useMigrateAnalysisRouteState(): void {
  const search = analysisRoute.useSearch();
  const hasManagedSearchValues = Object.keys(search as AnalysisRouteSearch).length > 0;

  useLegacySearchMigration({
    storageType: 'session',
    storageKey: 'trading25-analysis-store',
    pruneFields: [
      'activeSubTab',
      'screeningParams',
      'sameDayScreeningParams',
      'rankingParams',
      'fundamentalRankingParams',
    ],
    hasManagedSearchValues,
    extractLegacySearch: extractLegacyAnalysisSearch,
    to: '/analysis',
  });
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
    setSelectedStrategy: (strategy) => updateSearch((currentState) => ({ ...currentState, selectedStrategy: strategy })),
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

export function useMigrateBacktestRouteState(): void {
  const search = backtestRoute.useSearch();
  const hasManagedSearchValues = Object.keys(search).length > 0;

  useLegacySearchMigration({
    storageType: 'local',
    storageKey: 'trading25-backtest-store',
    pruneFields: ['activeSubTab', 'selectedStrategy', 'selectedResultJobId', 'selectedDatasetName', 'activeLabType'],
    hasManagedSearchValues,
    extractLegacySearch: extractLegacyBacktestSearch,
    to: '/backtest',
  });
}
