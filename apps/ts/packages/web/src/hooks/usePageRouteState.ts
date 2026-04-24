import { useNavigate } from '@tanstack/react-router';
import { useCallback, useEffect, useRef } from 'react';
import {
  BACKTEST_STORE_STORAGE_KEY,
  CHART_STORE_STORAGE_KEY,
  SCREENING_STORE_STORAGE_KEY,
  UI_STORE_STORAGE_KEY,
} from '@/lib/persistedState';
import {
  extractLegacyBacktestSearch,
  extractLegacySymbolWorkbenchSearch,
  extractLegacyIndicesSearch,
  extractLegacyPortfolioSearch,
  extractLegacyScreeningSearch,
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
  type PortfolioSubTab,
  prunePersistedStoreFields,
  readPersistedStoreState,
  type ScreeningRouteSearch,
  serializeBacktestSearch,
  serializeSymbolWorkbenchSearch,
  serializeIndicesSearch,
  serializePortfolioSearch,
  serializeRankingSearch,
  serializeScreeningSearch,
  validateBacktestSearch,
  validateSymbolWorkbenchSearch,
  validatePortfolioSearch,
  validateRankingSearch,
  validateScreeningSearch,
} from '@/lib/routeSearch';
import { backtestRoute, symbolWorkbenchRoute, indicesRoute, portfolioRoute, rankingRoute, screeningRoute } from '@/router';
import type { ScreeningSubTab } from '@/stores/screeningStore';
import type { BacktestSubTab, LabType } from '@/types/backtest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingDailyView, RankingPageTab, RankingParams } from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';
import type { ValueCompositeRankingParams } from '@/types/valueCompositeRanking';

const SYMBOL_WORKBENCH_PATH = '/symbol-workbench';

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

export function useMigrateSymbolWorkbenchRouteState(): void {
  const search = symbolWorkbenchRoute.useSearch();
  useLegacySearchMigration({
    storageType: 'local',
    storageKey: CHART_STORE_STORAGE_KEY,
    pruneFields: ['selectedSymbol'],
    hasManagedSearchValues: Boolean(search.symbol) || Boolean(search.strategy) || Boolean(search.matchedDate),
    extractLegacySearch: extractLegacySymbolWorkbenchSearch,
    to: SYMBOL_WORKBENCH_PATH,
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

export function useMigratePortfolioRouteState(): void {
  const search = portfolioRoute.useSearch();
  useLegacySearchMigration({
    storageType: 'local',
    storageKey: UI_STORE_STORAGE_KEY,
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
    storageKey: UI_STORE_STORAGE_KEY,
    pruneFields: ['selectedIndexCode'],
    hasManagedSearchValues: Boolean(search.code),
    extractLegacySearch: extractLegacyIndicesSearch,
    to: '/indices',
  });
}

export function useScreeningRouteState(): {
  activeSubTab: ScreeningSubTab;
  setActiveSubTab: (tab: ScreeningSubTab) => void;
  preOpenScreeningParams: ScreeningParams;
  setPreOpenScreeningParams: (params: ScreeningParams) => void;
  inSessionScreeningParams: ScreeningParams;
  setInSessionScreeningParams: (params: ScreeningParams) => void;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
  fundamentalRankingParams: FundamentalRankingParams;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
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
        rankingParams: RankingParams;
        fundamentalRankingParams: FundamentalRankingParams;
      }) => {
        activeSubTab: ScreeningSubTab;
        preOpenScreeningParams: ScreeningParams;
        inSessionScreeningParams: ScreeningParams;
        rankingParams: RankingParams;
        fundamentalRankingParams: FundamentalRankingParams;
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
    setRankingParams: (params) => updateSearch((currentState) => ({ ...currentState, rankingParams: params })),
    setFundamentalRankingParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, fundamentalRankingParams: params })),
  };
}

export function useRankingRouteState(): {
  activeSubTab: RankingPageTab;
  setActiveSubTab: (tab: RankingPageTab) => void;
  activeDailyView: RankingDailyView;
  setActiveDailyView: (view: RankingDailyView) => void;
  rankingParams: RankingParams;
  setRankingParams: (params: RankingParams) => void;
  fundamentalRankingParams: FundamentalRankingParams;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
  valueCompositeRankingParams: ValueCompositeRankingParams;
  setValueCompositeRankingParams: (params: ValueCompositeRankingParams) => void;
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
        fundamentalRankingParams: FundamentalRankingParams;
        valueCompositeRankingParams: ValueCompositeRankingParams;
      }) => {
        activeSubTab: RankingPageTab;
        activeDailyView: RankingDailyView;
        rankingParams: RankingParams;
        fundamentalRankingParams: FundamentalRankingParams;
        valueCompositeRankingParams: ValueCompositeRankingParams;
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
    setActiveSubTab: (tab) => updateSearch((currentState) => ({ ...currentState, activeSubTab: tab })),
    setActiveDailyView: (view) => updateSearch((currentState) => ({ ...currentState, activeDailyView: view })),
    setRankingParams: (params) => updateSearch((currentState) => ({ ...currentState, rankingParams: params })),
    setFundamentalRankingParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, fundamentalRankingParams: params })),
    setValueCompositeRankingParams: (params) =>
      updateSearch((currentState) => ({ ...currentState, valueCompositeRankingParams: params })),
  };
}

export function useMigrateScreeningRouteState(): void {
  const search = screeningRoute.useSearch();
  const hasManagedSearchValues = Object.keys(search as ScreeningRouteSearch).length > 0;

  useLegacySearchMigration({
    storageType: 'session',
    storageKey: SCREENING_STORE_STORAGE_KEY,
    pruneFields: [
      'activeSubTab',
      'screeningParams',
      'sameDayScreeningParams',
      'preOpenScreeningParams',
      'inSessionScreeningParams',
      'rankingParams',
      'fundamentalRankingParams',
    ],
    hasManagedSearchValues,
    extractLegacySearch: extractLegacyScreeningSearch,
    to: '/screening',
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

export function useMigrateBacktestRouteState(): void {
  const search = backtestRoute.useSearch();
  const hasManagedSearchValues = Object.keys(search).length > 0;

  useLegacySearchMigration({
    storageType: 'local',
    storageKey: BACKTEST_STORE_STORAGE_KEY,
    pruneFields: ['activeSubTab', 'selectedStrategy', 'selectedResultJobId', 'selectedDatasetName', 'activeLabType'],
    hasManagedSearchValues,
    extractLegacySearch: extractLegacyBacktestSearch,
    to: '/backtest',
  });
}
