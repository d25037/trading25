import type { ScreeningSortBy, SortOrder } from '@trading25/contracts/types/api-response-types';
import {
  DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
  DEFAULT_ORACLE_SCREENING_PARAMS,
  DEFAULT_RANKING_PARAMS,
  DEFAULT_SCREENING_PARAMS,
  type AnalysisSubTab,
} from '@/stores/analysisStore';
import type { BacktestSubTab, LabType } from '@/types/backtest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingParams } from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';

export type PortfolioSubTab = 'portfolios' | 'watchlists';

export interface ChartsRouteSearch {
  symbol?: string;
  strategy?: string;
  matchedDate?: string;
}

export interface PortfolioRouteSearch {
  tab?: PortfolioSubTab;
  portfolioId?: number;
  watchlistId?: number;
}

export interface IndicesRouteSearch {
  code?: string;
}

export interface AnalysisRouteSearch {
  tab?: AnalysisSubTab;
  screeningMarkets?: string;
  screeningStrategies?: string;
  screeningRecentDays?: number;
  screeningDate?: string;
  screeningSortBy?: ScreeningSortBy;
  screeningOrder?: SortOrder;
  screeningLimit?: number;
  oracleMarkets?: string;
  oracleStrategies?: string;
  oracleRecentDays?: number;
  oracleDate?: string;
  oracleSortBy?: ScreeningSortBy;
  oracleOrder?: SortOrder;
  oracleLimit?: number;
  rankingDate?: string;
  rankingLimit?: number;
  rankingMarkets?: string;
  rankingLookbackDays?: number;
  rankingPeriodDays?: number;
  fundamentalLimit?: number;
  fundamentalMarkets?: string;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
}

export interface BacktestRouteSearch {
  tab?: BacktestSubTab;
  strategy?: string;
  resultJobId?: string;
  dataset?: string;
  labType?: LabType;
}

interface PersistedContainer {
  state?: Record<string, unknown>;
  version?: number;
}

const ANALYSIS_SUB_TABS: AnalysisSubTab[] = ['screening', 'oracleScreening', 'ranking', 'fundamentalRanking'];
const PORTFOLIO_SUB_TABS: PortfolioSubTab[] = ['portfolios', 'watchlists'];
const BACKTEST_SUB_TABS: BacktestSubTab[] = ['runner', 'results', 'attribution', 'strategies', 'status', 'dataset', 'lab'];
const LAB_TYPES: LabType[] = ['generate', 'evolve', 'optimize', 'improve'];
const SCREENING_SORT_VALUES: ScreeningSortBy[] = ['bestStrategyScore', 'matchedDate', 'stockCode', 'matchStrategyCount'];
const SORT_ORDER_VALUES: SortOrder[] = ['asc', 'desc'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined;
}

function normalizeBoolean(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') return value;
  if (value === 'true') return true;
  if (value === 'false') return false;
  return undefined;
}

function normalizePositiveInt(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return undefined;
}

function normalizeEnum<T extends string>(value: unknown, values: readonly T[]): T | undefined {
  return typeof value === 'string' && values.includes(value as T) ? (value as T) : undefined;
}

function isEmptyObject(value: Record<string, unknown>): boolean {
  return Object.keys(value).length === 0;
}

function assignIfDefined<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

function assignIfDefinedAndNotDefault<T extends object, K extends keyof T>(
  target: T,
  key: K,
  value: T[K] | undefined,
  defaultValue: T[K]
): void {
  if (value !== undefined && value !== defaultValue) {
    target[key] = value;
  }
}

function assignAnalysisSearchParams<T extends object>(
  base: T,
  entries: Array<[keyof T, T[keyof T] | undefined]>
): T {
  const next = { ...base };

  for (const [key, value] of entries) {
    assignIfDefined(next, key, value);
  }

  return next;
}

export function validateChartsSearch(search: Record<string, unknown>): ChartsRouteSearch {
  const symbol = normalizeString(search.symbol);
  const strategy = normalizeString(search.strategy);
  const matchedDate = normalizeString(search.matchedDate);
  const next: ChartsRouteSearch = {};
  if (symbol) next.symbol = symbol;
  if (strategy) next.strategy = strategy;
  if (matchedDate) next.matchedDate = matchedDate;
  return next;
}

export function serializeChartsSearch(params: {
  symbol?: string | null;
  strategy?: string | null;
  matchedDate?: string | null;
}): ChartsRouteSearch {
  const normalizedSymbol = normalizeString(params.symbol);
  const normalizedStrategy = normalizeString(params.strategy);
  const normalizedMatchedDate = normalizeString(params.matchedDate);
  const next: ChartsRouteSearch = {};
  if (normalizedSymbol) next.symbol = normalizedSymbol;
  if (normalizedStrategy) next.strategy = normalizedStrategy;
  if (normalizedMatchedDate) next.matchedDate = normalizedMatchedDate;
  return next;
}

export function validatePortfolioSearch(search: Record<string, unknown>): PortfolioRouteSearch {
  const next: PortfolioRouteSearch = {};
  const tab = normalizeEnum(search.tab, PORTFOLIO_SUB_TABS);
  const portfolioId = normalizePositiveInt(search.portfolioId);
  const watchlistId = normalizePositiveInt(search.watchlistId);

  if (tab) next.tab = tab;
  if (portfolioId !== undefined) next.portfolioId = portfolioId;
  if (watchlistId !== undefined) next.watchlistId = watchlistId;
  return next;
}

export function serializePortfolioSearch(state: {
  tab: PortfolioSubTab;
  portfolioId: number | null;
  watchlistId: number | null;
}): PortfolioRouteSearch {
  const next: PortfolioRouteSearch = {};

  if (state.tab !== 'portfolios') {
    next.tab = state.tab;
  }
  if (typeof state.portfolioId === 'number' && state.portfolioId > 0) {
    next.portfolioId = state.portfolioId;
  }
  if (typeof state.watchlistId === 'number' && state.watchlistId > 0) {
    next.watchlistId = state.watchlistId;
  }

  return next;
}

export function validateIndicesSearch(search: Record<string, unknown>): IndicesRouteSearch {
  const code = normalizeString(search.code);
  return code ? { code } : {};
}

export function serializeIndicesSearch(code: string | null | undefined): IndicesRouteSearch {
  const normalizedCode = normalizeString(code);
  return normalizedCode ? { code: normalizedCode } : {};
}

export function validateAnalysisSearch(search: Record<string, unknown>): AnalysisRouteSearch {
  const next: AnalysisRouteSearch = {};

  assignIfDefined(next, 'tab', normalizeEnum(search.tab, ANALYSIS_SUB_TABS));
  assignIfDefined(next, 'screeningMarkets', normalizeString(search.screeningMarkets));
  assignIfDefined(next, 'screeningStrategies', normalizeString(search.screeningStrategies));
  assignIfDefined(next, 'screeningRecentDays', normalizePositiveInt(search.screeningRecentDays));
  assignIfDefined(next, 'screeningDate', normalizeString(search.screeningDate));
  assignIfDefined(next, 'screeningSortBy', normalizeEnum(search.screeningSortBy, SCREENING_SORT_VALUES));
  assignIfDefined(next, 'screeningOrder', normalizeEnum(search.screeningOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'screeningLimit', normalizePositiveInt(search.screeningLimit));
  assignIfDefined(next, 'oracleMarkets', normalizeString(search.oracleMarkets));
  assignIfDefined(next, 'oracleStrategies', normalizeString(search.oracleStrategies));
  assignIfDefined(next, 'oracleRecentDays', normalizePositiveInt(search.oracleRecentDays));
  assignIfDefined(next, 'oracleDate', normalizeString(search.oracleDate));
  assignIfDefined(next, 'oracleSortBy', normalizeEnum(search.oracleSortBy, SCREENING_SORT_VALUES));
  assignIfDefined(next, 'oracleOrder', normalizeEnum(search.oracleOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'oracleLimit', normalizePositiveInt(search.oracleLimit));
  assignIfDefined(next, 'rankingDate', normalizeString(search.rankingDate));
  assignIfDefined(next, 'rankingLimit', normalizePositiveInt(search.rankingLimit));
  assignIfDefined(next, 'rankingMarkets', normalizeString(search.rankingMarkets));
  assignIfDefined(next, 'rankingLookbackDays', normalizePositiveInt(search.rankingLookbackDays));
  assignIfDefined(next, 'rankingPeriodDays', normalizePositiveInt(search.rankingPeriodDays));
  assignIfDefined(next, 'fundamentalLimit', normalizePositiveInt(search.fundamentalLimit));
  assignIfDefined(next, 'fundamentalMarkets', normalizeString(search.fundamentalMarkets));
  assignIfDefined(next, 'forecastAboveRecentFyActuals', normalizeBoolean(search.forecastAboveRecentFyActuals));
  assignIfDefined(next, 'forecastLookbackFyCount', normalizePositiveInt(search.forecastLookbackFyCount));

  return next;
}

export function getAnalysisStateFromSearch(search: AnalysisRouteSearch): {
  activeSubTab: AnalysisSubTab;
  screeningParams: ScreeningParams;
  oracleScreeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
} {
  return {
    activeSubTab: search.tab ?? 'screening',
    screeningParams: assignAnalysisSearchParams(
      { ...DEFAULT_SCREENING_PARAMS, mode: 'standard' },
      [
        ['markets', search.screeningMarkets],
        ['strategies', search.screeningStrategies],
        ['recentDays', search.screeningRecentDays],
        ['date', search.screeningDate],
        ['sortBy', search.screeningSortBy],
        ['order', search.screeningOrder],
        ['limit', search.screeningLimit],
      ]
    ),
    oracleScreeningParams: assignAnalysisSearchParams(
      { ...DEFAULT_ORACLE_SCREENING_PARAMS, mode: 'oracle' },
      [
        ['markets', search.oracleMarkets],
        ['strategies', search.oracleStrategies],
        ['recentDays', search.oracleRecentDays],
        ['date', search.oracleDate],
        ['sortBy', search.oracleSortBy],
        ['order', search.oracleOrder],
        ['limit', search.oracleLimit],
      ]
    ),
    rankingParams: assignAnalysisSearchParams({ ...DEFAULT_RANKING_PARAMS }, [
      ['date', search.rankingDate],
      ['limit', search.rankingLimit],
      ['markets', search.rankingMarkets],
      ['lookbackDays', search.rankingLookbackDays],
      ['periodDays', search.rankingPeriodDays],
    ]),
    fundamentalRankingParams: assignAnalysisSearchParams({ ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS }, [
      ['limit', search.fundamentalLimit],
      ['markets', search.fundamentalMarkets],
      ['forecastAboveRecentFyActuals', search.forecastAboveRecentFyActuals],
      ['forecastLookbackFyCount', search.forecastLookbackFyCount],
    ]),
  };
}

export function serializeAnalysisSearch(state: {
  activeSubTab: AnalysisSubTab;
  screeningParams: ScreeningParams;
  oracleScreeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
}): AnalysisRouteSearch {
  const next: AnalysisRouteSearch = {};

  assignIfDefinedAndNotDefault(next, 'tab', state.activeSubTab, 'screening');
  assignIfDefinedAndNotDefault(
    next,
    'screeningMarkets',
    normalizeString(state.screeningParams.markets),
    DEFAULT_SCREENING_PARAMS.markets
  );
  assignIfDefined(next, 'screeningStrategies', normalizeString(state.screeningParams.strategies));
  assignIfDefinedAndNotDefault(
    next,
    'screeningRecentDays',
    typeof state.screeningParams.recentDays === 'number' ? state.screeningParams.recentDays : undefined,
    DEFAULT_SCREENING_PARAMS.recentDays
  );
  assignIfDefined(next, 'screeningDate', normalizeString(state.screeningParams.date));
  assignIfDefinedAndNotDefault(next, 'screeningSortBy', state.screeningParams.sortBy, DEFAULT_SCREENING_PARAMS.sortBy);
  assignIfDefinedAndNotDefault(next, 'screeningOrder', state.screeningParams.order, DEFAULT_SCREENING_PARAMS.order);
  assignIfDefinedAndNotDefault(
    next,
    'screeningLimit',
    typeof state.screeningParams.limit === 'number' ? state.screeningParams.limit : undefined,
    DEFAULT_SCREENING_PARAMS.limit
  );

  assignIfDefinedAndNotDefault(
    next,
    'oracleMarkets',
    normalizeString(state.oracleScreeningParams.markets),
    DEFAULT_ORACLE_SCREENING_PARAMS.markets
  );
  assignIfDefined(next, 'oracleStrategies', normalizeString(state.oracleScreeningParams.strategies));
  assignIfDefinedAndNotDefault(
    next,
    'oracleRecentDays',
    typeof state.oracleScreeningParams.recentDays === 'number' ? state.oracleScreeningParams.recentDays : undefined,
    DEFAULT_ORACLE_SCREENING_PARAMS.recentDays
  );
  assignIfDefined(next, 'oracleDate', normalizeString(state.oracleScreeningParams.date));
  assignIfDefinedAndNotDefault(
    next,
    'oracleSortBy',
    state.oracleScreeningParams.sortBy,
    DEFAULT_ORACLE_SCREENING_PARAMS.sortBy
  );
  assignIfDefinedAndNotDefault(
    next,
    'oracleOrder',
    state.oracleScreeningParams.order,
    DEFAULT_ORACLE_SCREENING_PARAMS.order
  );
  assignIfDefinedAndNotDefault(
    next,
    'oracleLimit',
    typeof state.oracleScreeningParams.limit === 'number' ? state.oracleScreeningParams.limit : undefined,
    DEFAULT_ORACLE_SCREENING_PARAMS.limit
  );

  assignIfDefined(next, 'rankingDate', normalizeString(state.rankingParams.date));
  assignIfDefinedAndNotDefault(
    next,
    'rankingLimit',
    typeof state.rankingParams.limit === 'number' ? state.rankingParams.limit : undefined,
    DEFAULT_RANKING_PARAMS.limit
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingMarkets',
    normalizeString(state.rankingParams.markets),
    DEFAULT_RANKING_PARAMS.markets
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingLookbackDays',
    typeof state.rankingParams.lookbackDays === 'number' ? state.rankingParams.lookbackDays : undefined,
    DEFAULT_RANKING_PARAMS.lookbackDays
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingPeriodDays',
    typeof state.rankingParams.periodDays === 'number' ? state.rankingParams.periodDays : undefined,
    DEFAULT_RANKING_PARAMS.periodDays
  );

  assignIfDefinedAndNotDefault(
    next,
    'fundamentalLimit',
    typeof state.fundamentalRankingParams.limit === 'number' ? state.fundamentalRankingParams.limit : undefined,
    DEFAULT_FUNDAMENTAL_RANKING_PARAMS.limit
  );
  assignIfDefinedAndNotDefault(
    next,
    'fundamentalMarkets',
    normalizeString(state.fundamentalRankingParams.markets),
    DEFAULT_FUNDAMENTAL_RANKING_PARAMS.markets
  );
  assignIfDefinedAndNotDefault(
    next,
    'forecastAboveRecentFyActuals',
    state.fundamentalRankingParams.forecastAboveRecentFyActuals,
    DEFAULT_FUNDAMENTAL_RANKING_PARAMS.forecastAboveRecentFyActuals
  );
  assignIfDefinedAndNotDefault(
    next,
    'forecastLookbackFyCount',
    typeof state.fundamentalRankingParams.forecastLookbackFyCount === 'number'
      ? state.fundamentalRankingParams.forecastLookbackFyCount
      : undefined,
    DEFAULT_FUNDAMENTAL_RANKING_PARAMS.forecastLookbackFyCount
  );

  return next;
}

export function validateBacktestSearch(search: Record<string, unknown>): BacktestRouteSearch {
  const next: BacktestRouteSearch = {};

  const tab = normalizeEnum(search.tab, BACKTEST_SUB_TABS);
  const strategy = normalizeString(search.strategy);
  const resultJobId = normalizeString(search.resultJobId);
  const dataset = normalizeString(search.dataset);
  const labType = normalizeEnum(search.labType, LAB_TYPES);

  if (tab) next.tab = tab;
  if (strategy) next.strategy = strategy;
  if (resultJobId) next.resultJobId = resultJobId;
  if (dataset) next.dataset = dataset;
  if (labType) next.labType = labType;

  return next;
}

export function serializeBacktestSearch(state: {
  activeSubTab: BacktestSubTab;
  selectedStrategy: string | null;
  selectedResultJobId: string | null;
  selectedDatasetName: string | null;
  activeLabType: LabType | null;
}): BacktestRouteSearch {
  const next: BacktestRouteSearch = {};

  if (state.activeSubTab !== 'runner') next.tab = state.activeSubTab;
  if (state.selectedStrategy) next.strategy = state.selectedStrategy;
  if (state.selectedResultJobId) next.resultJobId = state.selectedResultJobId;
  if (state.selectedDatasetName) next.dataset = state.selectedDatasetName;
  if (state.activeLabType) next.labType = state.activeLabType;

  return next;
}

export function readPersistedStoreState(storage: Storage, key: string): Record<string, unknown> | null {
  const raw = storage.getItem(key);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as PersistedContainer | Record<string, unknown>;
    if (isRecord(parsed) && isRecord(parsed.state)) {
      return parsed.state;
    }
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function prunePersistedStoreFields(storage: Storage, key: string, fields: string[]): void {
  const raw = storage.getItem(key);
  if (!raw) return;

  try {
    const parsed = JSON.parse(raw) as PersistedContainer | Record<string, unknown>;
    const container = isRecord(parsed) ? parsed : {};
    const state = isRecord(container.state) ? { ...container.state } : isRecord(parsed) ? { ...parsed } : {};

    for (const field of fields) {
      delete state[field];
    }

    if (isEmptyObject(state)) {
      storage.removeItem(key);
      return;
    }

    const next = isRecord(container.state) ? { ...container, state } : state;
    storage.setItem(key, JSON.stringify(next));
  } catch {
    storage.removeItem(key);
  }
}

export function extractLegacyChartsSearch(state: Record<string, unknown>): ChartsRouteSearch {
  return serializeChartsSearch({
    symbol: typeof state.selectedSymbol === 'string' ? state.selectedSymbol : null,
  });
}

export function extractLegacyPortfolioSearch(state: Record<string, unknown>): PortfolioRouteSearch {
  return serializePortfolioSearch({
    tab: normalizeEnum(state.portfolioSubTab, PORTFOLIO_SUB_TABS) ?? 'portfolios',
    portfolioId: normalizePositiveInt(state.selectedPortfolioId) ?? null,
    watchlistId: normalizePositiveInt(state.selectedWatchlistId) ?? null,
  });
}

export function extractLegacyIndicesSearch(state: Record<string, unknown>): IndicesRouteSearch {
  return serializeIndicesSearch(typeof state.selectedIndexCode === 'string' ? state.selectedIndexCode : null);
}

export function extractLegacyAnalysisSearch(state: Record<string, unknown>): AnalysisRouteSearch {
  const screeningParams = isRecord(state.screeningParams) ? (state.screeningParams as ScreeningParams) : DEFAULT_SCREENING_PARAMS;
  const oracleScreeningParams = isRecord(state.oracleScreeningParams)
    ? (state.oracleScreeningParams as ScreeningParams)
    : DEFAULT_ORACLE_SCREENING_PARAMS;
  const rankingParams = isRecord(state.rankingParams) ? (state.rankingParams as RankingParams) : DEFAULT_RANKING_PARAMS;
  const fundamentalRankingParams = isRecord(state.fundamentalRankingParams)
    ? (state.fundamentalRankingParams as FundamentalRankingParams)
    : DEFAULT_FUNDAMENTAL_RANKING_PARAMS;

  return serializeAnalysisSearch({
    activeSubTab: normalizeEnum(state.activeSubTab, ANALYSIS_SUB_TABS) ?? 'screening',
    screeningParams: { ...DEFAULT_SCREENING_PARAMS, ...screeningParams, mode: 'standard' },
    oracleScreeningParams: { ...DEFAULT_ORACLE_SCREENING_PARAMS, ...oracleScreeningParams, mode: 'oracle' },
    rankingParams: { ...DEFAULT_RANKING_PARAMS, ...rankingParams },
    fundamentalRankingParams: { ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS, ...fundamentalRankingParams },
  });
}

export function extractLegacyBacktestSearch(state: Record<string, unknown>): BacktestRouteSearch {
  return serializeBacktestSearch({
    activeSubTab: normalizeEnum(state.activeSubTab, BACKTEST_SUB_TABS) ?? 'runner',
    selectedStrategy: typeof state.selectedStrategy === 'string' ? state.selectedStrategy : null,
    selectedResultJobId: typeof state.selectedResultJobId === 'string' ? state.selectedResultJobId : null,
    selectedDatasetName: typeof state.selectedDatasetName === 'string' ? state.selectedDatasetName : null,
    activeLabType: normalizeEnum(state.activeLabType, LAB_TYPES) ?? null,
  });
}
