import type { LabType } from '@trading25/api-clients/backtest';
import type {
  Options225PutCallFilter,
  Options225SortBy,
  ScreeningSortBy,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';
import {
  DEFAULT_IN_SESSION_SCREENING_PARAMS,
  DEFAULT_PRE_OPEN_SCREENING_PARAMS,
  DEFAULT_RANKING_PARAMS,
  type ScreeningSubTab,
} from '@/stores/screeningStore';
import type {
  DailyRankingTableFilters,
  DailyRankingValuationSignalFilter,
  RankingDailyView,
  RankingLiquidityState,
  RankingParams,
  RankingRegimeState,
  RankingRiskState,
  RankingSortField,
  RankingSortOrder,
  RankingTechnicalEventType,
  RankingTechnicalState,
  SectorStrengthFamily,
} from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';

export type BacktestSubTab = 'runner' | 'results' | 'attribution' | 'strategies' | 'status' | 'dataset' | 'lab';
export type PortfolioSubTab = 'portfolios' | 'watchlists';

export interface SymbolWorkbenchRouteSearch {
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
  sectorMarkets?: string;
  sectorLookbackDays?: number;
  sectorSortBy?: RankingSortField;
  sectorOrder?: RankingSortOrder;
}

export interface ResearchRouteSearch {
  experimentId?: string;
  runId?: string;
}

export interface Options225RouteSearch {
  date?: string;
  putCall?: Options225PutCallFilter;
  contractMonth?: string;
  strikeMin?: number;
  strikeMax?: number;
  sortBy?: Options225SortBy;
  order?: SortOrder;
}

export interface ScreeningRouteSearch {
  tab?: ScreeningSubTab;
  preOpenMarkets?: string;
  preOpenStrategies?: string;
  preOpenRecentDays?: number;
  preOpenDate?: string;
  preOpenSortBy?: ScreeningSortBy;
  preOpenOrder?: SortOrder;
  preOpenLimit?: number;
  inSessionMarkets?: string;
  inSessionStrategies?: string;
  inSessionRecentDays?: number;
  inSessionDate?: string;
  inSessionSortBy?: ScreeningSortBy;
  inSessionOrder?: SortOrder;
  inSessionLimit?: number;
}

export interface RankingRouteSearch {
  dailyView?: RankingDailyView;
  rankingDate?: string;
  rankingLimit?: number;
  rankingMarkets?: string;
  rankingLookbackDays?: number;
  rankingPeriodDays?: number;
  rankingTechnicalEventType?: RankingTechnicalEventType;
  rankingLiquidityState?: RankingLiquidityState;
  rankingRegimeState?: RankingRegimeState;
  rankingRiskState?: RankingRiskState;
  rankingTechnicalState?: RankingTechnicalState;
  rankingSectorStrengthFamily?: SectorStrengthFamily;
  rankingSortBy?: RankingSortField;
  rankingOrder?: RankingSortOrder;
  rankingForwardEpsDisclosedWithinDays?: number;
  rankingFilterText?: string;
  rankingFilterMarket?: string;
  rankingFilterSector33?: string;
  rankingFilterWatchlistId?: number;
  rankingFilterRegime?: RankingRegimeState;
  rankingFilterSignal?: DailyRankingValuationSignalFilter;
  rankingFilterRisk?: RankingRiskState;
  rankingFilterTechnical?: RankingTechnicalState;
  rankingFilterMinChangePct?: number;
  rankingFilterMaxChangePct?: number;
  rankingFilterMinTradingValue?: number;
  rankingFilterMaxTradingValue?: number;
  rankingFilterMinMarketCap?: number;
  rankingFilterMaxMarketCap?: number;
  rankingFilterMinPer?: number;
  rankingFilterMaxPer?: number;
  rankingFilterMinForwardPer?: number;
  rankingFilterMaxForwardPer?: number;
  rankingFilterMinPsr?: number;
  rankingFilterMaxPsr?: number;
  rankingFilterMinForwardPsr?: number;
  rankingFilterMaxForwardPsr?: number;
  rankingFilterMinPbr?: number;
  rankingFilterMaxPbr?: number;
  rankingFilterMinLiquidityZ?: number;
  rankingFilterMaxLiquidityZ?: number;
  rankingFilterMinSectorScore?: number;
  rankingFilterMaxSectorScore?: number;
}

export interface BacktestRouteSearch {
  tab?: BacktestSubTab;
  strategy?: string;
  resultJobId?: string;
  dataset?: string;
  labType?: LabType;
}

const SCREENING_SUB_TABS: ScreeningSubTab[] = ['preOpenScreening', 'inSessionScreening'];
const RANKING_DAILY_VIEWS: RankingDailyView[] = ['stocks', 'technicalEvents', 'indices'];
const RANKING_TECHNICAL_EVENT_TYPES: RankingTechnicalEventType[] = ['periodHigh', 'periodLow'];
const RANKING_LIQUIDITY_STATE_VALUES: RankingLiquidityState[] = [
  'neutral_rerating',
  'crowded_rerating',
  'distribution_stress',
  'stale_liquidity',
  'neutral',
  'overheat',
  'stale_rally_fade',
];
const RANKING_REGIME_STATE_VALUES: RankingRegimeState[] = [
  'neutral_rerating',
  'neutral_rerating_good',
  'crowded_rerating',
  'crowded_rerating_good',
  'distribution_stress',
  'stale_liquidity',
  'neutral',
];
const RANKING_RISK_STATE_VALUES: RankingRiskState[] = ['overheat', 'stale_rally_fade'];
const RANKING_TECHNICAL_STATE_VALUES: RankingTechnicalState[] = ['atr20_acceleration', 'momentum_20_60_top20'];
const SECTOR_STRENGTH_FAMILY_VALUES: SectorStrengthFamily[] = ['balanced_sector_strength', 'long_hybrid_leadership'];
const DAILY_RANKING_VALUATION_SIGNAL_FILTER_VALUES: DailyRankingValuationSignalFilter[] = [
  'deep_value',
  'undervalued',
  'overvalued',
  'very_overvalued',
  'no_earnings',
];
const RANKING_ROUTE_SEARCH_KEYS: (keyof RankingRouteSearch)[] = [
  'dailyView',
  'rankingDate',
  'rankingLimit',
  'rankingMarkets',
  'rankingLookbackDays',
  'rankingPeriodDays',
  'rankingTechnicalEventType',
  'rankingLiquidityState',
  'rankingRegimeState',
  'rankingRiskState',
  'rankingTechnicalState',
  'rankingSectorStrengthFamily',
  'rankingSortBy',
  'rankingOrder',
  'rankingForwardEpsDisclosedWithinDays',
  'rankingFilterText',
  'rankingFilterMarket',
  'rankingFilterSector33',
  'rankingFilterWatchlistId',
  'rankingFilterRegime',
  'rankingFilterSignal',
  'rankingFilterRisk',
  'rankingFilterTechnical',
  'rankingFilterMinChangePct',
  'rankingFilterMaxChangePct',
  'rankingFilterMinTradingValue',
  'rankingFilterMaxTradingValue',
  'rankingFilterMinMarketCap',
  'rankingFilterMaxMarketCap',
  'rankingFilterMinPer',
  'rankingFilterMaxPer',
  'rankingFilterMinForwardPer',
  'rankingFilterMaxForwardPer',
  'rankingFilterMinPsr',
  'rankingFilterMaxPsr',
  'rankingFilterMinForwardPsr',
  'rankingFilterMaxForwardPsr',
  'rankingFilterMinPbr',
  'rankingFilterMaxPbr',
  'rankingFilterMinLiquidityZ',
  'rankingFilterMaxLiquidityZ',
  'rankingFilterMinSectorScore',
  'rankingFilterMaxSectorScore',
];
const INDICES_ROUTE_SEARCH_KEYS: (keyof IndicesRouteSearch)[] = [
  'code',
  'sectorMarkets',
  'sectorLookbackDays',
  'sectorSortBy',
  'sectorOrder',
];
const RANKING_SORT_VALUES: RankingSortField[] = [
  'tradingValue',
  'changePercentage',
  'code',
  'currentPrice',
  'sectorStrengthScore',
  'per',
  'forwardPer',
  'forwardPOp',
  'psr',
  'forwardPsr',
  'pbr',
  'marketCap',
  'liquidityResidualZ',
  'adv60ToFreeFloatPct',
];
const PORTFOLIO_SUB_TABS: PortfolioSubTab[] = ['portfolios', 'watchlists'];
const BACKTEST_SUB_TABS: BacktestSubTab[] = [
  'runner',
  'results',
  'attribution',
  'strategies',
  'status',
  'dataset',
  'lab',
];
const LAB_TYPES: LabType[] = ['generate', 'evolve', 'optimize', 'improve'];
const OPTIONS_225_PUT_CALL_VALUES: Options225PutCallFilter[] = ['all', 'put', 'call'];
const OPTIONS_225_SORT_VALUES: Options225SortBy[] = [
  'openInterest',
  'volume',
  'strikePrice',
  'impliedVolatility',
  'wholeDayClose',
];
const SCREENING_SORT_VALUES: ScreeningSortBy[] = [
  'bestStrategyScore',
  'matchedDate',
  'stockCode',
  'matchStrategyCount',
];
const SORT_ORDER_VALUES: SortOrder[] = ['asc', 'desc'];
const RANKING_SORT_ORDER_VALUES: RankingSortOrder[] = ['asc', 'desc'];
export const DEFAULT_INDICES_SECTOR_MARKETS = 'prime';
export const DEFAULT_INDICES_SECTOR_LOOKBACK_DAYS = 5;
export const DEFAULT_INDICES_SECTOR_SORT_BY: RankingSortField = 'tradingValue';
export const DEFAULT_INDICES_SECTOR_ORDER: RankingSortOrder = 'desc';

function normalizeString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined;
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

function normalizeNonNegativeInt(value: unknown): number | undefined {
  if (value === 0) {
    return 0;
  }
  if (typeof value === 'string' && value.trim() === '0') {
    return 0;
  }
  return normalizePositiveInt(value);
}

function normalizeFiniteNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function normalizeEnum<T extends string>(value: unknown, values: readonly T[]): T | undefined {
  return typeof value === 'string' && values.includes(value as T) ? (value as T) : undefined;
}

function normalizeScreeningSubTab(value: unknown): ScreeningSubTab | undefined {
  return normalizeEnum(normalizeString(value), SCREENING_SUB_TABS);
}

function normalizeRankingDailyView(value: unknown): RankingDailyView | undefined {
  return normalizeEnum(normalizeString(value), RANKING_DAILY_VIEWS);
}

function normalizeRankingTechnicalEventType(value: unknown): RankingTechnicalEventType | undefined {
  return normalizeEnum(normalizeString(value), RANKING_TECHNICAL_EVENT_TYPES);
}

function normalizeRankingLiquidityState(value: unknown): RankingLiquidityState | undefined {
  return normalizeEnum(normalizeString(value), RANKING_LIQUIDITY_STATE_VALUES);
}

function normalizeRankingRegimeState(value: unknown): RankingRegimeState | undefined {
  return normalizeEnum(normalizeString(value), RANKING_REGIME_STATE_VALUES);
}

function normalizeRankingRiskState(value: unknown): RankingRiskState | undefined {
  return normalizeEnum(normalizeString(value), RANKING_RISK_STATE_VALUES);
}

function normalizeRankingTechnicalState(value: unknown): RankingTechnicalState | undefined {
  return normalizeEnum(normalizeString(value), RANKING_TECHNICAL_STATE_VALUES);
}

function normalizeSectorStrengthFamily(value: unknown): SectorStrengthFamily | undefined {
  return normalizeEnum(normalizeString(value), SECTOR_STRENGTH_FAMILY_VALUES);
}

function normalizeDailyRankingValuationSignalFilter(value: unknown): DailyRankingValuationSignalFilter | undefined {
  return normalizeEnum(normalizeString(value), DAILY_RANKING_VALUATION_SIGNAL_FILTER_VALUES);
}

function normalizeRankingSortField(value: unknown): RankingSortField | undefined {
  return normalizeEnum(normalizeString(value), RANKING_SORT_VALUES);
}

function normalizeRankingSortOrder(value: unknown): RankingSortOrder | undefined {
  return normalizeEnum(normalizeString(value), RANKING_SORT_ORDER_VALUES);
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

function assignSearchParams<T extends object>(base: T, entries: Array<[keyof T, T[keyof T] | undefined]>): T {
  const next = { ...base };

  for (const [key, value] of entries) {
    assignIfDefined(next, key, value);
  }

  return next;
}

export function validateSymbolWorkbenchSearch(search: Record<string, unknown>): SymbolWorkbenchRouteSearch {
  const symbol = normalizeString(search.symbol);
  const strategy = normalizeString(search.strategy);
  const matchedDate = normalizeString(search.matchedDate);
  const next: SymbolWorkbenchRouteSearch = {};
  if (symbol) next.symbol = symbol;
  if (strategy) next.strategy = strategy;
  if (matchedDate) next.matchedDate = matchedDate;
  return next;
}

export function serializeSymbolWorkbenchSearch(params: {
  symbol?: string | null;
  strategy?: string | null;
  matchedDate?: string | null;
}): SymbolWorkbenchRouteSearch {
  const normalizedSymbol = normalizeString(params.symbol);
  const normalizedStrategy = normalizeString(params.strategy);
  const normalizedMatchedDate = normalizeString(params.matchedDate);
  const next: SymbolWorkbenchRouteSearch = {};
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
  const next: IndicesRouteSearch = {};
  assignIfDefined(next, 'code', normalizeString(search.code));
  assignIfDefined(next, 'sectorMarkets', normalizeString(search.sectorMarkets));
  assignIfDefined(next, 'sectorLookbackDays', normalizePositiveInt(search.sectorLookbackDays));
  assignIfDefined(next, 'sectorSortBy', normalizeRankingSortField(search.sectorSortBy));
  assignIfDefined(next, 'sectorOrder', normalizeRankingSortOrder(search.sectorOrder));
  return next;
}

export function validateResearchSearch(search: Record<string, unknown>): ResearchRouteSearch {
  const experimentId = normalizeString(search.experimentId);
  const runId = normalizeString(search.runId);
  const next: ResearchRouteSearch = {};
  if (experimentId) {
    next.experimentId = experimentId;
  }
  if (runId) {
    next.runId = runId;
  }
  return next;
}

export function getIndicesStateFromSearch(search: IndicesRouteSearch): {
  selectedIndexCode: string | null;
  sectorMarkets: string;
  sectorLookbackDays: number;
  sectorSortBy: RankingSortField;
  sectorOrder: RankingSortOrder;
} {
  return {
    selectedIndexCode: search.code ?? null,
    sectorMarkets: search.sectorMarkets ?? DEFAULT_INDICES_SECTOR_MARKETS,
    sectorLookbackDays: search.sectorLookbackDays ?? DEFAULT_INDICES_SECTOR_LOOKBACK_DAYS,
    sectorSortBy: search.sectorSortBy ?? DEFAULT_INDICES_SECTOR_SORT_BY,
    sectorOrder: search.sectorOrder ?? DEFAULT_INDICES_SECTOR_ORDER,
  };
}

export function serializeIndicesSearch(
  state:
    | string
    | null
    | undefined
    | {
        selectedIndexCode?: string | null;
        sectorMarkets?: string;
        sectorLookbackDays?: number;
        sectorSortBy?: RankingSortField;
        sectorOrder?: RankingSortOrder;
      }
): IndicesRouteSearch {
  const normalizedState = typeof state === 'string' || state == null ? { selectedIndexCode: state } : state;
  const next: IndicesRouteSearch = {};
  assignIfDefined(next, 'code', normalizeString(normalizedState.selectedIndexCode));
  assignIfDefinedAndNotDefault(
    next,
    'sectorMarkets',
    normalizeString(normalizedState.sectorMarkets),
    DEFAULT_INDICES_SECTOR_MARKETS
  );
  assignIfDefinedAndNotDefault(
    next,
    'sectorLookbackDays',
    typeof normalizedState.sectorLookbackDays === 'number' ? normalizedState.sectorLookbackDays : undefined,
    DEFAULT_INDICES_SECTOR_LOOKBACK_DAYS
  );
  assignIfDefinedAndNotDefault(next, 'sectorSortBy', normalizedState.sectorSortBy, DEFAULT_INDICES_SECTOR_SORT_BY);
  assignIfDefinedAndNotDefault(next, 'sectorOrder', normalizedState.sectorOrder, DEFAULT_INDICES_SECTOR_ORDER);
  return next;
}

export function serializeIndicesSearchForNavigation(
  currentSearch: IndicesRouteSearch | Record<string, unknown>,
  state: Parameters<typeof serializeIndicesSearch>[0]
): IndicesRouteSearch {
  const current = validateIndicesSearch(currentSearch as Record<string, unknown>);
  const next = serializeIndicesSearch(state);

  for (const key of INDICES_ROUTE_SEARCH_KEYS) {
    if (key in current && !(key in next)) {
      next[key] = undefined;
    }
  }

  return next;
}

export function serializeResearchSearch(params: {
  experimentId?: string | null;
  runId?: string | null;
}): ResearchRouteSearch {
  const experimentId = normalizeString(params.experimentId);
  const runId = normalizeString(params.runId);
  const next: ResearchRouteSearch = {};
  if (experimentId) {
    next.experimentId = experimentId;
  }
  if (runId) {
    next.runId = runId;
  }
  return next;
}

export function validateOptions225Search(search: Record<string, unknown>): Options225RouteSearch {
  const next: Options225RouteSearch = {};
  assignIfDefined(next, 'date', normalizeString(search.date));
  assignIfDefined(next, 'putCall', normalizeEnum(search.putCall, OPTIONS_225_PUT_CALL_VALUES));
  assignIfDefined(next, 'contractMonth', normalizeString(search.contractMonth));
  assignIfDefined(next, 'strikeMin', normalizeFiniteNumber(search.strikeMin));
  assignIfDefined(next, 'strikeMax', normalizeFiniteNumber(search.strikeMax));
  assignIfDefined(next, 'sortBy', normalizeEnum(search.sortBy, OPTIONS_225_SORT_VALUES));
  assignIfDefined(next, 'order', normalizeEnum(search.order, SORT_ORDER_VALUES));
  return next;
}

export function serializeOptions225Search(state: {
  date: string | null;
  putCall: Options225PutCallFilter;
  contractMonth: string | null;
  strikeMin: number | null;
  strikeMax: number | null;
  sortBy: Options225SortBy;
  order: SortOrder;
}): Options225RouteSearch {
  const next: Options225RouteSearch = {};

  assignIfDefined(next, 'date', normalizeString(state.date));
  assignIfDefinedAndNotDefault(next, 'putCall', state.putCall, 'all');
  assignIfDefined(next, 'contractMonth', normalizeString(state.contractMonth));
  if (typeof state.strikeMin === 'number' && Number.isFinite(state.strikeMin)) {
    next.strikeMin = state.strikeMin;
  }
  if (typeof state.strikeMax === 'number' && Number.isFinite(state.strikeMax)) {
    next.strikeMax = state.strikeMax;
  }
  assignIfDefinedAndNotDefault(next, 'sortBy', state.sortBy, 'openInterest');
  assignIfDefinedAndNotDefault(next, 'order', state.order, 'desc');

  return next;
}

export function validateScreeningSearch(search: Record<string, unknown>): ScreeningRouteSearch {
  const next: ScreeningRouteSearch = {};
  const normalizedTab = normalizeScreeningSubTab(search.tab);

  assignIfDefined(next, 'tab', normalizedTab);
  assignIfDefined(next, 'preOpenMarkets', normalizeString(search.preOpenMarkets));
  assignIfDefined(next, 'preOpenStrategies', normalizeString(search.preOpenStrategies));
  assignIfDefined(next, 'preOpenRecentDays', normalizePositiveInt(search.preOpenRecentDays));
  assignIfDefined(next, 'preOpenDate', normalizeString(search.preOpenDate));
  assignIfDefined(next, 'preOpenSortBy', normalizeEnum(search.preOpenSortBy, SCREENING_SORT_VALUES));
  assignIfDefined(next, 'preOpenOrder', normalizeEnum(search.preOpenOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'preOpenLimit', normalizePositiveInt(search.preOpenLimit));
  assignIfDefined(next, 'inSessionMarkets', normalizeString(search.inSessionMarkets));
  assignIfDefined(next, 'inSessionStrategies', normalizeString(search.inSessionStrategies));
  assignIfDefined(next, 'inSessionRecentDays', normalizePositiveInt(search.inSessionRecentDays));
  assignIfDefined(next, 'inSessionDate', normalizeString(search.inSessionDate));
  assignIfDefined(next, 'inSessionSortBy', normalizeEnum(search.inSessionSortBy, SCREENING_SORT_VALUES));
  assignIfDefined(next, 'inSessionOrder', normalizeEnum(search.inSessionOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'inSessionLimit', normalizePositiveInt(search.inSessionLimit));
  return next;
}

export function getScreeningStateFromSearch(search: ScreeningRouteSearch): {
  activeSubTab: ScreeningSubTab;
  preOpenScreeningParams: ScreeningParams;
  inSessionScreeningParams: ScreeningParams;
} {
  return {
    activeSubTab: normalizeScreeningSubTab(search.tab) ?? 'preOpenScreening',
    preOpenScreeningParams: assignSearchParams({ ...DEFAULT_PRE_OPEN_SCREENING_PARAMS }, [
      ['markets', search.preOpenMarkets],
      ['strategies', search.preOpenStrategies],
      ['recentDays', search.preOpenRecentDays],
      ['date', search.preOpenDate],
      ['sortBy', search.preOpenSortBy],
      ['order', search.preOpenOrder],
      ['limit', search.preOpenLimit],
    ]),
    inSessionScreeningParams: assignSearchParams({ ...DEFAULT_IN_SESSION_SCREENING_PARAMS }, [
      ['markets', search.inSessionMarkets],
      ['strategies', search.inSessionStrategies],
      ['recentDays', search.inSessionRecentDays],
      ['date', search.inSessionDate],
      ['sortBy', search.inSessionSortBy],
      ['order', search.inSessionOrder],
      ['limit', search.inSessionLimit],
    ]),
  };
}

export function getRankingStateFromSearch(search: RankingRouteSearch): {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  rankingTableFilters: DailyRankingTableFilters;
} {
  const legacyRegimeState = normalizeRankingRegimeState(search.rankingLiquidityState);
  const legacyRiskState = normalizeRankingRiskState(search.rankingLiquidityState);
  const rankingParams = assignSearchParams({ ...DEFAULT_RANKING_PARAMS }, [
    ['date', search.rankingDate],
    ['limit', search.rankingLimit],
    ['markets', search.rankingMarkets],
    ['lookbackDays', search.rankingLookbackDays],
    ['periodDays', search.rankingPeriodDays],
    ['technicalEventType', search.rankingTechnicalEventType],
    ['regimeState', search.rankingRegimeState ?? legacyRegimeState],
    ['riskState', search.rankingRiskState ?? legacyRiskState],
    ['technicalState', search.rankingTechnicalState],
    ['sectorStrengthFamily', search.rankingSectorStrengthFamily],
    ['sortBy', search.rankingSortBy],
    ['order', search.rankingOrder],
    ['forwardEpsDisclosedWithinDays', search.rankingForwardEpsDisclosedWithinDays],
  ]);
  const rankingTableFilters = assignSearchParams<DailyRankingTableFilters>({}, [
    ['text', search.rankingFilterText],
    ['market', search.rankingFilterMarket],
    ['sector33Name', search.rankingFilterSector33],
    ['watchlistId', search.rankingFilterWatchlistId],
    ['regimeState', search.rankingFilterRegime],
    ['valuationSignal', search.rankingFilterSignal],
    ['riskState', search.rankingFilterRisk],
    ['technicalState', search.rankingFilterTechnical],
    ['minChangePct', search.rankingFilterMinChangePct],
    ['maxChangePct', search.rankingFilterMaxChangePct],
    ['minTradingValue', search.rankingFilterMinTradingValue],
    ['maxTradingValue', search.rankingFilterMaxTradingValue],
    ['minMarketCap', search.rankingFilterMinMarketCap],
    ['maxMarketCap', search.rankingFilterMaxMarketCap],
    ['minPer', search.rankingFilterMinPer],
    ['maxPer', search.rankingFilterMaxPer],
    ['minForwardPer', search.rankingFilterMinForwardPer],
    ['maxForwardPer', search.rankingFilterMaxForwardPer],
    ['minPsr', search.rankingFilterMinPsr],
    ['maxPsr', search.rankingFilterMaxPsr],
    ['minForwardPsr', search.rankingFilterMinForwardPsr],
    ['maxForwardPsr', search.rankingFilterMaxForwardPsr],
    ['minPbr', search.rankingFilterMinPbr],
    ['maxPbr', search.rankingFilterMaxPbr],
    ['minLiquidityZ', search.rankingFilterMinLiquidityZ],
    ['maxLiquidityZ', search.rankingFilterMaxLiquidityZ],
    ['minSectorScore', search.rankingFilterMinSectorScore],
    ['maxSectorScore', search.rankingFilterMaxSectorScore],
  ]);

  return {
    activeDailyView: search.dailyView ?? 'stocks',
    rankingParams,
    rankingTableFilters,
  };
}

export function serializeScreeningSearch(state: {
  activeSubTab: ScreeningSubTab;
  preOpenScreeningParams: ScreeningParams;
  inSessionScreeningParams: ScreeningParams;
}): ScreeningRouteSearch {
  const next: ScreeningRouteSearch = {};

  assignIfDefinedAndNotDefault(next, 'tab', state.activeSubTab, 'preOpenScreening');
  assignIfDefinedAndNotDefault(
    next,
    'preOpenMarkets',
    normalizeString(state.preOpenScreeningParams.markets),
    DEFAULT_PRE_OPEN_SCREENING_PARAMS.markets
  );
  assignIfDefined(next, 'preOpenStrategies', normalizeString(state.preOpenScreeningParams.strategies));
  assignIfDefinedAndNotDefault(
    next,
    'preOpenRecentDays',
    typeof state.preOpenScreeningParams.recentDays === 'number' ? state.preOpenScreeningParams.recentDays : undefined,
    DEFAULT_PRE_OPEN_SCREENING_PARAMS.recentDays
  );
  assignIfDefined(next, 'preOpenDate', normalizeString(state.preOpenScreeningParams.date));
  assignIfDefinedAndNotDefault(
    next,
    'preOpenSortBy',
    state.preOpenScreeningParams.sortBy,
    DEFAULT_PRE_OPEN_SCREENING_PARAMS.sortBy
  );
  assignIfDefinedAndNotDefault(
    next,
    'preOpenOrder',
    state.preOpenScreeningParams.order,
    DEFAULT_PRE_OPEN_SCREENING_PARAMS.order
  );
  assignIfDefinedAndNotDefault(
    next,
    'preOpenLimit',
    typeof state.preOpenScreeningParams.limit === 'number' ? state.preOpenScreeningParams.limit : undefined,
    DEFAULT_PRE_OPEN_SCREENING_PARAMS.limit
  );

  assignIfDefinedAndNotDefault(
    next,
    'inSessionMarkets',
    normalizeString(state.inSessionScreeningParams.markets),
    DEFAULT_IN_SESSION_SCREENING_PARAMS.markets
  );
  assignIfDefined(next, 'inSessionStrategies', normalizeString(state.inSessionScreeningParams.strategies));
  assignIfDefinedAndNotDefault(
    next,
    'inSessionRecentDays',
    typeof state.inSessionScreeningParams.recentDays === 'number'
      ? state.inSessionScreeningParams.recentDays
      : undefined,
    DEFAULT_IN_SESSION_SCREENING_PARAMS.recentDays
  );
  assignIfDefined(next, 'inSessionDate', normalizeString(state.inSessionScreeningParams.date));
  assignIfDefinedAndNotDefault(
    next,
    'inSessionSortBy',
    state.inSessionScreeningParams.sortBy,
    DEFAULT_IN_SESSION_SCREENING_PARAMS.sortBy
  );
  assignIfDefinedAndNotDefault(
    next,
    'inSessionOrder',
    state.inSessionScreeningParams.order,
    DEFAULT_IN_SESSION_SCREENING_PARAMS.order
  );
  assignIfDefinedAndNotDefault(
    next,
    'inSessionLimit',
    typeof state.inSessionScreeningParams.limit === 'number' ? state.inSessionScreeningParams.limit : undefined,
    DEFAULT_IN_SESSION_SCREENING_PARAMS.limit
  );

  return next;
}

export function validateRankingSearch(search: Record<string, unknown>): RankingRouteSearch {
  const next: RankingRouteSearch = {};

  assignIfDefined(next, 'dailyView', normalizeRankingDailyView(search.dailyView));
  assignIfDefined(next, 'rankingDate', normalizeString(search.rankingDate));
  assignIfDefined(next, 'rankingLimit', normalizePositiveInt(search.rankingLimit));
  assignIfDefined(next, 'rankingMarkets', normalizeString(search.rankingMarkets));
  assignIfDefined(next, 'rankingLookbackDays', normalizePositiveInt(search.rankingLookbackDays));
  assignIfDefined(next, 'rankingPeriodDays', normalizePositiveInt(search.rankingPeriodDays));
  assignIfDefined(
    next,
    'rankingTechnicalEventType',
    normalizeRankingTechnicalEventType(search.rankingTechnicalEventType)
  );
  assignIfDefined(next, 'rankingLiquidityState', normalizeRankingLiquidityState(search.rankingLiquidityState));
  assignIfDefined(next, 'rankingRegimeState', normalizeRankingRegimeState(search.rankingRegimeState));
  assignIfDefined(next, 'rankingRiskState', normalizeRankingRiskState(search.rankingRiskState));
  assignIfDefined(next, 'rankingTechnicalState', normalizeRankingTechnicalState(search.rankingTechnicalState));
  assignIfDefined(
    next,
    'rankingSectorStrengthFamily',
    normalizeSectorStrengthFamily(search.rankingSectorStrengthFamily)
  );
  assignIfDefined(next, 'rankingSortBy', normalizeRankingSortField(search.rankingSortBy));
  assignIfDefined(next, 'rankingOrder', normalizeRankingSortOrder(search.rankingOrder));
  assignIfDefined(
    next,
    'rankingForwardEpsDisclosedWithinDays',
    normalizeNonNegativeInt(search.rankingForwardEpsDisclosedWithinDays)
  );
  assignIfDefined(next, 'rankingFilterText', normalizeString(search.rankingFilterText));
  assignIfDefined(next, 'rankingFilterMarket', normalizeString(search.rankingFilterMarket));
  assignIfDefined(next, 'rankingFilterSector33', normalizeString(search.rankingFilterSector33));
  assignIfDefined(next, 'rankingFilterWatchlistId', normalizePositiveInt(search.rankingFilterWatchlistId));
  assignIfDefined(next, 'rankingFilterRegime', normalizeRankingRegimeState(search.rankingFilterRegime));
  assignIfDefined(next, 'rankingFilterSignal', normalizeDailyRankingValuationSignalFilter(search.rankingFilterSignal));
  assignIfDefined(next, 'rankingFilterRisk', normalizeRankingRiskState(search.rankingFilterRisk));
  assignIfDefined(next, 'rankingFilterTechnical', normalizeRankingTechnicalState(search.rankingFilterTechnical));
  assignIfDefined(next, 'rankingFilterMinChangePct', normalizeFiniteNumber(search.rankingFilterMinChangePct));
  assignIfDefined(next, 'rankingFilterMaxChangePct', normalizeFiniteNumber(search.rankingFilterMaxChangePct));
  assignIfDefined(next, 'rankingFilterMinTradingValue', normalizeFiniteNumber(search.rankingFilterMinTradingValue));
  assignIfDefined(next, 'rankingFilterMaxTradingValue', normalizeFiniteNumber(search.rankingFilterMaxTradingValue));
  assignIfDefined(next, 'rankingFilterMinMarketCap', normalizeFiniteNumber(search.rankingFilterMinMarketCap));
  assignIfDefined(next, 'rankingFilterMaxMarketCap', normalizeFiniteNumber(search.rankingFilterMaxMarketCap));
  assignIfDefined(next, 'rankingFilterMinPer', normalizeFiniteNumber(search.rankingFilterMinPer));
  assignIfDefined(next, 'rankingFilterMaxPer', normalizeFiniteNumber(search.rankingFilterMaxPer));
  assignIfDefined(next, 'rankingFilterMinForwardPer', normalizeFiniteNumber(search.rankingFilterMinForwardPer));
  assignIfDefined(next, 'rankingFilterMaxForwardPer', normalizeFiniteNumber(search.rankingFilterMaxForwardPer));
  assignIfDefined(next, 'rankingFilterMinPsr', normalizeFiniteNumber(search.rankingFilterMinPsr));
  assignIfDefined(next, 'rankingFilterMaxPsr', normalizeFiniteNumber(search.rankingFilterMaxPsr));
  assignIfDefined(next, 'rankingFilterMinForwardPsr', normalizeFiniteNumber(search.rankingFilterMinForwardPsr));
  assignIfDefined(next, 'rankingFilterMaxForwardPsr', normalizeFiniteNumber(search.rankingFilterMaxForwardPsr));
  assignIfDefined(next, 'rankingFilterMinPbr', normalizeFiniteNumber(search.rankingFilterMinPbr));
  assignIfDefined(next, 'rankingFilterMaxPbr', normalizeFiniteNumber(search.rankingFilterMaxPbr));
  assignIfDefined(next, 'rankingFilterMinLiquidityZ', normalizeFiniteNumber(search.rankingFilterMinLiquidityZ));
  assignIfDefined(next, 'rankingFilterMaxLiquidityZ', normalizeFiniteNumber(search.rankingFilterMaxLiquidityZ));
  assignIfDefined(next, 'rankingFilterMinSectorScore', normalizeFiniteNumber(search.rankingFilterMinSectorScore));
  assignIfDefined(next, 'rankingFilterMaxSectorScore', normalizeFiniteNumber(search.rankingFilterMaxSectorScore));
  return next;
}

export function serializeRankingSearch(state: {
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  rankingTableFilters?: DailyRankingTableFilters;
}): RankingRouteSearch {
  const next: RankingRouteSearch = {};

  assignIfDefinedAndNotDefault(next, 'dailyView', state.activeDailyView, 'stocks');
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
    'rankingTechnicalEventType',
    state.rankingParams.technicalEventType,
    DEFAULT_RANKING_PARAMS.technicalEventType
  );
  assignIfDefined(next, 'rankingLiquidityState', state.rankingParams.liquidityState);
  assignIfDefined(next, 'rankingRegimeState', state.rankingParams.regimeState);
  assignIfDefined(next, 'rankingRiskState', state.rankingParams.riskState);
  assignIfDefined(next, 'rankingTechnicalState', state.rankingParams.technicalState);
  assignIfDefinedAndNotDefault(
    next,
    'rankingSectorStrengthFamily',
    state.rankingParams.sectorStrengthFamily,
    DEFAULT_RANKING_PARAMS.sectorStrengthFamily
  );
  assignIfDefinedAndNotDefault(next, 'rankingSortBy', state.rankingParams.sortBy, DEFAULT_RANKING_PARAMS.sortBy);
  assignIfDefinedAndNotDefault(next, 'rankingOrder', state.rankingParams.order, DEFAULT_RANKING_PARAMS.order);
  assignIfDefinedAndNotDefault(
    next,
    'rankingForwardEpsDisclosedWithinDays',
    typeof state.rankingParams.forwardEpsDisclosedWithinDays === 'number'
      ? state.rankingParams.forwardEpsDisclosedWithinDays
      : undefined,
    DEFAULT_RANKING_PARAMS.forwardEpsDisclosedWithinDays
  );
  const filters = state.rankingTableFilters ?? {};
  assignIfDefined(next, 'rankingFilterText', normalizeString(filters.text));
  assignIfDefined(next, 'rankingFilterMarket', normalizeString(filters.market));
  assignIfDefined(next, 'rankingFilterSector33', normalizeString(filters.sector33Name));
  assignIfDefined(next, 'rankingFilterWatchlistId', normalizePositiveInt(filters.watchlistId));
  assignIfDefined(next, 'rankingFilterRegime', filters.regimeState);
  assignIfDefined(next, 'rankingFilterSignal', filters.valuationSignal);
  assignIfDefined(next, 'rankingFilterRisk', filters.riskState);
  assignIfDefined(next, 'rankingFilterTechnical', filters.technicalState);
  assignIfDefined(next, 'rankingFilterMinChangePct', filters.minChangePct);
  assignIfDefined(next, 'rankingFilterMaxChangePct', filters.maxChangePct);
  assignIfDefined(next, 'rankingFilterMinTradingValue', filters.minTradingValue);
  assignIfDefined(next, 'rankingFilterMaxTradingValue', filters.maxTradingValue);
  assignIfDefined(next, 'rankingFilterMinMarketCap', filters.minMarketCap);
  assignIfDefined(next, 'rankingFilterMaxMarketCap', filters.maxMarketCap);
  assignIfDefined(next, 'rankingFilterMinPer', filters.minPer);
  assignIfDefined(next, 'rankingFilterMaxPer', filters.maxPer);
  assignIfDefined(next, 'rankingFilterMinForwardPer', filters.minForwardPer);
  assignIfDefined(next, 'rankingFilterMaxForwardPer', filters.maxForwardPer);
  assignIfDefined(next, 'rankingFilterMinPsr', filters.minPsr);
  assignIfDefined(next, 'rankingFilterMaxPsr', filters.maxPsr);
  assignIfDefined(next, 'rankingFilterMinForwardPsr', filters.minForwardPsr);
  assignIfDefined(next, 'rankingFilterMaxForwardPsr', filters.maxForwardPsr);
  assignIfDefined(next, 'rankingFilterMinPbr', filters.minPbr);
  assignIfDefined(next, 'rankingFilterMaxPbr', filters.maxPbr);
  assignIfDefined(next, 'rankingFilterMinLiquidityZ', filters.minLiquidityZ);
  assignIfDefined(next, 'rankingFilterMaxLiquidityZ', filters.maxLiquidityZ);
  assignIfDefined(next, 'rankingFilterMinSectorScore', filters.minSectorScore);
  assignIfDefined(next, 'rankingFilterMaxSectorScore', filters.maxSectorScore);

  return next;
}

export function serializeRankingSearchForNavigation(
  currentSearch: Record<string, unknown>,
  state: {
    activeDailyView: RankingDailyView;
    rankingParams: RankingParams;
    rankingTableFilters?: DailyRankingTableFilters;
  }
): RankingRouteSearch {
  const current = validateRankingSearch(currentSearch);
  const next = serializeRankingSearch(state);

  for (const key of RANKING_ROUTE_SEARCH_KEYS) {
    if (key in current && !(key in next)) {
      next[key] = undefined;
    }
  }

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
