import type {
  Options225PutCallFilter,
  Options225SortBy,
  ScreeningSortBy,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';
import {
  DEFAULT_FUNDAMENTAL_RANKING_PARAMS,
  DEFAULT_IN_SESSION_SCREENING_PARAMS,
  DEFAULT_PRE_OPEN_SCREENING_PARAMS,
  DEFAULT_RANKING_PARAMS,
  DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS,
  type ScreeningSubTab,
} from '@/stores/screeningStore';
import type { BacktestSubTab, LabType } from '@/types/backtest';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type {
  RankingDailyView,
  RankingPageTab,
  RankingParams,
  Topix100PriceBucketFilter,
  Topix100PriceSmaWindow,
  Topix100RankingMetric,
  Topix100RankingSortKey,
  Topix100StudyMode,
} from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';
import type {
  ValueCompositeForwardEpsMode,
  ValueCompositeRankingParams,
  ValueCompositeScoreMethod,
} from '@/types/valueCompositeRanking';

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
  rankingDate?: string;
  rankingLimit?: number;
  rankingMarkets?: string;
  rankingLookbackDays?: number;
  rankingPeriodDays?: number;
  rankingTopix100StudyMode?: Topix100StudyMode;
  rankingTopix100Metric?: Topix100RankingMetric;
  rankingTopix100SmaWindow?: Topix100PriceSmaWindow;
  rankingTopix100PriceBucket?: Topix100PriceBucketFilter;
  rankingTopix100SortBy?: Topix100RankingSortKey;
  rankingTopix100SortOrder?: SortOrder;
  fundamentalLimit?: number;
  fundamentalMarkets?: string;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
}

export interface RankingRouteSearch {
  tab?: RankingPageTab;
  dailyView?: RankingDailyView;
  rankingDate?: string;
  rankingLimit?: number;
  rankingMarkets?: string;
  rankingLookbackDays?: number;
  rankingPeriodDays?: number;
  rankingTopix100StudyMode?: Topix100StudyMode;
  rankingTopix100Metric?: Topix100RankingMetric;
  rankingTopix100SmaWindow?: Topix100PriceSmaWindow;
  rankingTopix100PriceBucket?: Topix100PriceBucketFilter;
  rankingTopix100SortBy?: Topix100RankingSortKey;
  rankingTopix100SortOrder?: SortOrder;
  fundamentalLimit?: number;
  fundamentalMarkets?: string;
  forecastAboveRecentFyActuals?: boolean;
  forecastLookbackFyCount?: number;
  valueDate?: string;
  valueLimit?: number;
  valueMarkets?: string;
  valueScoreMethod?: ValueCompositeScoreMethod;
  valueForwardEpsMode?: ValueCompositeForwardEpsMode;
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

const SCREENING_SUB_TABS: ScreeningSubTab[] = [
  'preOpenScreening',
  'inSessionScreening',
  'ranking',
  'fundamentalRanking',
];
const RANKING_PAGE_TABS: RankingPageTab[] = ['ranking', 'fundamentalRanking', 'valueComposite'];
const VALUE_COMPOSITE_SCORE_METHOD_VALUES: ValueCompositeScoreMethod[] = [
  'equal_weight',
  'walkforward_regression_weight',
];
const VALUE_COMPOSITE_FORWARD_EPS_MODE_VALUES: ValueCompositeForwardEpsMode[] = ['latest', 'fy'];
const RANKING_DAILY_VIEWS: RankingDailyView[] = ['stocks', 'indices', 'topix100'];
const LEGACY_TOPIX100_RANKING_METRIC = 'price_vs_sma20_gap';
const TOPIX100_STUDY_MODE_VALUES: Topix100StudyMode[] = ['intraday', 'swing_5d'];
const TOPIX100_RANKING_METRIC_VALUES: Topix100RankingMetric[] = ['price_vs_sma_gap', 'price_sma_20_80'];
const TOPIX100_PRICE_SMA_WINDOW_VALUES: Topix100PriceSmaWindow[] = [20, 50, 100];
const TOPIX100_PRICE_BUCKET_VALUES: Topix100PriceBucketFilter[] = ['all', 'q1', 'q10', 'q234'];
const TOPIX100_RANKING_SORT_KEY_VALUES: Topix100RankingSortKey[] = [
  'rank',
  'code',
  'companyName',
  'metric',
  'longScore5d',
  'longScore5dRank',
  'intradayScore',
  'intradayLongRank',
  'intradayShortRank',
  'openToOpen5dReturn',
  'nextSessionIntradayReturn',
  'volumeSma5_20',
  'currentPrice',
  'sector33Name',
  'volume',
];
const TOPIX100_SWING_ONLY_SORT_KEYS: Topix100RankingSortKey[] = [
  'longScore5d',
  'longScore5dRank',
  'openToOpen5dReturn',
];
const TOPIX100_INTRADAY_ONLY_SORT_KEYS: Topix100RankingSortKey[] = [
  'intradayScore',
  'intradayLongRank',
  'intradayShortRank',
  'nextSessionIntradayReturn',
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
  const tab = normalizeString(value);
  if (tab === 'screening') {
    return 'preOpenScreening';
  }
  if (tab === 'sameDayScreening') {
    return 'inSessionScreening';
  }
  return normalizeEnum(tab, SCREENING_SUB_TABS);
}

function normalizeRankingPageTab(value: unknown): RankingPageTab | undefined {
  return normalizeEnum(normalizeString(value), RANKING_PAGE_TABS);
}

function normalizeRankingDailyView(value: unknown): RankingDailyView | undefined {
  return normalizeEnum(normalizeString(value), RANKING_DAILY_VIEWS);
}

function normalizeValueCompositeScoreMethod(value: unknown): ValueCompositeScoreMethod | undefined {
  return normalizeEnum(normalizeString(value), VALUE_COMPOSITE_SCORE_METHOD_VALUES);
}

function normalizeValueCompositeForwardEpsMode(value: unknown): ValueCompositeForwardEpsMode | undefined {
  return normalizeEnum(normalizeString(value), VALUE_COMPOSITE_FORWARD_EPS_MODE_VALUES);
}

function normalizeTopix100PriceBucketFilter(value: unknown): Topix100PriceBucketFilter | undefined {
  const normalized = normalizeString(value);
  if (normalized === 'q456') {
    return 'q234';
  }
  return normalizeEnum(normalized, TOPIX100_PRICE_BUCKET_VALUES);
}

function normalizeTopix100StudyMode(value: unknown): Topix100StudyMode | undefined {
  return normalizeEnum(normalizeString(value), TOPIX100_STUDY_MODE_VALUES);
}

function normalizeTopix100RankingMetric(value: unknown): Topix100RankingMetric | undefined {
  const normalized = normalizeString(value);
  if (normalized === LEGACY_TOPIX100_RANKING_METRIC) {
    return 'price_vs_sma_gap';
  }
  return normalizeEnum(normalized, TOPIX100_RANKING_METRIC_VALUES);
}

function normalizeTopix100PriceSmaWindow(value: unknown): Topix100PriceSmaWindow | undefined {
  const normalized = normalizePositiveInt(value);
  return normalized !== undefined && TOPIX100_PRICE_SMA_WINDOW_VALUES.includes(normalized as Topix100PriceSmaWindow)
    ? (normalized as Topix100PriceSmaWindow)
    : undefined;
}

function normalizeTopix100RankingSortKey(value: unknown): Topix100RankingSortKey | undefined {
  const normalized = normalizeString(value);
  if (normalized === 'longScore5d') {
    return 'longScore5d';
  }
  if (normalized === 'shortScore1d') {
    return 'intradayShortRank';
  }
  return normalizeEnum(normalized, TOPIX100_RANKING_SORT_KEY_VALUES);
}

function resolveTopix100DefaultSortBy(studyMode: Topix100StudyMode | undefined): Topix100RankingSortKey {
  return studyMode === 'intraday' ? 'intradayScore' : 'longScore5d';
}

function normalizeTopix100SortByForStudyMode(
  sortBy: Topix100RankingSortKey | undefined,
  studyMode: Topix100StudyMode | undefined
): Topix100RankingSortKey {
  const resolvedStudyMode = studyMode ?? DEFAULT_RANKING_PARAMS.topix100StudyMode;
  if (!sortBy) {
    return resolveTopix100DefaultSortBy(resolvedStudyMode);
  }
  if (resolvedStudyMode === 'intraday' && TOPIX100_SWING_ONLY_SORT_KEYS.includes(sortBy)) {
    return 'intradayScore';
  }
  if (resolvedStudyMode === 'swing_5d' && TOPIX100_INTRADAY_ONLY_SORT_KEYS.includes(sortBy)) {
    return 'longScore5d';
  }
  return sortBy;
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

function assignSearchParams<T extends object>(base: T, entries: Array<[keyof T, T[keyof T] | undefined]>): T {
  const next = { ...base };

  for (const [key, value] of entries) {
    assignIfDefined(next, key, value);
  }

  return next;
}

function normalizeTopix100MetricWindowPair(
  metricValue: unknown,
  smaWindowValue: unknown
): {
  metric: Topix100RankingMetric | undefined;
  smaWindow: Topix100PriceSmaWindow | undefined;
} {
  const rawMetric = normalizeString(metricValue);
  const metric = normalizeTopix100RankingMetric(rawMetric);
  const smaWindow = normalizeTopix100PriceSmaWindow(smaWindowValue);

  if (rawMetric === LEGACY_TOPIX100_RANKING_METRIC && smaWindow === undefined) {
    return {
      metric,
      smaWindow: 20,
    };
  }

  return { metric, smaWindow };
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
  const code = normalizeString(search.code);
  return code ? { code } : {};
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

export function serializeIndicesSearch(code: string | null | undefined): IndicesRouteSearch {
  const normalizedCode = normalizeString(code);
  return normalizedCode ? { code: normalizedCode } : {};
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
  assignIfDefined(
    next,
    'preOpenMarkets',
    normalizeString(search.preOpenMarkets) ?? normalizeString(search.screeningMarkets)
  );
  assignIfDefined(
    next,
    'preOpenStrategies',
    normalizeString(search.preOpenStrategies) ?? normalizeString(search.screeningStrategies)
  );
  assignIfDefined(
    next,
    'preOpenRecentDays',
    normalizePositiveInt(search.preOpenRecentDays) ?? normalizePositiveInt(search.screeningRecentDays)
  );
  assignIfDefined(next, 'preOpenDate', normalizeString(search.preOpenDate) ?? normalizeString(search.screeningDate));
  assignIfDefined(
    next,
    'preOpenSortBy',
    normalizeEnum(search.preOpenSortBy, SCREENING_SORT_VALUES) ??
      normalizeEnum(search.screeningSortBy, SCREENING_SORT_VALUES)
  );
  assignIfDefined(
    next,
    'preOpenOrder',
    normalizeEnum(search.preOpenOrder, SORT_ORDER_VALUES) ?? normalizeEnum(search.screeningOrder, SORT_ORDER_VALUES)
  );
  assignIfDefined(
    next,
    'preOpenLimit',
    normalizePositiveInt(search.preOpenLimit) ?? normalizePositiveInt(search.screeningLimit)
  );
  assignIfDefined(
    next,
    'inSessionMarkets',
    normalizeString(search.inSessionMarkets) ?? normalizeString(search.sameDayMarkets)
  );
  assignIfDefined(
    next,
    'inSessionStrategies',
    normalizeString(search.inSessionStrategies) ?? normalizeString(search.sameDayStrategies)
  );
  assignIfDefined(
    next,
    'inSessionRecentDays',
    normalizePositiveInt(search.inSessionRecentDays) ?? normalizePositiveInt(search.sameDayRecentDays)
  );
  assignIfDefined(next, 'inSessionDate', normalizeString(search.inSessionDate) ?? normalizeString(search.sameDayDate));
  assignIfDefined(
    next,
    'inSessionSortBy',
    normalizeEnum(search.inSessionSortBy, SCREENING_SORT_VALUES) ??
      normalizeEnum(search.sameDaySortBy, SCREENING_SORT_VALUES)
  );
  assignIfDefined(
    next,
    'inSessionOrder',
    normalizeEnum(search.inSessionOrder, SORT_ORDER_VALUES) ?? normalizeEnum(search.sameDayOrder, SORT_ORDER_VALUES)
  );
  assignIfDefined(
    next,
    'inSessionLimit',
    normalizePositiveInt(search.inSessionLimit) ?? normalizePositiveInt(search.sameDayLimit)
  );
  assignIfDefined(next, 'rankingDate', normalizeString(search.rankingDate));
  assignIfDefined(next, 'rankingLimit', normalizePositiveInt(search.rankingLimit));
  assignIfDefined(next, 'rankingMarkets', normalizeString(search.rankingMarkets));
  assignIfDefined(next, 'rankingLookbackDays', normalizePositiveInt(search.rankingLookbackDays));
  assignIfDefined(next, 'rankingPeriodDays', normalizePositiveInt(search.rankingPeriodDays));
  assignIfDefined(next, 'rankingTopix100StudyMode', normalizeTopix100StudyMode(search.rankingTopix100StudyMode));
  const topix100MetricWindow = normalizeTopix100MetricWindowPair(
    search.rankingTopix100Metric,
    search.rankingTopix100SmaWindow
  );
  assignIfDefined(next, 'rankingTopix100Metric', topix100MetricWindow.metric);
  assignIfDefined(next, 'rankingTopix100SmaWindow', topix100MetricWindow.smaWindow);
  assignIfDefined(
    next,
    'rankingTopix100PriceBucket',
    normalizeTopix100PriceBucketFilter(search.rankingTopix100PriceBucket)
  );
  assignIfDefined(next, 'rankingTopix100SortBy', normalizeTopix100RankingSortKey(search.rankingTopix100SortBy));
  assignIfDefined(next, 'rankingTopix100SortOrder', normalizeEnum(search.rankingTopix100SortOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'fundamentalLimit', normalizePositiveInt(search.fundamentalLimit));
  assignIfDefined(next, 'fundamentalMarkets', normalizeString(search.fundamentalMarkets));
  assignIfDefined(next, 'forecastAboveRecentFyActuals', normalizeBoolean(search.forecastAboveRecentFyActuals));
  assignIfDefined(next, 'forecastLookbackFyCount', normalizePositiveInt(search.forecastLookbackFyCount));

  return next;
}

export function getScreeningStateFromSearch(search: ScreeningRouteSearch): {
  activeSubTab: ScreeningSubTab;
  preOpenScreeningParams: ScreeningParams;
  inSessionScreeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
} {
  const rankingParams = assignSearchParams({ ...DEFAULT_RANKING_PARAMS }, [
    ['date', search.rankingDate],
    ['limit', search.rankingLimit],
    ['markets', search.rankingMarkets],
    ['lookbackDays', search.rankingLookbackDays],
    ['periodDays', search.rankingPeriodDays],
    ['topix100StudyMode', search.rankingTopix100StudyMode],
    ['topix100Metric', search.rankingTopix100Metric],
    ['topix100SmaWindow', search.rankingTopix100SmaWindow],
    ['topix100PriceBucket', search.rankingTopix100PriceBucket],
    ['topix100SortBy', search.rankingTopix100SortBy],
    ['topix100SortOrder', search.rankingTopix100SortOrder],
  ]);
  rankingParams.topix100SortBy = normalizeTopix100SortByForStudyMode(
    rankingParams.topix100SortBy,
    rankingParams.topix100StudyMode
  );

  return {
    activeSubTab: search.tab ?? 'preOpenScreening',
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
    rankingParams,
    fundamentalRankingParams: assignSearchParams({ ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS }, [
      ['limit', search.fundamentalLimit],
      ['markets', search.fundamentalMarkets],
      ['forecastAboveRecentFyActuals', search.forecastAboveRecentFyActuals],
      ['forecastLookbackFyCount', search.forecastLookbackFyCount],
    ]),
  };
}

export function getRankingStateFromSearch(search: RankingRouteSearch): {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  valueCompositeRankingParams: ValueCompositeRankingParams;
} {
  const rankingParams = assignSearchParams({ ...DEFAULT_RANKING_PARAMS }, [
    ['date', search.rankingDate],
    ['limit', search.rankingLimit],
    ['markets', search.rankingMarkets],
    ['lookbackDays', search.rankingLookbackDays],
    ['periodDays', search.rankingPeriodDays],
    ['topix100StudyMode', search.rankingTopix100StudyMode],
    ['topix100Metric', search.rankingTopix100Metric],
    ['topix100SmaWindow', search.rankingTopix100SmaWindow],
    ['topix100PriceBucket', search.rankingTopix100PriceBucket],
    ['topix100SortBy', search.rankingTopix100SortBy],
    ['topix100SortOrder', search.rankingTopix100SortOrder],
  ]);
  rankingParams.topix100SortBy = normalizeTopix100SortByForStudyMode(
    rankingParams.topix100SortBy,
    rankingParams.topix100StudyMode
  );

  return {
    activeSubTab: search.tab ?? 'ranking',
    activeDailyView: search.dailyView ?? 'stocks',
    rankingParams,
    fundamentalRankingParams: assignSearchParams({ ...DEFAULT_FUNDAMENTAL_RANKING_PARAMS }, [
      ['limit', search.fundamentalLimit],
      ['markets', search.fundamentalMarkets],
      ['forecastAboveRecentFyActuals', search.forecastAboveRecentFyActuals],
      ['forecastLookbackFyCount', search.forecastLookbackFyCount],
    ]),
    valueCompositeRankingParams: assignSearchParams({ ...DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS }, [
      ['date', search.valueDate],
      ['limit', search.valueLimit],
      ['markets', search.valueMarkets],
      ['scoreMethod', search.valueScoreMethod],
      ['forwardEpsMode', search.valueForwardEpsMode],
    ]),
  };
}

export function getRankingStateFromScreeningSearch(search: ScreeningRouteSearch): {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
} {
  const state = getScreeningStateFromSearch(search);
  return {
    activeSubTab: state.activeSubTab === 'fundamentalRanking' ? 'fundamentalRanking' : 'ranking',
    activeDailyView: 'stocks',
    rankingParams: state.rankingParams,
    fundamentalRankingParams: state.fundamentalRankingParams,
  };
}

export function serializeScreeningSearch(state: {
  activeSubTab: ScreeningSubTab;
  preOpenScreeningParams: ScreeningParams;
  inSessionScreeningParams: ScreeningParams;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
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
    'rankingTopix100StudyMode',
    state.rankingParams.topix100StudyMode,
    DEFAULT_RANKING_PARAMS.topix100StudyMode
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100Metric',
    state.rankingParams.topix100Metric,
    DEFAULT_RANKING_PARAMS.topix100Metric
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SmaWindow',
    state.rankingParams.topix100SmaWindow,
    DEFAULT_RANKING_PARAMS.topix100SmaWindow
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100PriceBucket',
    state.rankingParams.topix100PriceBucket,
    DEFAULT_RANKING_PARAMS.topix100PriceBucket
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SortBy',
    state.rankingParams.topix100SortBy,
    resolveTopix100DefaultSortBy(state.rankingParams.topix100StudyMode)
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SortOrder',
    state.rankingParams.topix100SortOrder,
    DEFAULT_RANKING_PARAMS.topix100SortOrder
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

export function validateRankingSearch(search: Record<string, unknown>): RankingRouteSearch {
  const next: RankingRouteSearch = {};

  assignIfDefined(next, 'tab', normalizeRankingPageTab(search.tab));
  assignIfDefined(next, 'dailyView', normalizeRankingDailyView(search.dailyView));
  assignIfDefined(next, 'rankingDate', normalizeString(search.rankingDate));
  assignIfDefined(next, 'rankingLimit', normalizePositiveInt(search.rankingLimit));
  assignIfDefined(next, 'rankingMarkets', normalizeString(search.rankingMarkets));
  assignIfDefined(next, 'rankingLookbackDays', normalizePositiveInt(search.rankingLookbackDays));
  assignIfDefined(next, 'rankingPeriodDays', normalizePositiveInt(search.rankingPeriodDays));
  assignIfDefined(next, 'rankingTopix100StudyMode', normalizeTopix100StudyMode(search.rankingTopix100StudyMode));
  const topix100MetricWindow = normalizeTopix100MetricWindowPair(
    search.rankingTopix100Metric,
    search.rankingTopix100SmaWindow
  );
  assignIfDefined(next, 'rankingTopix100Metric', topix100MetricWindow.metric);
  assignIfDefined(next, 'rankingTopix100SmaWindow', topix100MetricWindow.smaWindow);
  assignIfDefined(
    next,
    'rankingTopix100PriceBucket',
    normalizeTopix100PriceBucketFilter(search.rankingTopix100PriceBucket)
  );
  assignIfDefined(next, 'rankingTopix100SortBy', normalizeTopix100RankingSortKey(search.rankingTopix100SortBy));
  assignIfDefined(next, 'rankingTopix100SortOrder', normalizeEnum(search.rankingTopix100SortOrder, SORT_ORDER_VALUES));
  assignIfDefined(next, 'fundamentalLimit', normalizePositiveInt(search.fundamentalLimit));
  assignIfDefined(next, 'fundamentalMarkets', normalizeString(search.fundamentalMarkets));
  assignIfDefined(next, 'forecastAboveRecentFyActuals', normalizeBoolean(search.forecastAboveRecentFyActuals));
  assignIfDefined(next, 'forecastLookbackFyCount', normalizePositiveInt(search.forecastLookbackFyCount));
  assignIfDefined(next, 'valueDate', normalizeString(search.valueDate));
  assignIfDefined(next, 'valueLimit', normalizePositiveInt(search.valueLimit));
  assignIfDefined(next, 'valueMarkets', normalizeString(search.valueMarkets));
  assignIfDefined(next, 'valueScoreMethod', normalizeValueCompositeScoreMethod(search.valueScoreMethod));
  assignIfDefined(next, 'valueForwardEpsMode', normalizeValueCompositeForwardEpsMode(search.valueForwardEpsMode));
  return next;
}

export function serializeRankingSearch(state: {
  activeSubTab: RankingPageTab;
  activeDailyView: RankingDailyView;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  valueCompositeRankingParams: ValueCompositeRankingParams;
}): RankingRouteSearch {
  const next: RankingRouteSearch = {};

  assignIfDefinedAndNotDefault(next, 'tab', state.activeSubTab, 'ranking');
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
    'rankingTopix100StudyMode',
    state.rankingParams.topix100StudyMode,
    DEFAULT_RANKING_PARAMS.topix100StudyMode
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100Metric',
    state.rankingParams.topix100Metric,
    DEFAULT_RANKING_PARAMS.topix100Metric
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SmaWindow',
    state.rankingParams.topix100SmaWindow,
    DEFAULT_RANKING_PARAMS.topix100SmaWindow
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100PriceBucket',
    state.rankingParams.topix100PriceBucket,
    DEFAULT_RANKING_PARAMS.topix100PriceBucket
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SortBy',
    state.rankingParams.topix100SortBy,
    resolveTopix100DefaultSortBy(state.rankingParams.topix100StudyMode)
  );
  assignIfDefinedAndNotDefault(
    next,
    'rankingTopix100SortOrder',
    state.rankingParams.topix100SortOrder,
    DEFAULT_RANKING_PARAMS.topix100SortOrder
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
  assignIfDefined(next, 'valueDate', normalizeString(state.valueCompositeRankingParams.date));
  assignIfDefinedAndNotDefault(
    next,
    'valueLimit',
    typeof state.valueCompositeRankingParams.limit === 'number' ? state.valueCompositeRankingParams.limit : undefined,
    DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS.limit
  );
  assignIfDefinedAndNotDefault(
    next,
    'valueMarkets',
    normalizeString(state.valueCompositeRankingParams.markets),
    DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS.markets
  );
  assignIfDefinedAndNotDefault(
    next,
    'valueScoreMethod',
    state.valueCompositeRankingParams.scoreMethod,
    DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS.scoreMethod
  );
  assignIfDefinedAndNotDefault(
    next,
    'valueForwardEpsMode',
    state.valueCompositeRankingParams.forwardEpsMode,
    DEFAULT_VALUE_COMPOSITE_RANKING_PARAMS.forwardEpsMode
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

export function extractLegacySymbolWorkbenchSearch(state: Record<string, unknown>): SymbolWorkbenchRouteSearch {
  return serializeSymbolWorkbenchSearch({
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

export function extractLegacyScreeningSearch(state: Record<string, unknown>): ScreeningRouteSearch {
  const screeningParams = isRecord(state.screeningParams)
    ? (state.screeningParams as ScreeningParams)
    : DEFAULT_PRE_OPEN_SCREENING_PARAMS;
  const sameDayScreeningParams = isRecord(state.sameDayScreeningParams)
    ? (state.sameDayScreeningParams as ScreeningParams)
    : DEFAULT_IN_SESSION_SCREENING_PARAMS;
  const rankingParams = isRecord(state.rankingParams) ? (state.rankingParams as RankingParams) : DEFAULT_RANKING_PARAMS;
  const fundamentalRankingParams = isRecord(state.fundamentalRankingParams)
    ? (state.fundamentalRankingParams as FundamentalRankingParams)
    : DEFAULT_FUNDAMENTAL_RANKING_PARAMS;
  return serializeScreeningSearch({
    activeSubTab: normalizeScreeningSubTab(state.activeSubTab) ?? 'preOpenScreening',
    preOpenScreeningParams: {
      ...DEFAULT_PRE_OPEN_SCREENING_PARAMS,
      ...screeningParams,
      entry_decidability: 'pre_open_decidable',
    },
    inSessionScreeningParams: {
      ...DEFAULT_IN_SESSION_SCREENING_PARAMS,
      ...sameDayScreeningParams,
      entry_decidability: 'requires_same_session_observation',
    },
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
