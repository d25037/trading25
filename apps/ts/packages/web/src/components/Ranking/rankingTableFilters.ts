import type { RankingItem } from '@trading25/contracts/types/api-response-types';
import type {
  DailyRankingTableFilters,
  DailyRankingValuationSignalFilter,
  DailyRankingWarningFilter,
} from '@/types/ranking';
import { getValuationSignal, type ValuationSignal } from './rankingEvidenceTiers';

type NumericFilterKey = keyof Pick<
  DailyRankingTableFilters,
  | 'minChangePct'
  | 'maxChangePct'
  | 'minTradingValue'
  | 'maxTradingValue'
  | 'minMarketCap'
  | 'maxMarketCap'
  | 'minSma5AboveCount5d'
  | 'maxSma5AboveCount5d'
  | 'minPer'
  | 'maxPer'
  | 'minForwardPer'
  | 'maxForwardPer'
  | 'minForecastOperatingProfitGrowthRatio'
  | 'maxForecastOperatingProfitGrowthRatio'
  | 'minPsr'
  | 'maxPsr'
  | 'minForwardPsr'
  | 'maxForwardPsr'
  | 'minPbr'
  | 'maxPbr'
  | 'minValueCompositeScore'
  | 'maxValueCompositeScore'
  | 'minLiquidityZ'
  | 'maxLiquidityZ'
  | 'minSectorScore'
  | 'maxSectorScore'
>;

const VALUATION_SIGNAL_BY_FILTER = {
  deep_value: 'strong_value_confirmation',
  undervalued: 'medium_value_confirmation',
  overvalued: 'overvalued_warning',
  very_overvalued: 'very_overvalued_warning',
  no_earnings: 'no_positive_earnings_valuation',
} as const satisfies Partial<Record<DailyRankingValuationSignalFilter, ValuationSignal>>;

const NUMERIC_FILTER_KEYS: NumericFilterKey[] = [
  'minChangePct',
  'maxChangePct',
  'minTradingValue',
  'maxTradingValue',
  'minMarketCap',
  'maxMarketCap',
  'minSma5AboveCount5d',
  'maxSma5AboveCount5d',
  'minPer',
  'maxPer',
  'minForwardPer',
  'maxForwardPer',
  'minForecastOperatingProfitGrowthRatio',
  'maxForecastOperatingProfitGrowthRatio',
  'minPsr',
  'maxPsr',
  'minForwardPsr',
  'maxForwardPsr',
  'minPbr',
  'maxPbr',
  'minValueCompositeScore',
  'maxValueCompositeScore',
  'minLiquidityZ',
  'maxLiquidityZ',
  'minSectorScore',
  'maxSectorScore',
];

export function countActiveDailyRankingTableFilters(filters: DailyRankingTableFilters): number {
  let count = 0;
  if (filters.text?.trim()) count += 1;
  if (filters.market?.trim()) count += 1;
  if (filters.sector33Name?.trim()) count += 1;
  if (typeof filters.watchlistId === 'number' && Number.isInteger(filters.watchlistId) && filters.watchlistId > 0) {
    count += 1;
  }
  if (filters.regimeState) count += 1;
  if (filters.valuationSignal) count += 1;
  if (filters.warningSignal) count += 1;
  if (filters.technicalState) count += 1;
  for (const key of NUMERIC_FILTER_KEYS) {
    const value = filters[key];
    if (typeof value === 'number' && Number.isFinite(value)) count += 1;
  }
  return count;
}

export function hasActiveDailyRankingTableFilters(filters: DailyRankingTableFilters): boolean {
  return countActiveDailyRankingTableFilters(filters) > 0;
}

export function filterDailyRankingItems<T extends RankingItem>(
  items: T[],
  filters: DailyRankingTableFilters | undefined,
  watchlistCodes?: ReadonlySet<string>
): T[] {
  if (!filters || !hasActiveDailyRankingTableFilters(filters)) {
    return items;
  }
  return items.filter((item) => matchesDailyRankingTableFilters(item, filters, watchlistCodes));
}

function matchesDailyRankingTableFilters(
  item: RankingItem,
  filters: DailyRankingTableFilters,
  watchlistCodes?: ReadonlySet<string>
): boolean {
  return (
    matchesText(item, filters.text) &&
    matchesStringFilter(item.marketCode, filters.market) &&
    matchesStringFilter(item.sector33Name, filters.sector33Name) &&
    matchesWatchlistFilter(item.code, filters.watchlistId, watchlistCodes) &&
    matchesStringFilter(item.liquidityRegime ?? undefined, filters.regimeState) &&
    matchesValuationSignal(item, filters.valuationSignal) &&
    matchesWarningSignal(item, filters.warningSignal) &&
    matchesArrayFilter(item.technicalFlags, filters.technicalState) &&
    matchesRange(item.changePercentage, filters.minChangePct, filters.maxChangePct) &&
    matchesRange(item.tradingValue ?? item.tradingValueAverage, filters.minTradingValue, filters.maxTradingValue) &&
    matchesRange(item.marketCap, filters.minMarketCap, filters.maxMarketCap) &&
    matchesRange(item.sma5AboveCount5d, filters.minSma5AboveCount5d, filters.maxSma5AboveCount5d) &&
    matchesRange(item.per, filters.minPer, filters.maxPer) &&
    matchesRange(item.forwardPer, filters.minForwardPer, filters.maxForwardPer) &&
    matchesRange(
      item.forecastOperatingProfitGrowthRatio,
      filters.minForecastOperatingProfitGrowthRatio,
      filters.maxForecastOperatingProfitGrowthRatio
    ) &&
    matchesRange(item.psr, filters.minPsr, filters.maxPsr) &&
    matchesRange(item.forwardPsr, filters.minForwardPsr, filters.maxForwardPsr) &&
    matchesRange(item.pbr, filters.minPbr, filters.maxPbr) &&
    matchesRange(item.valueCompositeScore, filters.minValueCompositeScore, filters.maxValueCompositeScore) &&
    matchesRange(item.liquidityResidualZ, filters.minLiquidityZ, filters.maxLiquidityZ) &&
    matchesRange(item.sectorStrengthScore, filters.minSectorScore, filters.maxSectorScore)
  );
}

function matchesWatchlistFilter(
  code: string,
  watchlistId: number | undefined,
  watchlistCodes: ReadonlySet<string> | undefined
): boolean {
  if (typeof watchlistId !== 'number' || !Number.isInteger(watchlistId) || watchlistId <= 0) return true;
  return watchlistCodes?.has(code) ?? false;
}

function matchesText(item: RankingItem, text: string | undefined): boolean {
  const query = text?.trim().toLowerCase();
  if (!query) return true;
  return [item.code, item.companyName].some((value) => value.toLowerCase().includes(query));
}

function matchesStringFilter(value: string | undefined, filterValue: string | undefined): boolean {
  if (!filterValue?.trim()) return true;
  return value === filterValue;
}

function matchesArrayFilter(values: string[] | undefined, filterValue: string | undefined): boolean {
  if (!filterValue) return true;
  return values?.includes(filterValue) ?? false;
}

function matchesValuationSignal(
  item: RankingItem,
  filterValue: DailyRankingValuationSignalFilter | undefined
): boolean {
  if (!filterValue) return true;
  if (filterValue === 'value_confirmed') {
    return hasValueConfirmation(item);
  }
  if (filterValue === 'expensive_or') {
    return hasExpensivePerOrPsr(item);
  }
  return getValuationSignal(item) === VALUATION_SIGNAL_BY_FILTER[filterValue];
}

function hasValueConfirmation(item: RankingItem): boolean {
  return (
    hasDeepValueConfirmation(item) ||
    isPercentileAtOrBelow(item.pbrPercentile, 0.2) ||
    hasLowPerForwardPerImprovement(item, 1.0)
  );
}

function hasExpensivePerOrPsr(item: RankingItem): boolean {
  return [item.perPercentile, item.forwardPerPercentile, item.psrPercentile, item.forwardPsrPercentile].some(
    (percentile) => percentile != null && Number.isFinite(percentile) && percentile >= 0.8
  );
}

function hasDeepValueConfirmation(item: RankingItem): boolean {
  return (
    (isPercentileAtOrBelow(item.pbrPercentile, 0.2) && isPercentileAtOrBelow(item.forwardPerPercentile, 0.2)) ||
    hasLowPerForwardPerImprovement(item, 0.8)
  );
}

function hasLowPerForwardPerImprovement(item: RankingItem, maxRatio: number): boolean {
  if (!isPercentileAtOrBelow(item.perPercentile, 0.2)) return false;
  const ratio = getPositiveRatio(item.forwardPer, item.per);
  return ratio != null && ratio <= maxRatio;
}

function getPositiveRatio(numerator: number | null | undefined, denominator: number | null | undefined): number | null {
  if (
    numerator == null ||
    denominator == null ||
    !Number.isFinite(numerator) ||
    !Number.isFinite(denominator) ||
    numerator <= 0 ||
    denominator <= 0
  ) {
    return null;
  }
  return numerator / denominator;
}

function isPercentileAtOrBelow(value: number | null | undefined, threshold: number): boolean {
  return value != null && Number.isFinite(value) && value <= threshold;
}

function matchesWarningSignal(item: RankingItem, filterValue: DailyRankingWarningFilter | undefined): boolean {
  if (!filterValue) return true;
  if (filterValue === 'overheat') {
    return matchesArrayFilter(item.riskFlags, 'overheat');
  }
  if (filterValue === 'sma5_weak_0_1') {
    return matchesRange(item.sma5AboveCount5d, undefined, 1);
  }
  if (filterValue === 'sma5_below_streak_3') {
    return matchesRange(item.sma5BelowStreak, 3, undefined);
  }
  return true;
}

function matchesRange(
  value: number | null | undefined,
  minValue: number | undefined,
  maxValue: number | undefined
): boolean {
  const hasMin = typeof minValue === 'number' && Number.isFinite(minValue);
  const hasMax = typeof maxValue === 'number' && Number.isFinite(maxValue);
  if (!hasMin && !hasMax) return true;
  if (value == null || !Number.isFinite(value)) return false;
  if (hasMin && value < minValue) return false;
  if (hasMax && value > maxValue) return false;
  return true;
}
