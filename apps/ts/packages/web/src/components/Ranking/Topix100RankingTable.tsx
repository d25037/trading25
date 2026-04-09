import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type {
  SortOrder,
  Topix100PriceBucketFilter,
  Topix100PriceSmaWindow,
  Topix100RankingItem,
  Topix100RankingSortKey,
  Topix100RankingMetric,
  Topix100RankingResponse,
  Topix100StreakModeFilter,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import { formatPriceJPY, formatRate, formatVolume, formatVolumeRatio } from '@/utils/formatters';
import {
  getTopix100RankingMetricLabel,
  getTopix100StreakModeLabel,
} from './topix100RankingMetric';

interface Topix100RankingTableProps {
  data: Topix100RankingResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  rankingMetric: Topix100RankingMetric;
  rankingSmaWindow: Topix100PriceSmaWindow;
  priceBucketFilter: Topix100PriceBucketFilter;
  volumeBucketFilter: Topix100VolumeBucketFilter;
  shortModeFilter: Topix100StreakModeFilter;
  longModeFilter: Topix100StreakModeFilter;
  sortBy: Topix100RankingSortKey;
  sortOrder: SortOrder;
  onSortChange: (sortBy: Topix100RankingSortKey, sortOrder: SortOrder) => void;
}

function matchesFilters(
  item: Topix100RankingItem,
  priceBucketFilter: Topix100PriceBucketFilter,
  volumeBucketFilter: Topix100VolumeBucketFilter,
  shortModeFilter: Topix100StreakModeFilter,
  longModeFilter: Topix100StreakModeFilter
): boolean {
  if (priceBucketFilter !== 'all' && item.priceBucket !== priceBucketFilter) {
    return false;
  }
  if (volumeBucketFilter !== 'all' && item.volumeBucket !== volumeBucketFilter) {
    return false;
  }
  if (shortModeFilter !== 'all' && item.streakShortMode !== shortModeFilter) {
    return false;
  }
  if (longModeFilter !== 'all' && item.streakLongMode !== longModeFilter) {
    return false;
  }
  return true;
}

function streakModeToneClass(mode: Topix100RankingItem['streakShortMode']): string {
  if (mode === 'bullish') {
    return 'bg-emerald-500/12 text-emerald-700 dark:text-emerald-300';
  }
  if (mode === 'bearish') {
    return 'bg-rose-500/12 text-rose-700 dark:text-rose-300';
  }
  return 'bg-muted text-muted-foreground';
}

function getStudyReadItems(metric: Topix100RankingMetric): string[] {
  if (metric === 'price_vs_sma_gap') {
    return ['Q10 = below SMA', 'Q2-4 = trough', 'Decile-only score', 'Volume/state = context'];
  }

  return ['Legacy comparison', 'Decile-only intraday score'];
}

function formatScore(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '-';
  }
  return formatRate(value);
}

function getDefaultSortOrder(sortBy: Topix100RankingSortKey): SortOrder {
  switch (sortBy) {
    case 'code':
    case 'companyName':
    case 'volumeBucket':
    case 'streakShortMode':
    case 'streakLongMode':
    case 'intradayLongRank':
    case 'intradayShortRank':
    case 'sector33Name':
      return 'asc';
    default:
      return 'desc';
  }
}

function resolveNextSortOrder(
  currentSortBy: Topix100RankingSortKey,
  currentSortOrder: SortOrder,
  nextSortBy: Topix100RankingSortKey
): SortOrder {
  if (currentSortBy === nextSortBy) {
    return currentSortOrder === 'asc' ? 'desc' : 'asc';
  }
  return getDefaultSortOrder(nextSortBy);
}

function compareNullableNumbers(
  left: number | null | undefined,
  right: number | null | undefined,
  sortOrder: SortOrder
): number {
  const leftMissing = typeof left !== 'number' || !Number.isFinite(left);
  const rightMissing = typeof right !== 'number' || !Number.isFinite(right);
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  return sortOrder === 'asc' ? left - right : right - left;
}

function compareNullableStrings(
  left: string | null | undefined,
  right: string | null | undefined,
  sortOrder: SortOrder
): number {
  const leftValue = typeof left === 'string' ? left.trim() : '';
  const rightValue = typeof right === 'string' ? right.trim() : '';
  const leftMissing = leftValue.length === 0;
  const rightMissing = rightValue.length === 0;
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  return sortOrder === 'asc' ? leftValue.localeCompare(rightValue) : rightValue.localeCompare(leftValue);
}

function compareItems(
  left: Topix100RankingItem,
  right: Topix100RankingItem,
  rankingMetric: Topix100RankingMetric,
  sortBy: Topix100RankingSortKey,
  sortOrder: SortOrder
): number {
  switch (sortBy) {
    case 'rank':
      return compareNullableNumbers(left.rank, right.rank, sortOrder);
    case 'code':
      return compareNullableStrings(left.code, right.code, sortOrder);
    case 'companyName':
      return compareNullableStrings(left.companyName, right.companyName, sortOrder);
    case 'metric':
      return compareNullableNumbers(
        rankingMetric === 'price_sma_20_80' ? left.priceSma20_80 : left.priceVsSmaGap,
        rankingMetric === 'price_sma_20_80' ? right.priceSma20_80 : right.priceVsSmaGap,
        sortOrder
      );
    case 'volumeBucket':
      return compareNullableStrings(left.volumeBucket, right.volumeBucket, sortOrder);
    case 'streakShortMode':
      return compareNullableStrings(left.streakShortMode, right.streakShortMode, sortOrder);
    case 'streakLongMode':
      return compareNullableStrings(left.streakLongMode, right.streakLongMode, sortOrder);
    case 'intradayScore':
      return compareNullableNumbers(left.intradayScore, right.intradayScore, sortOrder);
    case 'intradayLongRank':
      return compareNullableNumbers(left.intradayLongRank, right.intradayLongRank, sortOrder);
    case 'intradayShortRank':
      return compareNullableNumbers(left.intradayShortRank, right.intradayShortRank, sortOrder);
    case 'nextSessionIntradayReturn':
      return compareNullableNumbers(
        left.nextSessionIntradayReturn,
        right.nextSessionIntradayReturn,
        sortOrder
      );
    case 'volumeSma5_20':
      return compareNullableNumbers(left.volumeSma5_20, right.volumeSma5_20, sortOrder);
    case 'currentPrice':
      return compareNullableNumbers(left.currentPrice, right.currentPrice, sortOrder);
    case 'sector33Name':
      return compareNullableStrings(left.sector33Name, right.sector33Name, sortOrder);
    case 'volume':
      return compareNullableNumbers(left.volume, right.volume, sortOrder);
    default:
      return 0;
  }
}

function sortItems(
  items: Topix100RankingItem[],
  rankingMetric: Topix100RankingMetric,
  sortBy: Topix100RankingSortKey,
  sortOrder: SortOrder
): Topix100RankingItem[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((left, right) => {
      const comparison = compareItems(left.item, right.item, rankingMetric, sortBy, sortOrder);
      if (comparison !== 0) {
        return comparison;
      }
      return left.index - right.index;
    })
    .map(({ item }) => item);
}

function renderSortMark(active: boolean, sortOrder: SortOrder): string {
  if (!active) {
    return '↕';
  }
  return sortOrder === 'asc' ? '↑' : '↓';
}

function SortableHeader({
  label,
  sortField,
  activeSortBy,
  activeSortOrder,
  onSortChange,
  className,
  buttonClassName,
}: {
  label: string;
  sortField: Topix100RankingSortKey;
  activeSortBy: Topix100RankingSortKey;
  activeSortOrder: SortOrder;
  onSortChange: (sortBy: Topix100RankingSortKey, sortOrder: SortOrder) => void;
  className: string;
  buttonClassName?: string;
}) {
  const isActive = activeSortBy === sortField;
  const nextOrder = resolveNextSortOrder(activeSortBy, activeSortOrder, sortField);

  return (
    <th
      aria-sort={isActive ? (activeSortOrder === 'asc' ? 'ascending' : 'descending') : 'none'}
      className={className}
    >
      <button
        type="button"
        className={`inline-flex w-full items-center gap-1 text-current transition-colors hover:text-foreground ${buttonClassName ?? 'justify-start'}`}
        onClick={() => onSortChange(sortField, nextOrder)}
      >
        <span>{label}</span>
        <span aria-hidden="true" className="text-[10px] text-muted-foreground">
          {renderSortMark(isActive, activeSortOrder)}
        </span>
      </button>
    </th>
  );
}

export function Topix100RankingTable({
  data,
  isLoading,
  error,
  onStockClick,
  rankingMetric,
  rankingSmaWindow,
  priceBucketFilter,
  volumeBucketFilter,
  shortModeFilter,
  longModeFilter,
  sortBy,
  sortOrder,
  onSortChange,
}: Topix100RankingTableProps) {
  const filteredItems = (data?.items ?? []).filter((item) =>
    matchesFilters(item, priceBucketFilter, volumeBucketFilter, shortModeFilter, longModeFilter)
  );
  const effectiveMetric = data?.rankingMetric ?? rankingMetric;
  const effectiveSmaWindow = data?.smaWindow ?? rankingSmaWindow;
  const items = sortItems(filteredItems, effectiveMetric, sortBy, sortOrder);
  const metricLabel = getTopix100RankingMetricLabel(effectiveMetric, effectiveSmaWindow);
  const studyReadItems = getStudyReadItems(effectiveMetric);

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-1 border-b border-border/70 px-4 py-2">
        <SectionEyebrow>Results</SectionEyebrow>
        <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1">
          <h2 className="text-base font-semibold text-foreground">
            TOPIX100 SMA Divergence
            {items.length > 0 ? (
              <span className="ml-2 text-sm font-normal text-muted-foreground">({items.length})</span>
            ) : null}
          </h2>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
            <span>{metricLabel}</span>
            {studyReadItems.map((item) => (
              <span key={item}>{item}</span>
            ))}
            <span>
              State X = {data?.shortWindowStreaks ?? 3}/{data?.longWindowStreaks ?? 53}
            </span>
            <span>Score = Next-session open → close LightGBM (decile-only)</span>
            <span>Realized = next available open → close when present</span>
            <span>{data?.date ?? '-'}</span>
          </div>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto">
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={items.length === 0}
          emptyMessage="No TOPIX100 ranking data available"
          emptySubMessage="Try a different date or relax the filters."
          height="h-full min-h-[18rem]"
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
              <tr>
                <th className="w-12 px-2 py-1.5 text-center text-muted-foreground">#</th>
                <SortableHeader
                  label="Code"
                  sortField="code"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-16 px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label="Company"
                  sortField="companyName"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label={metricLabel}
                  sortField="metric"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-28 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
                <SortableHeader
                  label="Vol Split"
                  sortField="volumeBucket"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-20 px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label="Short"
                  sortField="streakShortMode"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-20 px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label="Long"
                  sortField="streakLongMode"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-20 px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label="ID Score"
                  sortField="intradayScore"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-24 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
                <SortableHeader
                  label="Next Ret"
                  sortField="nextSessionIntradayReturn"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-28 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
                <SortableHeader
                  label="Volume SMA 5/20"
                  sortField="volumeSma5_20"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-28 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
                <SortableHeader
                  label="Price"
                  sortField="currentPrice"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-24 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
                <SortableHeader
                  label="Sector"
                  sortField="sector33Name"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-24 px-2 py-1.5 text-left"
                />
                <SortableHeader
                  label="Volume"
                  sortField="volume"
                  activeSortBy={sortBy}
                  activeSortOrder={sortOrder}
                  onSortChange={onSortChange}
                  className="w-24 px-2 py-1.5 text-right"
                  buttonClassName="justify-end"
                />
              </tr>
            </thead>
            <tbody>
              {items.map((item, index) => (
                <tr
                  key={item.code}
                  className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
                  onClick={() => onStockClick(item.code)}
                >
                  <td className="px-2 py-1.5 text-center font-medium tabular-nums">{index + 1}</td>
                  <td className="px-2 py-1.5 font-medium">{item.code}</td>
                  <td className="max-w-[220px] truncate px-2 py-1.5">{item.companyName}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">
                    {effectiveMetric === 'price_sma_20_80'
                      ? formatVolumeRatio(item.priceSma20_80)
                      : formatRate(item.priceVsSmaGap)}
                  </td>
                  <td className="px-2 py-1.5 text-muted-foreground">{item.volumeBucket ?? '-'}</td>
                  <td className="px-2 py-1.5">
                    <span
                      className={`inline-flex rounded-full px-2 py-1 text-[11px] font-medium ${streakModeToneClass(item.streakShortMode)}`}
                    >
                      {item.streakShortMode ? getTopix100StreakModeLabel(item.streakShortMode) : '-'}
                    </span>
                  </td>
                  <td className="px-2 py-1.5">
                    <span
                      className={`inline-flex rounded-full px-2 py-1 text-[11px] font-medium ${streakModeToneClass(item.streakLongMode)}`}
                    >
                      {item.streakLongMode ? getTopix100StreakModeLabel(item.streakLongMode) : '-'}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums">
                    {formatScore(item.intradayScore)}
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums">
                    <div>{formatScore(item.nextSessionIntradayReturn)}</div>
                    <div className="text-[10px] text-muted-foreground">
                      {item.nextSessionDate ?? '-'}
                    </div>
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{formatVolumeRatio(item.volumeSma5_20)}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
                  <td className="max-w-[120px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                    {formatVolume(item.volume)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
