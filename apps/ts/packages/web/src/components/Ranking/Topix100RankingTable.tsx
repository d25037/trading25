import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type {
  Topix100PriceBucketFilter,
  Topix100PriceSmaWindow,
  Topix100RankingItem,
  Topix100RankingMetric,
  Topix100RankingResponse,
  Topix100StreakModeFilter,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import { formatPriceJPY, formatRate, formatVolume, formatVolumeRatio } from '@/utils/formatters';
import {
  getTopix100PriceBucketLabel,
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

function bucketToneClass(priceBucket: Topix100RankingItem['priceBucket']): string {
  switch (priceBucket) {
    case 'q1':
      return 'bg-emerald-500/12 text-emerald-700 dark:text-emerald-300';
    case 'q10':
      return 'bg-amber-500/12 text-amber-700 dark:text-amber-300';
    case 'q234':
      return 'bg-sky-500/12 text-sky-700 dark:text-sky-300';
    default:
      return 'bg-muted text-muted-foreground';
  }
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
    return ['Q10 = below SMA', 'Q2-4 = trough', 'Volume split by decile', 'Streak 3/53 overlay'];
  }

  return ['Legacy comparison', 'Streak 3/53 overlay'];
}

function formatScore(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '-';
  }
  return formatRate(value);
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
}: Topix100RankingTableProps) {
  const items = (data?.items ?? []).filter((item) =>
    matchesFilters(item, priceBucketFilter, volumeBucketFilter, shortModeFilter, longModeFilter)
  );
  const effectiveMetric = data?.rankingMetric ?? rankingMetric;
  const effectiveSmaWindow = data?.smaWindow ?? rankingSmaWindow;
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
            <span>
              Score = {data?.longScoreHorizonDays ?? 5}d long / {data?.shortScoreHorizonDays ?? 1}d short
            </span>
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
                <th className="w-12 px-2 py-1.5 text-center">#</th>
                <th className="w-16 px-2 py-1.5 text-left">Code</th>
                <th className="px-2 py-1.5 text-left">Company</th>
                <th className="w-28 px-2 py-1.5 text-right">{metricLabel}</th>
                <th className="w-24 px-2 py-1.5 text-left">Bucket</th>
                <th className="w-20 px-2 py-1.5 text-left">Vol Split</th>
                <th className="w-20 px-2 py-1.5 text-left">Short</th>
                <th className="w-20 px-2 py-1.5 text-left">Long</th>
                <th className="w-24 px-2 py-1.5 text-right">L5d</th>
                <th className="w-24 px-2 py-1.5 text-right">S1d</th>
                <th className="w-28 px-2 py-1.5 text-right">Volume SMA 5/20</th>
                <th className="w-24 px-2 py-1.5 text-right">Price</th>
                <th className="w-24 px-2 py-1.5 text-left">Sector</th>
                <th className="w-24 px-2 py-1.5 text-right">Volume</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr
                  key={item.code}
                  className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
                  onClick={() => onStockClick(item.code)}
                >
                  <td className="px-2 py-1.5 text-center font-medium tabular-nums">{item.rank}</td>
                  <td className="px-2 py-1.5 font-medium">{item.code}</td>
                  <td className="max-w-[220px] truncate px-2 py-1.5">{item.companyName}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">
                    {effectiveMetric === 'price_sma_20_80'
                      ? formatVolumeRatio(item.priceSma20_80)
                      : formatRate(item.priceVsSmaGap)}
                  </td>
                  <td className="px-2 py-1.5">
                    <span
                      className={`rounded-full px-2 py-1 text-[11px] font-medium ${bucketToneClass(item.priceBucket)}`}
                    >
                      {getTopix100PriceBucketLabel(item.priceBucket)}
                    </span>
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
                    {item.longScore5dRank ? (
                      <span className="mr-1 text-[11px] text-muted-foreground">#{item.longScore5dRank}</span>
                    ) : null}
                    {formatScore(item.longScore5d)}
                  </td>
                  <td className="px-2 py-1.5 text-right tabular-nums">
                    {item.shortScore1dRank ? (
                      <span className="mr-1 text-[11px] text-muted-foreground">#{item.shortScore1dRank}</span>
                    ) : null}
                    {formatScore(item.shortScore1d)}
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
