import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import type {
  Topix100PriceBucketFilter,
  Topix100RankingItem,
  Topix100RankingResponse,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import { formatPriceJPY, formatVolume, formatVolumeRatio } from '@/utils/formatters';

interface Topix100RankingTableProps {
  data: Topix100RankingResponse | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  priceBucketFilter: Topix100PriceBucketFilter;
  volumeBucketFilter: Topix100VolumeBucketFilter;
}

function matchesFilters(
  item: Topix100RankingItem,
  priceBucketFilter: Topix100PriceBucketFilter,
  volumeBucketFilter: Topix100VolumeBucketFilter
): boolean {
  if (priceBucketFilter !== 'all' && item.priceBucket !== priceBucketFilter) {
    return false;
  }
  if (volumeBucketFilter !== 'all' && item.volumeBucket !== volumeBucketFilter) {
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
    case 'q456':
      return 'bg-sky-500/12 text-sky-700 dark:text-sky-300';
    default:
      return 'bg-muted text-muted-foreground';
  }
}

export function Topix100RankingTable({
  data,
  isLoading,
  error,
  onStockClick,
  priceBucketFilter,
  volumeBucketFilter,
}: Topix100RankingTableProps) {
  const items = (data?.items ?? []).filter((item) => matchesFilters(item, priceBucketFilter, volumeBucketFilter));

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-2 border-b border-border/70 px-4 py-3">
        <SectionEyebrow>Results</SectionEyebrow>
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div className="space-y-1">
            <h2 className="text-base font-semibold text-foreground">
              TOPIX100 Ranking
              {items.length > 0 ? (
                <span className="ml-2 text-sm font-normal text-muted-foreground">({items.length})</span>
              ) : null}
            </h2>
            <p className="text-xs text-muted-foreground">
              Price SMA 20/80 leader board with volume SMA 20/80 sidecar buckets.
            </p>
          </div>
          <div className="text-right text-[11px] text-muted-foreground">
            <div>Universe: latest TOPIX100</div>
            <div>Date: {data?.date ?? '-'}</div>
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
                <th className="w-24 px-2 py-1.5 text-left">Sector</th>
                <th className="w-28 px-2 py-1.5 text-right">Price SMA 20/80</th>
                <th className="w-28 px-2 py-1.5 text-right">Volume SMA 20/80</th>
                <th className="w-24 px-2 py-1.5 text-left">Bucket</th>
                <th className="w-20 px-2 py-1.5 text-left">Vol</th>
                <th className="w-24 px-2 py-1.5 text-right">Price</th>
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
                  <td className="max-w-[120px] truncate px-2 py-1.5 text-muted-foreground">{item.sector33Name}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{formatVolumeRatio(item.priceSma20_80)}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{formatVolumeRatio(item.volumeSma20_80)}</td>
                  <td className="px-2 py-1.5">
                    <span
                      className={`rounded-full px-2 py-1 text-[11px] font-medium uppercase ${bucketToneClass(item.priceBucket)}`}
                    >
                      {item.priceBucket}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-muted-foreground">{item.volumeBucket ?? '-'}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
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
