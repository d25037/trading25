import { TrendingUp } from 'lucide-react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import { getIndexCategorySortOrder, INDEX_CATEGORY_LABELS } from '@/lib/indexCategories';
import { cn } from '@/lib/utils';
import type { IndexPerformanceItem } from '@/types/ranking';
import { formatPercentage } from '@/utils/formatters';

interface IndexPerformanceTableProps {
  items: IndexPerformanceItem[] | undefined;
  isLoading: boolean;
  error: Error | null;
  onIndexClick: (code: string) => void;
  lookbackDays?: number;
}

const VIRTUALIZATION_THRESHOLD = 120;
const INDEX_ROW_HEIGHT = 34;
const INDEX_VIEWPORT_HEIGHT = 560;

function formatIndexLevel(value: number): string {
  return value.toLocaleString('ja-JP', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function IndexPerformanceRow({
  item,
  onIndexClick,
}: {
  item: IndexPerformanceItem;
  onIndexClick: (code: string) => void;
}) {
  const isPositive = item.changePercentage >= 0;

  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onIndexClick(item.code)}
    >
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      <td className="px-2 py-1.5">
        <div className="font-medium">{item.name}</div>
        <div className="text-[11px] text-muted-foreground">{INDEX_CATEGORY_LABELS[item.category] ?? item.category}</div>
      </td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatIndexLevel(item.currentClose)}</td>
      <td className="px-2 py-1.5 text-xs text-muted-foreground">{item.currentDate}</td>
      <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">{formatIndexLevel(item.baseClose)}</td>
      <td className="px-2 py-1.5 text-xs text-muted-foreground">{item.baseDate}</td>
      <td
        className={cn(
          'px-2 py-1.5 text-right font-medium tabular-nums',
          isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
        )}
      >
        {formatPercentage(item.changePercentage)}
      </td>
    </tr>
  );
}

export function IndexPerformanceTable({
  items,
  isLoading,
  error,
  onIndexClick,
  lookbackDays: selectedLookbackDays,
}: IndexPerformanceTableProps) {
  const rows = (items ?? []).slice().sort((left, right) => {
    const byChange = right.changePercentage - left.changePercentage;
    if (byChange !== 0) {
      return byChange;
    }

    const byCategory = getIndexCategorySortOrder(left.category) - getIndexCategorySortOrder(right.category);
    if (byCategory !== 0) {
      return byCategory;
    }

    return left.code.localeCompare(right.code);
  });
  const shouldVirtualize = rows.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(rows, {
    enabled: shouldVirtualize,
    rowHeight: INDEX_ROW_HEIGHT,
    viewportHeight: INDEX_VIEWPORT_HEIGHT,
  });
  const lookbackDays = rows[0]?.lookbackDays ?? selectedLookbackDays ?? 5;

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-1 border-b border-border/70 px-4 py-3">
        <SectionEyebrow>Results</SectionEyebrow>
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold text-foreground">
              Indices
              {rows.length > 0 ? (
                <span className="ml-2 text-sm font-normal text-muted-foreground">({rows.length})</span>
              ) : null}
            </h2>
            <p className="mt-1 text-xs text-muted-foreground">
              Baseline: {lookbackDays} trading sessions before each index close
            </p>
          </div>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={rows.length === 0}
          emptyMessage="No index performance data available"
          emptySubMessage="Run index sync or choose a later date"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
          height="h-full min-h-[18rem]"
        >
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
              <tr>
                <th className="w-20 px-2 py-1.5 text-left">Code</th>
                <th className="px-2 py-1.5 text-left">Index</th>
                <th className="w-28 px-2 py-1.5 text-right">Close</th>
                <th className="w-24 px-2 py-1.5 text-left">Date</th>
                <th className="w-28 px-2 py-1.5 text-right">Base Close</th>
                <th className="w-24 px-2 py-1.5 text-left">Base Date</th>
                <th className="w-20 px-2 py-1.5 text-right">{lookbackDays}D</th>
              </tr>
            </thead>
            <tbody>
              {shouldVirtualize && virtual.paddingTop > 0 ? (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingTop }} />
                </tr>
              ) : null}
              {virtual.visibleItems.map((item) => (
                <IndexPerformanceRow key={item.code} item={item} onIndexClick={onIndexClick} />
              ))}
              {shouldVirtualize && virtual.paddingBottom > 0 ? (
                <tr>
                  <td colSpan={7} className="p-0" style={{ height: virtual.paddingBottom }} />
                </tr>
              ) : null}
            </tbody>
          </table>
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
