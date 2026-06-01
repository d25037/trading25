import type { IndexPerformanceItem } from '@trading25/contracts/types/api-response-types';
import { TrendingUp } from 'lucide-react';
import { useEffect, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import { getIndexCategorySortOrder, INDEX_CATEGORY_LABELS } from '@/lib/indexCategories';
import { cn } from '@/lib/utils';
import { formatPercentage } from '@/utils/formatters';

interface IndexPerformanceTableProps {
  items: IndexPerformanceItem[] | undefined;
  isLoading: boolean;
  error: Error | null;
  onIndexClick: (code: string) => void;
  lookbackDays?: number;
  title?: string;
  description?: string;
  emptyMessage?: string;
  emptySubMessage?: string;
}

const VIRTUALIZATION_THRESHOLD = 120;
const INDEX_ROW_HEIGHT = 34;
const INDEX_CARD_ROW_HEIGHT = 120;
const INDEX_SECTOR_CARD_ROW_HEIGHT = 148;
const INDEX_VIEWPORT_HEIGHT = 560;

const SECTOR_STRENGTH_BUCKET_META = {
  sector_strong: {
    label: 'Strong',
    className: 'border-emerald-500/40 bg-emerald-50 text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300',
  },
  sector_neutral: {
    label: 'Neutral',
    className: 'border-slate-400/40 bg-slate-50 text-slate-600 dark:bg-slate-900/50 dark:text-slate-300',
  },
  sector_weak: {
    label: 'Weak',
    className: 'border-amber-500/40 bg-amber-50 text-amber-700 dark:bg-amber-950/40 dark:text-amber-300',
  },
} as const;

function getIsMobileIndexLayout(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(max-width: 1023px)').matches
  );
}

function useIsMobileIndexLayout(): boolean {
  const [isMobileIndexLayout, setIsMobileIndexLayout] = useState(getIsMobileIndexLayout);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mediaQuery = window.matchMedia('(max-width: 1023px)');
    const updateLayout = () => setIsMobileIndexLayout(mediaQuery.matches);
    updateLayout();
    mediaQuery.addEventListener('change', updateLayout);
    return () => mediaQuery.removeEventListener('change', updateLayout);
  }, []);

  return isMobileIndexLayout;
}

function formatIndexLevel(value: number): string {
  return value.toLocaleString('ja-JP', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatSectorStrengthScore(value: number | null | undefined): string {
  if (value == null) return 'n/a';
  return value.toFixed(2);
}

function formatSectorStrengthMetric(value: number | null | undefined): string {
  if (value == null) return 'n/a';
  return formatPercentage(value);
}

function getSectorStrengthTitle(item: IndexPerformanceItem): string {
  return [
    `Trade score: average of official sector-index strength and constituent strength`,
    `Constituent 20D TOPIX excess: ${formatSectorStrengthMetric(item.sector20dTopixExcessPct)}`,
    `Constituent 60D TOPIX excess: ${formatSectorStrengthMetric(item.sector60dTopixExcessPct)}`,
    `Constituent 20D breadth: ${formatSectorStrengthMetric(item.sectorBreadth20dPct)}`,
    `Constituent stocks: ${item.sectorStockCount ?? 'n/a'}`,
  ].join('\n');
}

function SectorStrengthBucketBadge({ bucket }: { bucket: IndexPerformanceItem['sectorStrengthBucket'] }) {
  if (bucket == null) {
    return <span className="text-muted-foreground">n/a</span>;
  }
  const meta = SECTOR_STRENGTH_BUCKET_META[bucket];
  return (
    <span className={cn('inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold', meta.className)}>
      {meta.label}
    </span>
  );
}

function IndexPerformanceRow({
  item,
  onIndexClick,
  showSectorStrength,
}: {
  item: IndexPerformanceItem;
  onIndexClick: (code: string) => void;
  showSectorStrength: boolean;
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
      {showSectorStrength ? (
        <>
          <td className="px-2 py-1.5 text-right font-semibold tabular-nums" title={getSectorStrengthTitle(item)}>
            {formatSectorStrengthScore(item.sectorStrengthScore)}
          </td>
          <td className="px-2 py-1.5 text-right">
            <SectorStrengthBucketBadge bucket={item.sectorStrengthBucket} />
          </td>
        </>
      ) : null}
    </tr>
  );
}

function IndexPerformanceCard({
  item,
  onIndexClick,
  lookbackDays,
  showSectorStrength,
}: {
  item: IndexPerformanceItem;
  onIndexClick: (code: string) => void;
  lookbackDays: number;
  showSectorStrength: boolean;
}) {
  const isPositive = item.changePercentage >= 0;

  return (
    <button
      type="button"
      onClick={() => onIndexClick(item.code)}
      className="min-h-[7rem] w-full rounded-2xl border border-border/60 bg-background/80 p-3 text-left shadow-sm transition-colors hover:bg-[var(--app-surface-muted)]"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-mono text-sm font-semibold text-primary">{item.code}</p>
          <p className="mt-1 truncate text-sm font-semibold text-foreground">{item.name}</p>
          <p className="mt-0.5 text-xs text-muted-foreground">
            {INDEX_CATEGORY_LABELS[item.category] ?? item.category}
          </p>
        </div>
        <span
          className={cn(
            'shrink-0 text-sm font-semibold tabular-nums',
            isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
          )}
        >
          {formatPercentage(item.changePercentage)}
        </span>
      </div>
      <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
        <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2">
          <p className="text-[10px] font-semibold text-muted-foreground">終値</p>
          <p className="mt-0.5 font-semibold tabular-nums text-foreground">{formatIndexLevel(item.currentClose)}</p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">{item.currentDate}</p>
        </div>
        <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2">
          <p className="text-[10px] font-semibold text-muted-foreground">基準 / {lookbackDays}日</p>
          <p className="mt-0.5 font-semibold tabular-nums text-foreground">{formatIndexLevel(item.baseClose)}</p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">{item.baseDate}</p>
        </div>
        {showSectorStrength ? (
          <>
            <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2" title={getSectorStrengthTitle(item)}>
              <p className="text-[10px] font-semibold text-muted-foreground">Trade Score</p>
              <p className="mt-0.5 font-semibold tabular-nums text-foreground">
                {formatSectorStrengthScore(item.sectorStrengthScore)}
              </p>
            </div>
            <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2">
              <p className="text-[10px] font-semibold text-muted-foreground">Bucket</p>
              <div className="mt-1">
                <SectorStrengthBucketBadge bucket={item.sectorStrengthBucket} />
              </div>
            </div>
          </>
        ) : null}
      </div>
    </button>
  );
}

interface IndexVirtualRows {
  visibleItems: IndexPerformanceItem[];
  paddingTop: number;
  paddingBottom: number;
}

function VirtualSpacer({ height }: { height: number }) {
  if (height <= 0) return null;
  return <div aria-hidden="true" className="shrink-0" style={{ height }} />;
}

function IndexPerformanceCardList({
  virtual,
  shouldVirtualize,
  onIndexClick,
  lookbackDays,
  showSectorStrength,
}: {
  virtual: IndexVirtualRows;
  shouldVirtualize: boolean;
  onIndexClick: (code: string) => void;
  lookbackDays: number;
  showSectorStrength: boolean;
}) {
  return (
    <div className="flex flex-col gap-2 p-3">
      {shouldVirtualize ? <VirtualSpacer height={virtual.paddingTop} /> : null}
      {virtual.visibleItems.map((item) => (
        <IndexPerformanceCard
          key={item.code}
          item={item}
          onIndexClick={onIndexClick}
          lookbackDays={lookbackDays}
          showSectorStrength={showSectorStrength}
        />
      ))}
      {shouldVirtualize ? <VirtualSpacer height={virtual.paddingBottom} /> : null}
    </div>
  );
}

function IndexPerformanceRowsTable({
  virtual,
  shouldVirtualize,
  onIndexClick,
  lookbackDays,
  showSectorStrength,
}: {
  virtual: IndexVirtualRows;
  shouldVirtualize: boolean;
  onIndexClick: (code: string) => void;
  lookbackDays: number;
  showSectorStrength: boolean;
}) {
  const colSpan = showSectorStrength ? 9 : 7;
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
        <tr>
          <th className="w-20 px-2 py-1.5 text-left">コード</th>
          <th className="px-2 py-1.5 text-left">指数名</th>
          <th className="w-28 px-2 py-1.5 text-right">終値</th>
          <th className="w-24 px-2 py-1.5 text-left">日付</th>
          <th className="w-28 px-2 py-1.5 text-right">基準終値</th>
          <th className="w-24 px-2 py-1.5 text-left">基準日</th>
          <th className="w-20 px-2 py-1.5 text-right">{lookbackDays}日騰落率</th>
          {showSectorStrength ? (
            <>
              <th className="w-24 px-2 py-1.5 text-right">Trade Score</th>
              <th className="w-24 px-2 py-1.5 text-right">Bucket</th>
            </>
          ) : null}
        </tr>
      </thead>
      <tbody>
        {shouldVirtualize && virtual.paddingTop > 0 ? (
          <tr>
            <td colSpan={colSpan} className="p-0" style={{ height: virtual.paddingTop }} />
          </tr>
        ) : null}
        {virtual.visibleItems.map((item) => (
          <IndexPerformanceRow
            key={item.code}
            item={item}
            onIndexClick={onIndexClick}
            showSectorStrength={showSectorStrength}
          />
        ))}
        {shouldVirtualize && virtual.paddingBottom > 0 ? (
          <tr>
            <td colSpan={colSpan} className="p-0" style={{ height: virtual.paddingBottom }} />
          </tr>
        ) : null}
      </tbody>
    </table>
  );
}

export function IndexPerformanceTable({
  items,
  isLoading,
  error,
  onIndexClick,
  lookbackDays: selectedLookbackDays,
  title = '指数一覧',
  description,
  emptyMessage = 'No index performance data available',
  emptySubMessage = 'Run index sync or choose a later date',
}: IndexPerformanceTableProps) {
  const rows = (items ?? []).slice().sort((left, right) => {
    const leftScore = left.sectorStrengthScore ?? null;
    const rightScore = right.sectorStrengthScore ?? null;
    if (leftScore != null || rightScore != null) {
      const byScore = (rightScore ?? -1) - (leftScore ?? -1);
      if (byScore !== 0) {
        return byScore;
      }
    }

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
  const showSectorStrength = rows.some((item) => item.sectorStrengthScore != null || item.sectorStrengthBucket != null);
  const shouldVirtualize = rows.length >= VIRTUALIZATION_THRESHOLD;
  const isMobileIndexLayout = useIsMobileIndexLayout();
  const virtual = useVirtualizedRows(rows, {
    enabled: shouldVirtualize,
    rowHeight: isMobileIndexLayout
      ? showSectorStrength
        ? INDEX_SECTOR_CARD_ROW_HEIGHT
        : INDEX_CARD_ROW_HEIGHT
      : INDEX_ROW_HEIGHT,
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
              {title}
              {rows.length > 0 ? (
                <span className="ml-2 text-sm font-normal text-muted-foreground">({rows.length})</span>
              ) : null}
            </h2>
            <p className="mt-1 text-xs text-muted-foreground">
              {description ?? `基準: 各指数終値の ${lookbackDays} 営業日前`}
            </p>
          </div>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={rows.length === 0}
          emptyMessage={emptyMessage}
          emptySubMessage={emptySubMessage}
          emptyIcon={<TrendingUp className="h-8 w-8" />}
          height="h-full min-h-[18rem]"
        >
          {isMobileIndexLayout ? (
            <IndexPerformanceCardList
              virtual={virtual}
              shouldVirtualize={shouldVirtualize}
              onIndexClick={onIndexClick}
              lookbackDays={lookbackDays}
              showSectorStrength={showSectorStrength}
            />
          ) : (
            <IndexPerformanceRowsTable
              virtual={virtual}
              shouldVirtualize={shouldVirtualize}
              onIndexClick={onIndexClick}
              lookbackDays={lookbackDays}
              showSectorStrength={showSectorStrength}
            />
          )}
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
