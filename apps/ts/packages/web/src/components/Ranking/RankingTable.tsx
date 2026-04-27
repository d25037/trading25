import { ArrowDownCircle, ArrowUpCircle, DollarSign, TrendingDown, TrendingUp } from 'lucide-react';
import { useEffect, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import { cn } from '@/lib/utils';
import type { RankingItem, Rankings, RankingType } from '@/types/ranking';
import { formatPercentage, formatPriceJPY, formatTradingValue } from '@/utils/formatters';

interface RankingTableProps {
  rankings: Rankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  periodDays?: number;
}

const rankingTabs: { id: RankingType; label: string; icon: typeof DollarSign }[] = [
  { id: 'tradingValue', label: 'Trading Value', icon: DollarSign },
  { id: 'gainers', label: 'Gainers', icon: TrendingUp },
  { id: 'losers', label: 'Losers', icon: TrendingDown },
  { id: 'periodHigh', label: 'Period High', icon: ArrowUpCircle },
  { id: 'periodLow', label: 'Period Low', icon: ArrowDownCircle },
];

const VIRTUALIZATION_THRESHOLD = 120;
const RANKING_ROW_HEIGHT = 36;
const RANKING_CARD_ROW_HEIGHT = 128;
const RANKING_VIEWPORT_HEIGHT = 520;

function getIsMobileRankingLayout(): boolean {
  return (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(max-width: 1023px)').matches
  );
}

function useIsMobileRankingLayout(): boolean {
  const [isMobileRankingLayout, setIsMobileRankingLayout] = useState(getIsMobileRankingLayout);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const mediaQuery = window.matchMedia('(max-width: 1023px)');
    const updateLayout = () => setIsMobileRankingLayout(mediaQuery.matches);
    updateLayout();
    mediaQuery.addEventListener('change', updateLayout);
    return () => mediaQuery.removeEventListener('change', updateLayout);
  }, []);

  return isMobileRankingLayout;
}

function RankingRow({
  item,
  onStockClick,
  showChange,
}: {
  item: RankingItem;
  onStockClick: (code: string) => void;
  showChange: boolean;
}) {
  const isPositive = (item.changePercentage ?? 0) >= 0;

  return (
    <tr
      className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
      onClick={() => onStockClick(item.code)}
    >
      <td className="px-2 py-1.5 text-center font-medium tabular-nums">{item.rank}</td>
      <td className="px-2 py-1.5 font-medium">{item.code}</td>
      <td className="px-2 py-1.5 truncate max-w-[180px]">{item.companyName}</td>
      <td className="px-2 py-1.5 truncate max-w-[100px] text-muted-foreground">{item.sector33Name}</td>
      <td className="px-2 py-1.5 text-right tabular-nums">{formatPriceJPY(item.currentPrice)}</td>
      {showChange && (
        <td
          className={cn(
            'px-2 py-1.5 text-right tabular-nums font-medium',
            isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
          )}
        >
          {formatPercentage(item.changePercentage)}
        </td>
      )}
      <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
        {formatTradingValue(item.tradingValue ?? item.tradingValueAverage)}
      </td>
    </tr>
  );
}

function RankingCard({
  item,
  onStockClick,
  showChange,
  isPeriodType,
}: {
  item: RankingItem;
  onStockClick: (code: string) => void;
  showChange: boolean;
  isPeriodType: boolean;
}) {
  const isPositive = (item.changePercentage ?? 0) >= 0;

  return (
    <button
      type="button"
      onClick={() => onStockClick(item.code)}
      className="min-h-[7.5rem] w-full rounded-2xl border border-border/60 bg-background/80 p-3 text-left shadow-sm transition-colors hover:bg-[var(--app-surface-muted)]"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className="rounded-full bg-[var(--app-surface-muted)] px-2 py-0.5 text-[11px] font-semibold tabular-nums text-muted-foreground">
              #{item.rank}
            </span>
            <span className="font-mono text-sm font-semibold text-primary">{item.code}</span>
          </div>
          <p className="mt-1 truncate text-sm font-semibold text-foreground">{item.companyName}</p>
          <p className="mt-0.5 truncate text-xs text-muted-foreground">{item.sector33Name}</p>
        </div>
        {showChange ? (
          <span
            className={cn(
              'shrink-0 text-sm font-semibold tabular-nums',
              isPositive ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'
            )}
          >
            {formatPercentage(item.changePercentage)}
          </span>
        ) : null}
      </div>
      <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
        <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2">
          <p className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">Price</p>
          <p className="mt-0.5 font-semibold tabular-nums text-foreground">{formatPriceJPY(item.currentPrice)}</p>
        </div>
        <div className="rounded-xl bg-[var(--app-surface-muted)] px-2.5 py-2">
          <p className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            {showChange ? (isPeriodType ? 'Break' : 'Change') : 'Trading Value'}
          </p>
          <p className="mt-0.5 font-semibold tabular-nums text-foreground">
            {showChange
              ? formatPercentage(item.changePercentage)
              : formatTradingValue(item.tradingValue ?? item.tradingValueAverage)}
          </p>
        </div>
      </div>
      {showChange ? (
        <p className="mt-2 text-xs text-muted-foreground">
          Trading Value: {formatTradingValue(item.tradingValue ?? item.tradingValueAverage)}
        </p>
      ) : null}
    </button>
  );
}

interface RankingVirtualRows {
  visibleItems: RankingItem[];
  paddingTop: number;
  paddingBottom: number;
}

function VirtualSpacer({ height }: { height: number }) {
  if (height <= 0) return null;
  return <div aria-hidden="true" className="shrink-0" style={{ height }} />;
}

function RankingCardList({
  virtual,
  shouldVirtualize,
  onStockClick,
  showChange,
  isPeriodType,
}: {
  virtual: RankingVirtualRows;
  shouldVirtualize: boolean;
  onStockClick: (code: string) => void;
  showChange: boolean;
  isPeriodType: boolean;
}) {
  return (
    <div className="flex flex-col gap-2 p-3">
      {shouldVirtualize ? <VirtualSpacer height={virtual.paddingTop} /> : null}
      {virtual.visibleItems.map((item) => (
        <RankingCard
          key={`${item.code}-${item.rank}`}
          item={item}
          onStockClick={onStockClick}
          showChange={showChange}
          isPeriodType={isPeriodType}
        />
      ))}
      {shouldVirtualize ? <VirtualSpacer height={virtual.paddingBottom} /> : null}
    </div>
  );
}

function RankingRowsTable({
  virtual,
  shouldVirtualize,
  onStockClick,
  showChange,
  isPeriodType,
  columnCount,
}: {
  virtual: RankingVirtualRows;
  shouldVirtualize: boolean;
  onStockClick: (code: string) => void;
  showChange: boolean;
  isPeriodType: boolean;
  columnCount: number;
}) {
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
        <tr>
          <th className="text-center px-2 py-1.5 w-12">#</th>
          <th className="text-left px-2 py-1.5 w-16">Code</th>
          <th className="text-left px-2 py-1.5">Company</th>
          <th className="text-left px-2 py-1.5 w-24">Sector</th>
          <th className="text-right px-2 py-1.5 w-24">Price</th>
          {showChange && <th className="text-right px-2 py-1.5 w-20">{isPeriodType ? 'Break %' : 'Change'}</th>}
          <th className="text-right px-2 py-1.5 w-24">Trading Value</th>
        </tr>
      </thead>
      <tbody>
        {shouldVirtualize && virtual.paddingTop > 0 && (
          <tr>
            <td colSpan={columnCount} className="p-0" style={{ height: virtual.paddingTop }} />
          </tr>
        )}
        {virtual.visibleItems.map((item) => (
          <RankingRow
            key={`${item.code}-${item.rank}`}
            item={item}
            onStockClick={onStockClick}
            showChange={showChange}
          />
        ))}
        {shouldVirtualize && virtual.paddingBottom > 0 && (
          <tr>
            <td colSpan={columnCount} className="p-0" style={{ height: virtual.paddingBottom }} />
          </tr>
        )}
      </tbody>
    </table>
  );
}

export function RankingTable({ rankings, isLoading, error, onStockClick, periodDays }: RankingTableProps) {
  const [activeRankingType, setActiveRankingType] = useState<RankingType>('tradingValue');
  const isMobileRankingLayout = useIsMobileRankingLayout();

  const currentItems = rankings?.[activeRankingType] ?? [];
  const showChange = activeRankingType !== 'tradingValue';
  const isPeriodType = activeRankingType === 'periodHigh' || activeRankingType === 'periodLow';
  const shouldVirtualize = currentItems.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(currentItems, {
    enabled: shouldVirtualize,
    rowHeight: isMobileRankingLayout ? RANKING_CARD_ROW_HEIGHT : RANKING_ROW_HEIGHT,
    viewportHeight: RANKING_VIEWPORT_HEIGHT,
  });
  const columnCount = showChange ? 7 : 6;

  // Dynamic label for period tabs
  const getTabLabel = (tab: (typeof rankingTabs)[number]) => {
    if (tab.id === 'periodHigh') return `${periodDays || 250}D High`;
    if (tab.id === 'periodLow') return `${periodDays || 250}D Low`;
    return tab.label;
  };

  return (
    <Surface className="flex min-h-[24rem] flex-1 flex-col overflow-hidden">
      <div className="space-y-4 border-b border-border/70 px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <SectionEyebrow>Results</SectionEyebrow>
            <h2 className="text-base font-semibold text-foreground">
              Market Rankings
              {currentItems.length > 0 && (
                <span className="text-sm font-normal text-muted-foreground ml-2">({currentItems.length})</span>
              )}
            </h2>
          </div>
          <Select value={activeRankingType} onValueChange={(value) => setActiveRankingType(value as RankingType)}>
            <SelectTrigger className="h-8 w-[12.5rem] text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {rankingTabs.map((tab) => (
                <SelectItem key={tab.id} value={tab.id} className="text-xs">
                  {getTabLabel(tab)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto" onScroll={shouldVirtualize ? virtual.onScroll : undefined}>
        <DataStateWrapper
          isLoading={isLoading}
          error={error}
          isEmpty={currentItems.length === 0}
          emptyMessage="No ranking data available"
          emptySubMessage="Try a different date or market"
          emptyIcon={<TrendingUp className="h-8 w-8" />}
          height="h-full min-h-[18rem]"
        >
          {isMobileRankingLayout ? (
            <RankingCardList
              virtual={virtual}
              shouldVirtualize={shouldVirtualize}
              onStockClick={onStockClick}
              showChange={showChange}
              isPeriodType={isPeriodType}
            />
          ) : (
            <RankingRowsTable
              virtual={virtual}
              shouldVirtualize={shouldVirtualize}
              onStockClick={onStockClick}
              showChange={showChange}
              isPeriodType={isPeriodType}
              columnCount={columnCount}
            />
          )}
        </DataStateWrapper>
      </div>
    </Surface>
  );
}
