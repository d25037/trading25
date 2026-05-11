import { ArrowDownCircle, ArrowUpCircle, DollarSign, TrendingDown, TrendingUp } from 'lucide-react';
import { type CSSProperties, type ReactNode, useMemo, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import {
  type EquityRankingItem,
  type EquityRankingLabels,
  EquityRankingTable,
  type EquitySortField,
  type EquitySortOrder,
} from '@/components/Ranking/EquityRankingTable';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { Rankings, RankingType } from '@/types/ranking';

interface RankingTableProps {
  rankings: Rankings | undefined;
  isLoading: boolean;
  error: Error | null;
  onStockClick: (code: string) => void;
  periodDays?: number;
  title?: string;
  eyebrow?: string;
  showValuation?: boolean;
  showLiquidity?: boolean;
  showMarket?: boolean;
  showChangeForTradingValue?: boolean;
  enableColumnSort?: boolean;
  formatLargeValue?: (value: number | null | undefined) => string;
  labels?: Partial<EquityRankingLabels>;
  emptyMessage?: string;
  emptySubMessage?: string;
  headerActions?: ReactNode;
  className?: string;
  style?: CSSProperties;
  testId?: string;
}

const rankingTabs: { id: RankingType; label: string; icon: typeof DollarSign }[] = [
  { id: 'tradingValue', label: '売買代金', icon: DollarSign },
  { id: 'gainers', label: '値上がり', icon: TrendingUp },
  { id: 'losers', label: '値下がり', icon: TrendingDown },
  { id: 'periodHigh', label: '期間高値', icon: ArrowUpCircle },
  { id: 'periodLow', label: '期間安値', icon: ArrowDownCircle },
];

function getSortValue(item: EquityRankingItem, field: EquitySortField): number | string | null | undefined {
  if (field === 'tradingValue') {
    return item.tradingValue ?? item.tradingValueAverage;
  }
  if (field === 'changePercentage') {
    return item.changePercentage;
  }
  return item[field];
}

function isMissingSortValue(value: number | string | null | undefined): boolean {
  return value == null || (typeof value === 'number' && !Number.isFinite(value));
}

function comparePresentValues(aValue: number | string, bValue: number | string, direction: number): number {
  if (typeof aValue === 'string' || typeof bValue === 'string') {
    return String(aValue).localeCompare(String(bValue)) * direction;
  }
  if (aValue === bValue) return 0;
  return (aValue < bValue ? -1 : 1) * direction;
}

function sortEquityItems<T extends EquityRankingItem>(items: T[], field: EquitySortField, order: EquitySortOrder): T[] {
  const direction = order === 'desc' ? -1 : 1;
  return [...items].sort((a, b) => {
    const aValue = getSortValue(a, field);
    const bValue = getSortValue(b, field);
    const aMissing = isMissingSortValue(aValue);
    const bMissing = isMissingSortValue(bValue);
    if (aMissing && bMissing) return a.code.localeCompare(b.code);
    if (aMissing) return 1;
    if (bMissing) return -1;
    return (
      comparePresentValues(aValue as number | string, bValue as number | string, direction) ||
      a.code.localeCompare(b.code)
    );
  });
}

export function RankingTable({
  rankings,
  isLoading,
  error,
  onStockClick,
  periodDays,
  title = 'Market Rankings',
  eyebrow = 'Results',
  showValuation = false,
  showLiquidity = false,
  showMarket = false,
  showChangeForTradingValue = false,
  enableColumnSort = false,
  formatLargeValue,
  labels,
  emptyMessage,
  emptySubMessage,
  headerActions,
  className = 'flex min-h-[24rem] flex-1 flex-col overflow-hidden',
  style,
  testId,
}: RankingTableProps) {
  const [activeRankingType, setActiveRankingType] = useState<RankingType>('tradingValue');
  const [sortState, setSortState] = useState<{ field: EquitySortField; order: EquitySortOrder } | null>(null);
  const currentItems = rankings?.[activeRankingType] ?? [];
  const displayedItems = useMemo(() => {
    if (!enableColumnSort || sortState === null) {
      return currentItems;
    }
    return sortEquityItems(currentItems, sortState.field, sortState.order);
  }, [currentItems, enableColumnSort, sortState]);
  const showChange = showChangeForTradingValue || activeRankingType !== 'tradingValue';

  const getTabLabel = (tab: (typeof rankingTabs)[number]) => {
    if (tab.id === 'periodHigh') return `${periodDays || 250}日高値`;
    if (tab.id === 'periodLow') return `${periodDays || 250}日安値`;
    return tab.label;
  };
  const handleColumnSort = (field: EquitySortField) => {
    setSortState((current) => {
      if (current?.field === field) {
        return { field, order: current.order === 'desc' ? 'asc' : 'desc' };
      }
      return { field, order: 'desc' };
    });
  };

  return (
    <Surface className={className} style={style} data-testid={testId}>
      <div className="space-y-4 border-b border-border/70 px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="space-y-1">
            <SectionEyebrow>{eyebrow}</SectionEyebrow>
            <h2 className="text-base font-semibold text-foreground">
              {title}
              {displayedItems.length > 0 ? (
                <span className="ml-2 text-sm font-normal text-muted-foreground">({displayedItems.length})</span>
              ) : null}
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {headerActions}
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
      </div>

      <EquityRankingTable
        items={displayedItems}
        isLoading={isLoading}
        error={error}
        onStockClick={onStockClick}
        showChange={showChange}
        showValuation={showValuation}
        showLiquidity={showLiquidity}
        showMarket={showMarket}
        formatLargeValue={formatLargeValue}
        labels={labels}
        sortState={
          enableColumnSort
            ? {
                field: sortState?.field ?? (activeRankingType === 'tradingValue' ? 'tradingValue' : 'changePercentage'),
                order: sortState?.order ?? 'desc',
                onSort: handleColumnSort,
              }
            : undefined
        }
        emptyMessage={emptyMessage}
        emptySubMessage={emptySubMessage}
      />
    </Surface>
  );
}
