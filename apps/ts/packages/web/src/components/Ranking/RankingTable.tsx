import type { RankingItem, WatchlistSummaryResponse } from '@trading25/contracts/types/api-response-types';
import { type CSSProperties, type ReactNode, useMemo, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import {
  type EquityRankingItem,
  type EquityRankingLabels,
  EquityRankingTable,
  type EquitySortField,
  type EquitySortOrder,
} from '@/components/Ranking/EquityRankingTable';
import { RankingTableFilterDialog } from '@/components/Ranking/RankingTableFilterDialog';
import type { DailyRankingTableFilters } from '@/types/ranking';
import {
  countActiveDailyRankingTableFilters,
  filterDailyRankingItems,
  hasActiveDailyRankingTableFilters,
} from './rankingTableFilters';

export interface RankingTableSortState {
  field: EquitySortField;
  order: EquitySortOrder;
}

interface RankingTableProps {
  items: RankingItem[] | undefined;
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
  sortState?: RankingTableSortState;
  onSortChange?: (state: RankingTableSortState) => void;
  enableTableFilters?: boolean;
  filterState?: DailyRankingTableFilters;
  filterWatchlists?: WatchlistSummaryResponse[];
  filterWatchlistsLoading?: boolean;
  filterWatchlistsError?: Error | null;
  filterWatchlistCodes?: ReadonlySet<string>;
  onFilterChange?: (filters: DailyRankingTableFilters) => void;
}

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
  items,
  isLoading,
  error,
  onStockClick,
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
  sortState,
  onSortChange,
  enableTableFilters = false,
  filterState = {},
  filterWatchlists = [],
  filterWatchlistsLoading = false,
  filterWatchlistsError = null,
  filterWatchlistCodes,
  onFilterChange,
}: RankingTableProps) {
  const [localSortState, setLocalSortState] = useState<RankingTableSortState>({
    field: 'tradingValue',
    order: 'desc',
  });
  const activeSortState = sortState ?? localSortState;
  const currentItems = items ?? [];
  const activeFilterCount = countActiveDailyRankingTableFilters(filterState);
  const filtersActive = hasActiveDailyRankingTableFilters(filterState);
  const filteredItems = useMemo(() => {
    if (!enableTableFilters) {
      return currentItems;
    }
    return filterDailyRankingItems(currentItems, filterState, filterWatchlistCodes);
  }, [currentItems, enableTableFilters, filterState, filterWatchlistCodes]);
  const displayedItems = useMemo(() => {
    if (!enableColumnSort) {
      return filteredItems;
    }
    return sortEquityItems(filteredItems, activeSortState.field, activeSortState.order);
  }, [filteredItems, enableColumnSort, activeSortState]);
  const showChange = showChangeForTradingValue;
  const handleColumnSort = (field: EquitySortField) => {
    const nextState: RankingTableSortState =
      activeSortState.field === field
        ? { field, order: activeSortState.order === 'desc' ? 'asc' : 'desc' }
        : { field, order: 'desc' as const };
    if (onSortChange) {
      onSortChange(nextState);
      return;
    }
    setLocalSortState(nextState);
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
                <span className="ml-2 text-sm font-normal text-muted-foreground">
                  {filtersActive ? `(${displayedItems.length} / ${currentItems.length})` : `(${displayedItems.length})`}
                </span>
              ) : null}
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {enableTableFilters && onFilterChange ? (
              <RankingTableFilterDialog
                items={currentItems}
                filters={filterState}
                watchlists={filterWatchlists}
                watchlistsLoading={filterWatchlistsLoading}
                watchlistsError={filterWatchlistsError}
                onChange={onFilterChange}
              />
            ) : null}
            {activeFilterCount > 0 && onFilterChange ? (
              <button
                type="button"
                className="text-xs font-medium text-muted-foreground hover:text-foreground"
                onClick={() => onFilterChange({})}
              >
                Clear
              </button>
            ) : null}
            {headerActions}
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
                field: activeSortState.field,
                order: activeSortState.order,
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
