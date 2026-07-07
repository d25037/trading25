import type { RankingItem, WatchlistSummaryResponse } from '@trading25/contracts/types/api-response-types';
import { type CSSProperties, type ReactNode, useEffect, useMemo, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import {
  EQUITY_SORT_FIELDS,
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

const DEFAULT_RANKING_TABLE_SORT: RankingTableSortState = {
  field: 'tradingValue',
  order: 'desc',
};

const EQUITY_SORT_FIELD_SET = new Set<string>(EQUITY_SORT_FIELDS);

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
  initialSortState?: RankingTableSortState;
  sortStorageKey?: string;
  onSortChange?: (state: RankingTableSortState) => void;
  enableTableFilters?: boolean;
  filterState?: DailyRankingTableFilters;
  filterWatchlists?: WatchlistSummaryResponse[];
  filterWatchlistsLoading?: boolean;
  filterWatchlistsError?: Error | null;
  filterWatchlistCodes?: ReadonlySet<string>;
  onFilterChange?: (filters: DailyRankingTableFilters) => void;
  scrollRestorationKey?: string;
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

function isRankingTableSortState(value: unknown): value is RankingTableSortState {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<RankingTableSortState>;
  return (
    typeof candidate.field === 'string' &&
    EQUITY_SORT_FIELD_SET.has(candidate.field) &&
    (candidate.order === 'asc' || candidate.order === 'desc')
  );
}

function readStoredSortState(storageKey: string | undefined): RankingTableSortState | null {
  if (!storageKey || typeof window === 'undefined') return null;
  try {
    const storedValue = window.localStorage.getItem(storageKey);
    if (!storedValue) return null;
    const parsedValue = JSON.parse(storedValue) as unknown;
    return isRankingTableSortState(parsedValue) ? parsedValue : null;
  } catch {
    return null;
  }
}

function writeStoredSortState(storageKey: string | undefined, state: RankingTableSortState): void {
  if (!storageKey || typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(storageKey, JSON.stringify(state));
  } catch {
    // Storage can be unavailable in privacy modes; sorting should remain usable.
  }
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
  initialSortState,
  sortStorageKey,
  onSortChange,
  enableTableFilters = false,
  filterState = {},
  filterWatchlists = [],
  filterWatchlistsLoading = false,
  filterWatchlistsError = null,
  filterWatchlistCodes,
  onFilterChange,
  scrollRestorationKey,
}: RankingTableProps) {
  const defaultSortField = initialSortState?.field ?? DEFAULT_RANKING_TABLE_SORT.field;
  const defaultSortOrder = initialSortState?.order ?? DEFAULT_RANKING_TABLE_SORT.order;
  const defaultSortState = useMemo(
    () => ({ field: defaultSortField, order: defaultSortOrder }),
    [defaultSortField, defaultSortOrder]
  );
  const [localSortState, setLocalSortState] = useState<RankingTableSortState>(
    () => readStoredSortState(sortStorageKey) ?? defaultSortState
  );
  const isControlledSort = sortState != null;
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
    writeStoredSortState(sortStorageKey, nextState);
  };

  useEffect(() => {
    if (isControlledSort) return;
    setLocalSortState(readStoredSortState(sortStorageKey) ?? defaultSortState);
  }, [defaultSortState, isControlledSort, sortStorageKey]);

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
            {headerActions}
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
        scrollRestorationKey={scrollRestorationKey}
      />
    </Surface>
  );
}
