import { useNavigate } from '@tanstack/react-router';
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronRight, Loader2, TrendingDown, TrendingUp } from 'lucide-react';
import { type CSSProperties, type ReactNode, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LinePriceChart } from '@/components/Chart/LinePriceChart';
import { StockChart } from '@/components/Chart/StockChart';
import { SectionEyebrow, SplitLayout, SplitMain, SplitSidebar, Surface } from '@/components/Layout/Workspace';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useIndexData, useIndicesList } from '@/hooks/useIndices';
import { useIndicesRouteState, useMigrateIndicesRouteState } from '@/hooks/usePageRouteState';
import { type SectorStockItem, useSectorStocks } from '@/hooks/useSectorStocks';
import { INDEX_CATEGORY_LABELS, INDEX_CATEGORY_ORDER } from '@/lib/indexCategories';
import { cn } from '@/lib/utils';
import type { IndexItem } from '@/types/indices';

type SortField = 'tradingValue' | 'changePercentage' | 'code';
type SortOrder = 'asc' | 'desc';

const BENCHMARK_DISPLAY_ORDER: Record<string, number> = {
  N225_UNDERPX: 0,
  NT_RATIO: 1,
};

const MARKET_LABELS: Record<string, string> = {
  prime: 'P',
  standard: 'S',
  growth: 'G',
};

function formatNumber(value: number | undefined): string {
  if (value === undefined || value === null) return '-';
  return value.toLocaleString();
}

function formatTradingValue(value: number | undefined): string {
  if (value === undefined || value === null) return '-';
  if (value >= 1e12) return `${(value / 1e12).toFixed(2)}兆`;
  if (value >= 1e8) return `${(value / 1e8).toFixed(0)}億`;
  if (value >= 1e4) return `${(value / 1e4).toFixed(0)}万`;
  return value.toLocaleString();
}

function formatChangePercentage(value: number | undefined): string {
  if (value === undefined || value === null) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatLatestIndexValue(value: number | null, code: string | null | undefined): string {
  if (value === undefined || value === null) return '-';
  if (code === 'NT_RATIO') {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }
  return value.toLocaleString();
}

function reorderCategoryIndices(category: string, indices: IndexItem[]): IndexItem[] {
  if (category !== 'synthetic' || indices.length < 2) {
    return indices;
  }

  return indices
    .map((index, position) => ({ index, position }))
    .sort((left, right) => {
      const leftPriority = BENCHMARK_DISPLAY_ORDER[left.index.code] ?? Number.MAX_SAFE_INTEGER;
      const rightPriority = BENCHMARK_DISPLAY_ORDER[right.index.code] ?? Number.MAX_SAFE_INTEGER;
      if (leftPriority !== rightPriority) {
        return leftPriority - rightPriority;
      }
      return left.position - right.position;
    })
    .map(({ index }) => index);
}

function getFlatIndicesList(indices: IndexItem[]): IndexItem[] {
  const groups: Record<string, IndexItem[]> = {};
  for (const index of indices) {
    const category = index.category;
    const group = groups[category];
    if (group) {
      group.push(index);
    } else {
      groups[category] = [index];
    }
  }

  const result: IndexItem[] = [];
  for (const category of INDEX_CATEGORY_ORDER) {
    const categoryGroup = groups[category];
    if (categoryGroup) {
      result.push(...reorderCategoryIndices(category, categoryGroup));
    }
  }
  return result;
}

function useObservedElementHeight<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [height, setHeight] = useState<number | null>(null);

  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return;
    }

    const updateHeight = () => {
      const nextHeight = Math.round(element.getBoundingClientRect().height);
      setHeight((current) => (current === nextHeight ? current : nextHeight));
    };

    updateHeight();

    if (typeof ResizeObserver === 'undefined') {
      return;
    }

    const observer = new ResizeObserver(() => {
      updateHeight();
    });

    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  return [ref, height] as const;
}

interface SortableHeaderProps {
  field: SortField;
  currentField: SortField;
  order: SortOrder;
  onSort: (field: SortField) => void;
  children: ReactNode;
  align?: 'left' | 'right';
}

function CompactMetaStrip({
  items,
  className,
}: {
  items: readonly { label: string; value: string }[];
  className?: string;
}) {
  if (items.length === 0) {
    return null;
  }

  return (
    <dl className={cn('flex min-w-0 flex-nowrap items-center gap-2 overflow-x-auto pb-1', className)}>
      {items.map((item) => (
        <div
          key={item.label}
          className="flex shrink-0 items-baseline gap-2 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-3 py-1.5"
        >
          <dt className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            {item.label}
          </dt>
          <dd className="max-w-[14rem] truncate text-sm font-medium text-foreground">{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}

function SortableHeader({ field, currentField, order, onSort, children, align = 'left' }: SortableHeaderProps) {
  const isActive = field === currentField;
  return (
    <button
      type="button"
      onClick={() => onSort(field)}
      className={cn(
        'flex items-center gap-1 text-xs font-medium uppercase tracking-wider transition-colors hover:text-foreground',
        isActive ? 'text-foreground' : 'text-muted-foreground',
        align === 'right' && 'justify-end w-full'
      )}
    >
      {children}
      {isActive ? (
        order === 'desc' ? (
          <ArrowDown className="h-3 w-3" />
        ) : (
          <ArrowUp className="h-3 w-3" />
        )
      ) : (
        <ArrowUpDown className="h-3 w-3 opacity-50" />
      )}
    </button>
  );
}

interface IndicesListProps {
  indices: IndexItem[];
  selectedCode: string | null;
  onSelect: (code: string) => void;
  isLoading: boolean;
  containerRef: RefObject<HTMLDivElement | null>;
}

function IndicesList({ indices, selectedCode, onSelect, isLoading, containerRef }: IndicesListProps) {
  const groupedIndices = useMemo(() => {
    const groups: Record<string, IndexItem[]> = {};
    for (const index of indices) {
      const category = index.category;
      if (!groups[category]) {
        groups[category] = [];
      }
      groups[category].push(index);
    }
    return groups;
  }, [indices]);

  if (isLoading) {
    return (
      <div ref={containerRef} className="min-h-0 flex-1 overflow-auto">
        <div className="flex h-full min-h-[20rem] items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      </div>
    );
  }

  if (indices.length === 0) {
    return (
      <div ref={containerRef} className="min-h-0 flex-1 overflow-auto">
        <div className="flex min-h-[20rem] flex-col items-center justify-center px-4 text-center">
          <TrendingUp className="mb-4 h-10 w-10 text-muted-foreground" />
          <p className="text-sm font-medium text-foreground">No indices found</p>
          <p className="mt-1 text-xs text-muted-foreground">Run database sync to fetch index data.</p>
        </div>
      </div>
    );
  }

  return (
    <div ref={containerRef} className="min-h-0 flex-1 overflow-auto pr-1">
      <div className="space-y-4">
        {INDEX_CATEGORY_ORDER.filter((category) => groupedIndices[category]).map((category) => {
          const rawCategoryIndices = groupedIndices[category];
          if (!rawCategoryIndices) return null;
          const categoryIndices = reorderCategoryIndices(category, rawCategoryIndices);

          return (
            <section key={category} className="space-y-1.5">
              <SectionEyebrow className="px-1">
                {INDEX_CATEGORY_LABELS[category] ?? category}
              </SectionEyebrow>
              <div className="space-y-1">
                {categoryIndices.map((index) => {
                  const isSelected = selectedCode === index.code;

                  return (
                    <button
                      key={index.code}
                      type="button"
                      data-index-code={index.code}
                      onClick={() => onSelect(index.code)}
                      aria-label={`Select ${index.name}`}
                      aria-pressed={isSelected}
                      className={cn(
                        'app-interactive flex w-full items-center justify-between gap-2 rounded-xl border px-3 py-2 text-left text-xs transition-colors',
                        isSelected
                          ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                          : 'border-transparent text-foreground hover:border-border/60 hover:bg-[var(--app-surface-muted)]'
                      )}
                    >
                      <span className="truncate font-medium">{index.name}</span>
                      <ChevronRight className="h-3 w-3 shrink-0 opacity-50" />
                    </button>
                  );
                })}
              </div>
            </section>
          );
        })}
      </div>
    </div>
  );
}

interface SectorStocksListProps {
  sectorName: string;
  sectorType: 'sector33' | 'sector17';
  onStockClick: (code: string) => void;
  panelMinHeight?: number | null;
}

function SectorStocksList({ sectorName, sectorType, onStockClick, panelMinHeight }: SectorStocksListProps) {
  const [sortBy, setSortBy] = useState<SortField>('tradingValue');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [lookbackDays, setLookbackDays] = useState(5);
  const [markets, setMarkets] = useState('prime');
  const resolvedPanelMinHeight = panelMinHeight ? Math.max(panelMinHeight, 320) : 280;
  const panelStyle: CSSProperties = panelMinHeight
    ? {
        height: `${resolvedPanelMinHeight}px`,
        minHeight: `${resolvedPanelMinHeight}px`,
      }
    : {
        minHeight: `${resolvedPanelMinHeight}px`,
      };

  const params = sectorType === 'sector33' ? { sector33Name: sectorName } : { sector17Name: sectorName };

  const { data, isLoading, error } = useSectorStocks(
    {
      ...params,
      markets,
      sortBy,
      sortOrder,
      lookbackDays,
      limit: 100,
    },
    true
  );

  const handleSort = (field: SortField) => {
    if (field === sortBy) {
      setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc');
    } else {
      setSortBy(field);
      setSortOrder('desc');
    }
  };

  return (
    <Surface className="flex flex-col overflow-hidden lg:min-h-0" style={panelStyle}>
      <div className="border-b border-border/70 px-4 py-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-1">
            <SectionEyebrow>Constituents</SectionEyebrow>
            <h3 className="text-base font-semibold text-foreground">
              銘柄一覧
              {data?.stocks.length !== undefined ? (
                <span className="ml-2 text-sm font-normal text-muted-foreground">({data.stocks.length}銘柄)</span>
              ) : null}
            </h3>
            <p className="text-xs text-muted-foreground">
              {sectorName} constituent view sorted by trading value, change, or code.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">市場</span>
              <Select value={markets} onValueChange={setMarkets}>
                <SelectTrigger className="h-8 w-36 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="prime">プライム</SelectItem>
                  <SelectItem value="standard">スタンダード</SelectItem>
                  <SelectItem value="growth">グロース</SelectItem>
                  <SelectItem value="prime,standard">P + S</SelectItem>
                  <SelectItem value="prime,standard,growth">全市場</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">比較期間</span>
              <Select value={lookbackDays.toString()} onValueChange={(value) => setLookbackDays(Number(value))}>
                <SelectTrigger className="h-8 w-28 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">前日比</SelectItem>
                  <SelectItem value="5">5営業日</SelectItem>
                  <SelectItem value="10">10営業日</SelectItem>
                  <SelectItem value="20">20営業日</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
      </div>

      <div className="min-h-[16rem] flex-1 overflow-auto">
        {isLoading ? (
          <div className="flex h-40 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <div className="flex h-40 items-center justify-center px-4 text-center text-sm text-destructive">
            {error.message}
          </div>
        ) : !data?.stocks.length ? (
          <div className="flex h-40 items-center justify-center px-4 text-center text-sm text-muted-foreground">
            銘柄が見つかりません
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead className="sticky top-0 z-10 border-b bg-[var(--app-surface-muted)]">
              <tr>
                <th className="w-12 p-2 text-left">#</th>
                <th className="w-20 p-2 text-left">
                  <SortableHeader field="code" currentField={sortBy} order={sortOrder} onSort={handleSort}>
                    コード
                  </SortableHeader>
                </th>
                <th className="w-12 p-2 text-center">市場</th>
                <th className="p-2 text-left">銘柄名</th>
                <th className="w-24 p-2 text-right">現在値</th>
                <th className="w-28 p-2 text-right">
                  <SortableHeader
                    field="tradingValue"
                    currentField={sortBy}
                    order={sortOrder}
                    onSort={handleSort}
                    align="right"
                  >
                    売買代金(15日)
                  </SortableHeader>
                </th>
                <th className="w-24 p-2 text-right">
                  <SortableHeader
                    field="changePercentage"
                    currentField={sortBy}
                    order={sortOrder}
                    onSort={handleSort}
                    align="right"
                  >
                    騰落率
                  </SortableHeader>
                </th>
              </tr>
            </thead>
            <tbody>
              {data.stocks.map((stock: SectorStockItem) => {
                const isPositive = (stock.changePercentage ?? 0) > 0;
                const isNegative = (stock.changePercentage ?? 0) < 0;

                return (
                  <tr
                    key={stock.code}
                    className="cursor-pointer border-b border-border/30 transition-colors hover:bg-[var(--app-surface-muted)]"
                    onClick={() => onStockClick(stock.code)}
                  >
                    <td className="p-2 text-muted-foreground tabular-nums">{stock.rank}</td>
                    <td className="p-2 font-medium">{stock.code}</td>
                    <td className="p-2 text-center text-muted-foreground">
                      {MARKET_LABELS[stock.marketCode] || stock.marketCode}
                    </td>
                    <td className="max-w-[200px] truncate p-2">{stock.companyName}</td>
                    <td className="p-2 text-right tabular-nums">{formatNumber(stock.currentPrice)}</td>
                    <td className="p-2 text-right tabular-nums">{formatTradingValue(stock.tradingValue)}</td>
                    <td
                      className={cn(
                        'p-2 text-right tabular-nums',
                        isPositive && 'text-green-600 dark:text-green-400',
                        isNegative && 'text-red-600 dark:text-red-400'
                      )}
                    >
                      <span className="flex items-center justify-end gap-1">
                        {isPositive ? <TrendingUp className="h-3 w-3" /> : null}
                        {isNegative ? <TrendingDown className="h-3 w-3" /> : null}
                        {formatChangePercentage(stock.changePercentage)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </Surface>
  );
}

interface IndexChartProps {
  code: string | null;
  indexInfo?: IndexItem;
  onStockClick: (code: string) => void;
  panelMinHeight?: number | null;
}

function IndexChart({ code, indexInfo, onStockClick, panelMinHeight }: IndexChartProps) {
  const { data, isLoading, error } = useIndexData(code);

  const isSectorIndex = indexInfo?.category === 'sector33' || indexInfo?.category === 'sector17';
  const isSyntheticIndex = indexInfo?.category === 'synthetic';
  const isNtRatioIndex = code === 'NT_RATIO';
  const sectorType = indexInfo?.category as 'sector33' | 'sector17' | undefined;
  const resolvedWorkspaceMinHeight = panelMinHeight ? Math.max(panelMinHeight, 432) : 432;
  const chartPanelMinHeight = isSectorIndex ? Math.max(resolvedWorkspaceMinHeight, 520) : resolvedWorkspaceMinHeight;
  const sectorPanelMinHeight = isSectorIndex
    ? Math.max(Math.round(resolvedWorkspaceMinHeight * 0.55), 320)
    : null;
  const workspaceStyle: CSSProperties = { minHeight: `${resolvedWorkspaceMinHeight}px` };
  const chartPanelStyle: CSSProperties = { minHeight: `${chartPanelMinHeight}px` };

  const chartData = useMemo(() => {
    if (!data?.data) return [];
    return data.data.map((point) => ({
      time: point.date,
      open: point.open,
      high: point.high,
      low: point.low,
      close: point.close,
    }));
  }, [data]);

  const lineData = useMemo(() => {
    if (!data?.data) return [];
    return data.data.map((point) => ({
      time: point.date,
      value: point.close,
    }));
  }, [data]);

  const sectorName = useMemo(() => {
    if (!data?.name || !sectorType) return null;
    return sectorType === 'sector17' ? data.name.replace(/^TOPIX-17 /, '') : data.name;
  }, [data?.name, sectorType]);

  if (!code) {
    return (
      <Surface className="flex flex-col overflow-hidden" style={{ minHeight: `${resolvedWorkspaceMinHeight}px` }}>
        <div className="border-b border-border/70 px-4 py-3">
          <SectionEyebrow>Results</SectionEyebrow>
          <h2 className="mt-1 text-xl font-semibold tracking-tight text-foreground">Index Workspace</h2>
        </div>
        <div className="flex min-h-[24rem] flex-1 flex-col items-center justify-center px-6 text-center">
          <TrendingUp className="mb-4 h-14 w-14 text-muted-foreground" />
          <p className="text-lg font-medium text-foreground">Select an index to view chart</p>
        </div>
      </Surface>
    );
  }

  if (isLoading) {
    return (
      <Surface className="flex flex-col overflow-hidden" style={{ minHeight: `${resolvedWorkspaceMinHeight}px` }}>
        <div className="flex min-h-[24rem] flex-1 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </Surface>
    );
  }

  if (error) {
    return (
      <Surface className="flex flex-col overflow-hidden" style={{ minHeight: `${resolvedWorkspaceMinHeight}px` }}>
        <div className="flex min-h-[24rem] flex-1 items-center justify-center px-6 text-center">
          <p className="text-sm text-destructive">Failed to load index data: {error.message}</p>
        </div>
      </Surface>
    );
  }

  if (!data) {
    return null;
  }

  const lastDataPoint = chartData.length > 0 ? chartData[chartData.length - 1] : null;
  const latestPrice = lastDataPoint?.close ?? lineData[lineData.length - 1]?.value ?? null;
  const chartTypeLabel = isSyntheticIndex ? 'Line reference' : 'OHLC bars';
  const categoryLabel = indexInfo
    ? indexInfo.category === 'synthetic'
      ? 'Synthetic'
      : (INDEX_CATEGORY_LABELS[indexInfo.category] ?? indexInfo.category)
    : 'Index';

  return (
    <div className="space-y-3" style={workspaceStyle}>
      <Surface className="flex flex-col overflow-hidden lg:min-h-0" style={chartPanelStyle}>
        <div className="border-b border-border/70 px-4 py-4">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div className="min-w-0 space-y-2">
              <SectionEyebrow>Results</SectionEyebrow>
              <div className="space-y-1">
                <h2 className="truncate text-2xl font-semibold tracking-tight text-foreground">{data.name}</h2>
                <CompactMetaStrip
                  items={[
                    { label: 'Code', value: data.code },
                    { label: 'Series', value: chartTypeLabel },
                  ]}
                  className="max-w-full"
                />
              </div>
              {isSyntheticIndex ? (
                <p className="text-xs text-muted-foreground">
                  {isNtRatioIndex
                    ? 'Nikkei 225 close / TOPIX close from local market snapshot'
                    : 'UnderPx derived daily reference series'}
                </p>
              ) : null}
            </div>
            <div className="shrink-0 rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3">
              <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Latest</p>
              <p className="mt-1 whitespace-nowrap text-xl font-semibold tracking-tight text-foreground tabular-nums">
                {formatLatestIndexValue(latestPrice, data.code)}
              </p>
              <p className="mt-1 whitespace-nowrap text-xs text-muted-foreground">{categoryLabel}</p>
            </div>
          </div>
        </div>

        <div className="border-b border-border/70 px-4 py-3">
          <SectionEyebrow>Chart</SectionEyebrow>
          <h3 className="mt-1 text-sm font-semibold text-foreground">
            Price Chart ({isSyntheticIndex ? lineData.length : chartData.length} data points)
          </h3>
        </div>

        <div className="min-h-[24rem] flex-1 p-0">
          <div className="h-full min-h-[24rem]">
            {isSyntheticIndex ? <LinePriceChart data={lineData} /> : <StockChart data={chartData} />}
          </div>
        </div>
      </Surface>

      {isSectorIndex && sectorType && sectorName ? (
        <SectorStocksList
          sectorName={sectorName}
          sectorType={sectorType}
          onStockClick={onStockClick}
          panelMinHeight={sectorPanelMinHeight}
        />
      ) : null}
    </div>
  );
}

export function IndicesPage() {
  useMigrateIndicesRouteState();
  const navigate = useNavigate();
  const { selectedIndexCode, setSelectedIndexCode } = useIndicesRouteState();
  const { data: indicesData, isLoading: indicesLoading, error: indicesError } = useIndicesList();
  const listContainerRef = useRef<HTMLDivElement>(null);
  const [sidebarRef, sidebarHeight] = useObservedElementHeight<HTMLDivElement>();

  const handleStockClick = useCallback(
    (code: string) => {
      void navigate({ to: '/charts', search: { symbol: code } });
    },
    [navigate]
  );

  const selectedIndexInfo = useMemo(() => {
    if (!selectedIndexCode || !indicesData?.indices) return undefined;
    return indicesData.indices.find((index) => index.code === selectedIndexCode);
  }, [indicesData?.indices, selectedIndexCode]);

  const flatIndices = useMemo(() => {
    if (!indicesData?.indices) return [];
    return getFlatIndicesList(indicesData.indices);
  }, [indicesData?.indices]);

  const scrollToSelected = useCallback((code: string) => {
    const container = listContainerRef.current;
    if (!container) return;

    const button = container.querySelector(`[data-index-code="${code}"]`);
    if (button) {
      button.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, []);

  const getNextIndex = useCallback((direction: 'up' | 'down', currentIndex: number, length: number): number => {
    if (direction === 'down') {
      return currentIndex < length - 1 ? currentIndex + 1 : 0;
    }
    return currentIndex > 0 ? currentIndex - 1 : length - 1;
  }, []);

  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      if (flatIndices.length === 0) return;
      if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;

      event.preventDefault();

      const currentIndex = selectedIndexCode ? flatIndices.findIndex((item) => item.code === selectedIndexCode) : -1;
      const direction = event.key === 'ArrowDown' ? 'down' : 'up';
      const nextIndex = getNextIndex(direction, currentIndex, flatIndices.length);
      const nextItem = flatIndices[nextIndex];

      if (nextItem) {
        setSelectedIndexCode(nextItem.code);
        scrollToSelected(nextItem.code);
      }
    },
    [flatIndices, getNextIndex, scrollToSelected, selectedIndexCode, setSelectedIndexCode]
  );

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  const introMetaItems = selectedIndexInfo
    ? [{ label: 'Selected', value: selectedIndexInfo.code }]
    : [{ label: 'Selected', value: 'Choose an index' }];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:overflow-hidden">
      <Surface className="px-4 py-3">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <SectionEyebrow>Analytics Workspace</SectionEyebrow>
            <div className="space-y-1">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Indices</h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Benchmarks, TOPIX families, sector baskets, and style indices from the local market snapshot.
              </p>
            </div>
          </div>
          <CompactMetaStrip items={introMetaItems} />
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
        <SplitSidebar className="w-full lg:flex lg:h-full lg:w-[18rem] lg:overflow-hidden xl:w-[19rem] 2xl:w-[20rem]">
          <div ref={sidebarRef} className="flex h-full min-h-0 flex-col">
            <Surface className="flex h-full min-h-[24rem] flex-col overflow-hidden p-3">
              <div className="space-y-1 border-b border-border/70 pb-3">
                <SectionEyebrow>Workspace</SectionEyebrow>
                <h2 className="text-sm font-semibold text-foreground">Index List</h2>
                <p className="text-xs text-muted-foreground">
                  Keyboard arrows move through the current category order and keep the selected index in view.
                </p>
              </div>

              <div className="flex min-h-0 flex-1 flex-col pt-3">
                <IndicesList
                  indices={indicesData?.indices || []}
                  selectedCode={selectedIndexCode}
                  onSelect={setSelectedIndexCode}
                  isLoading={indicesLoading}
                  containerRef={listContainerRef}
                />
              </div>

              {indicesError ? (
                <div className="mt-3 rounded-xl border border-red-500/20 bg-red-500/8 px-3 py-2 text-sm text-destructive">
                  Failed to load indices: {indicesError.message}
                </div>
              ) : null}
            </Surface>
          </div>
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-y-auto lg:pr-1">
          <IndexChart
            code={selectedIndexCode}
            indexInfo={selectedIndexInfo}
            onStockClick={handleStockClick}
            panelMinHeight={sidebarHeight}
          />
        </SplitMain>
      </SplitLayout>
    </div>
  );
}
