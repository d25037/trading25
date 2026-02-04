import { ArrowDown, ArrowUp, ArrowUpDown, ChevronRight, Loader2, TrendingDown, TrendingUp } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { StockChart } from '@/components/Chart/StockChart';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useIndexData, useIndicesList } from '@/hooks/useIndices';
import { type SectorStockItem, useSectorStocks } from '@/hooks/useSectorStocks';
import { cn } from '@/lib/utils';
import { useChartStore } from '@/stores/chartStore';
import { useUiStore } from '@/stores/uiStore';
import type { IndexItem } from '@/types/indices';

type SortField = 'tradingValue' | 'changePercentage' | 'code';
type SortOrder = 'asc' | 'desc';

const CATEGORY_ORDER = ['topix', 'sector17', 'sector33', 'market', 'style', 'growth', 'reit'];

// Category display names
const CATEGORY_LABELS: Record<string, string> = {
  topix: 'TOPIX',
  sector33: '33 Sectors',
  sector17: 'TOPIX-17 Sectors',
  market: 'Market',
  growth: 'Growth',
  reit: 'REIT',
  style: 'Style',
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

interface SortableHeaderProps {
  field: SortField;
  currentField: SortField;
  order: SortOrder;
  onSort: (field: SortField) => void;
  children: React.ReactNode;
  align?: 'left' | 'right';
}

function SortableHeader({ field, currentField, order, onSort, children, align = 'left' }: SortableHeaderProps) {
  const isActive = field === currentField;
  return (
    <button
      type="button"
      onClick={() => onSort(field)}
      className={cn(
        'flex items-center gap-1 font-medium text-xs uppercase tracking-wider hover:text-foreground transition-colors',
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

// Get flat list of indices in category order
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
  for (const category of CATEGORY_ORDER) {
    const categoryGroup = groups[category];
    if (categoryGroup) {
      result.push(...categoryGroup);
    }
  }
  return result;
}

interface IndicesListProps {
  indices: IndexItem[];
  selectedCode: string | null;
  onSelect: (code: string) => void;
  isLoading: boolean;
  containerRef: React.RefObject<HTMLDivElement | null>;
}

function IndicesList({ indices, selectedCode, onSelect, isLoading, containerRef }: IndicesListProps) {
  // Group indices by category
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
      <div className="flex items-center justify-center h-32">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (indices.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-8">
        <TrendingUp className="h-12 w-12 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-center">No indices found</p>
        <p className="text-sm text-muted-foreground text-center mt-1">Run database sync to fetch index data.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3" ref={containerRef}>
      {CATEGORY_ORDER.filter((cat) => groupedIndices[cat]).map((category) => {
        const categoryIndices = groupedIndices[category];
        if (!categoryIndices) return null;
        return (
          <div key={category}>
            <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-1 px-1">
              {CATEGORY_LABELS[category] || category}
            </h3>
            <div className="space-y-1">
              {categoryIndices.map((index) => (
                <button
                  key={index.code}
                  type="button"
                  data-index-code={index.code}
                  onClick={() => onSelect(index.code)}
                  aria-label={`Select ${index.name}`}
                  aria-pressed={selectedCode === index.code}
                  className={cn(
                    'w-full text-left px-2 py-1.5 rounded-lg transition-all text-xs',
                    selectedCode === index.code ? 'gradient-primary text-white shadow-sm' : 'hover:bg-accent/50'
                  )}
                >
                  <div className="flex items-center justify-between gap-1">
                    <span className="font-medium truncate">{index.name}</span>
                    <ChevronRight className="h-3 w-3 flex-shrink-0 opacity-50" />
                  </div>
                </button>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

interface SectorStocksListProps {
  sectorName: string;
  sectorType: 'sector33' | 'sector17';
  onStockClick: (code: string) => void;
}

// Market code display labels
const MARKET_LABELS: Record<string, string> = {
  prime: 'P',
  standard: 'S',
  growth: 'G',
};

function SectorStocksList({ sectorName, sectorType, onStockClick }: SectorStocksListProps) {
  const [sortBy, setSortBy] = useState<SortField>('tradingValue');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [lookbackDays, setLookbackDays] = useState(5);
  const [markets, setMarkets] = useState('prime');

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
    <Card className="glass-panel overflow-hidden">
      <CardHeader className="border-b border-border/30 py-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">
            銘柄一覧
            {data?.stocks.length !== undefined && (
              <span className="text-sm font-normal text-muted-foreground ml-2">({data.stocks.length}銘柄)</span>
            )}
          </CardTitle>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">市場:</span>
              <Select value={markets} onValueChange={setMarkets}>
                <SelectTrigger className="h-7 w-32 text-xs">
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
              <span className="text-xs text-muted-foreground">比較期間:</span>
              <Select value={lookbackDays.toString()} onValueChange={(v) => setLookbackDays(Number(v))}>
                <SelectTrigger className="h-7 w-24 text-xs">
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
      </CardHeader>
      <CardContent className="p-0 max-h-[400px] overflow-auto">
        {isLoading ? (
          <div className="flex items-center justify-center h-32">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-32 text-destructive text-sm">{error.message}</div>
        ) : !data?.stocks.length ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground text-sm">
            銘柄が見つかりません
          </div>
        ) : (
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-background border-b z-10">
              <tr>
                <th className="text-left p-2 w-12">#</th>
                <th className="text-left p-2 w-20">
                  <SortableHeader field="code" currentField={sortBy} order={sortOrder} onSort={handleSort}>
                    コード
                  </SortableHeader>
                </th>
                <th className="text-center p-2 w-12">市場</th>
                <th className="text-left p-2">銘柄名</th>
                <th className="text-right p-2 w-24">現在値</th>
                <th className="text-right p-2 w-28">
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
                <th className="text-right p-2 w-24">
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
                    className="border-b border-border/30 hover:bg-accent/30 cursor-pointer transition-colors"
                    onClick={() => onStockClick(stock.code)}
                  >
                    <td className="p-2 text-muted-foreground tabular-nums">{stock.rank}</td>
                    <td className="p-2 font-medium">{stock.code}</td>
                    <td className="p-2 text-center text-muted-foreground">
                      {MARKET_LABELS[stock.marketCode] || stock.marketCode}
                    </td>
                    <td className="p-2 truncate max-w-[200px]">{stock.companyName}</td>
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
                        {isPositive && <TrendingUp className="h-3 w-3" />}
                        {isNegative && <TrendingDown className="h-3 w-3" />}
                        {formatChangePercentage(stock.changePercentage)}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}

interface IndexChartProps {
  code: string | null;
  indexInfo?: IndexItem;
  onStockClick: (code: string) => void;
}

function IndexChart({ code, indexInfo, onStockClick }: IndexChartProps) {
  const { data, isLoading, error } = useIndexData(code);

  const isSectorIndex = indexInfo?.category === 'sector33' || indexInfo?.category === 'sector17';
  const sectorType = indexInfo?.category as 'sector33' | 'sector17' | undefined;

  // Convert IndexDataPoint to StockDataPoint format
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

  // Get sector name (strip "TOPIX-17 " prefix for sector17)
  const sectorName = useMemo(() => {
    if (!data?.name || !sectorType) return null;
    return sectorType === 'sector17' ? data.name.replace(/^TOPIX-17 /, '') : data.name;
  }, [data?.name, sectorType]);

  if (!code) {
    return (
      <Card className="glass-panel">
        <CardContent className="flex flex-col items-center justify-center py-16">
          <TrendingUp className="h-16 w-16 text-muted-foreground mb-4" />
          <p className="text-muted-foreground text-lg">Select an index to view chart</p>
        </CardContent>
      </Card>
    );
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error) {
    return (
      <Card className="glass-panel">
        <CardContent className="py-8 text-center">
          <p className="text-destructive">Failed to load index data: {error.message}</p>
        </CardContent>
      </Card>
    );
  }

  if (!data) {
    return null;
  }

  const lastDataPoint = chartData.length > 0 ? chartData[chartData.length - 1] : null;
  const latestPrice = lastDataPoint?.close ?? null;

  return (
    <div className="space-y-4">
      {/* Index Header */}
      <div className="px-6 py-4 gradient-primary rounded-xl">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-white">{data.name}</h2>
            <p className="text-white/80">Code: {data.code}</p>
          </div>
          {latestPrice !== null && (
            <div className="text-right text-white">
              <p className="text-sm opacity-80">Latest</p>
              <p className="text-2xl font-bold tabular-nums">{latestPrice.toLocaleString()}</p>
            </div>
          )}
        </div>
      </div>

      {/* Chart */}
      <Card className="glass-panel overflow-hidden">
        <CardHeader className="border-b border-border/30">
          <CardTitle>Price Chart ({chartData.length} data points)</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="h-[400px]">
            <StockChart data={chartData} />
          </div>
        </CardContent>
      </Card>

      {/* Sector Stocks List */}
      {isSectorIndex && sectorType && sectorName && (
        <SectorStocksList sectorName={sectorName} sectorType={sectorType} onStockClick={onStockClick} />
      )}
    </div>
  );
}

export function IndicesPage() {
  const { selectedIndexCode, setSelectedIndexCode, setActiveTab } = useUiStore();
  const { setSelectedSymbol } = useChartStore();
  const { data: indicesData, isLoading: indicesLoading, error: indicesError } = useIndicesList();
  const listContainerRef = useRef<HTMLDivElement>(null);

  const handleStockClick = useCallback(
    (code: string) => {
      setSelectedSymbol(code);
      setActiveTab('charts');
    },
    [setSelectedSymbol, setActiveTab]
  );

  // Get the currently selected index info
  const selectedIndexInfo = useMemo(() => {
    if (!selectedIndexCode || !indicesData?.indices) return undefined;
    return indicesData.indices.find((idx) => idx.code === selectedIndexCode);
  }, [selectedIndexCode, indicesData?.indices]);

  // Get flat list of indices for keyboard navigation
  const flatIndices = useMemo(() => {
    if (!indicesData?.indices) return [];
    return getFlatIndicesList(indicesData.indices);
  }, [indicesData?.indices]);

  // Scroll selected item into view
  const scrollToSelected = useCallback((code: string) => {
    const container = listContainerRef.current;
    if (!container) return;

    const button = container.querySelector(`[data-index-code="${code}"]`);
    if (button) {
      button.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, []);

  // Calculate next index for keyboard navigation
  const getNextIndex = useCallback((direction: 'up' | 'down', currentIndex: number, length: number): number => {
    if (direction === 'down') {
      return currentIndex < length - 1 ? currentIndex + 1 : 0;
    }
    return currentIndex > 0 ? currentIndex - 1 : length - 1;
  }, []);

  // Handle keyboard navigation
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (flatIndices.length === 0) return;
      if (e.key !== 'ArrowUp' && e.key !== 'ArrowDown') return;

      e.preventDefault();

      const currentIndex = selectedIndexCode ? flatIndices.findIndex((item) => item.code === selectedIndexCode) : -1;

      const direction = e.key === 'ArrowDown' ? 'down' : 'up';
      const newIndex = getNextIndex(direction, currentIndex, flatIndices.length);
      const newItem = flatIndices[newIndex];

      if (newItem) {
        setSelectedIndexCode(newItem.code);
        scrollToSelected(newItem.code);
      }
    },
    [flatIndices, selectedIndexCode, setSelectedIndexCode, scrollToSelected, getNextIndex]
  );

  // Register keyboard event listener
  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  return (
    <div className="flex h-full overflow-hidden">
      {/* Indices List Sidebar */}
      <aside
        className="w-64 shrink-0 border-r border-border/30 overflow-y-auto p-4 glass-panel"
        style={{ minWidth: '16rem', maxWidth: '16rem' }}
      >
        <div className="mb-4">
          <div className="flex items-center gap-2 mb-2">
            <div className="gradient-primary rounded-lg p-1.5">
              <TrendingUp className="h-5 w-5 text-white" />
            </div>
            <h2 className="text-lg font-bold">Indices</h2>
          </div>
        </div>
        <IndicesList
          indices={indicesData?.indices || []}
          selectedCode={selectedIndexCode}
          onSelect={setSelectedIndexCode}
          isLoading={indicesLoading}
          containerRef={listContainerRef}
        />
        {indicesError && (
          <div className="mt-4 p-4 rounded-lg bg-destructive/10 text-destructive text-sm">
            Failed to load indices: {indicesError.message}
          </div>
        )}
      </aside>

      {/* Index Chart Area */}
      <main className="flex-1 min-w-0 p-6 overflow-y-auto">
        <IndexChart code={selectedIndexCode} indexInfo={selectedIndexInfo} onStockClick={handleStockClick} />
      </main>
    </div>
  );
}
