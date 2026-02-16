import { ArrowDown, ArrowUp, ArrowUpDown, Briefcase, TrendingUp } from 'lucide-react';
import { useNavigate } from '@tanstack/react-router';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { type HoldingPerformance, usePortfolioPerformance } from '@/hooks/usePortfolioPerformance';
import { useChartStore } from '@/stores/chartStore';
import type { PortfolioItem, PortfolioWithItems } from '@/types/portfolio';
import { getPositiveNegativeColor } from '@/utils/color-schemes';
import { formatRate } from '@/utils/formatters';
import { AddStockDialog } from './AddStockDialog';
import { BenchmarkChart } from './BenchmarkChart';
import { DeletePortfolioDialog } from './DeletePortfolioDialog';
import { DeleteStockDialog } from './DeleteStockDialog';
import { EditPortfolioDialog } from './EditPortfolioDialog';
import { EditStockDialog } from './EditStockDialog';
import { PerformanceSummary } from './PerformanceSummary';
import { PortfolioFactorRegressionPanel } from './PortfolioFactorRegressionPanel';

interface StockRowProps {
  item: PortfolioItem;
  performance: HoldingPerformance | undefined;
  onNavigateToChart: (code: string) => void;
}

function StockRow({ item, performance, onNavigateToChart }: StockRowProps) {
  const cost = item.quantity * item.purchasePrice;
  const currentPrice = performance?.currentPrice ?? item.purchasePrice;
  const marketValue = performance?.marketValue ?? cost;
  const pnl = performance?.pnl ?? 0;
  const returnRate = performance?.returnRate ?? 0;

  return (
    <tr className="border-b border-border/30 hover:bg-accent/30 transition-colors">
      <td className="py-3 px-4">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.code} ${item.companyName}`}
          className="flex items-center gap-2 text-primary hover:text-primary/80 font-medium transition-colors"
        >
          <TrendingUp className="h-4 w-4" />
          {item.code}
        </button>
      </td>
      <td className="py-3 px-4">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.companyName}`}
          className="text-left hover:text-primary transition-colors"
        >
          {item.companyName}
        </button>
      </td>
      <td className="py-3 px-4 text-right tabular-nums">{item.quantity.toLocaleString()}</td>
      <td className="py-3 px-4 text-right tabular-nums">{item.purchasePrice.toLocaleString()}</td>
      <td className="py-3 px-4 text-right tabular-nums">{currentPrice.toLocaleString()}</td>
      <td className="py-3 px-4 text-right tabular-nums">{marketValue.toLocaleString()}</td>
      <td className={`py-3 px-4 text-right font-medium tabular-nums ${getPositiveNegativeColor(pnl)}`}>
        {pnl >= 0 ? '+' : ''}
        {pnl.toLocaleString()}
      </td>
      <td className={`py-3 px-4 text-right tabular-nums ${getPositiveNegativeColor(returnRate)}`}>
        {formatRate(returnRate)}
      </td>
      <td className="py-3 px-2">
        <div className="flex items-center gap-1">
          <EditStockDialog item={item} />
          <DeleteStockDialog item={item} />
        </div>
      </td>
    </tr>
  );
}

interface PortfolioDetailProps {
  portfolio: PortfolioWithItems | undefined;
  isLoading: boolean;
  error: Error | null;
  onPortfolioDeleted?: () => void;
}

/**
 * Create lookup map for holding performance
 */
function createHoldingPerformanceMap(holdings: HoldingPerformance[] | undefined): Map<string, HoldingPerformance> {
  const map = new Map<string, HoldingPerformance>();
  if (holdings) {
    for (const holding of holdings) {
      map.set(holding.code, holding);
    }
  }
  return map;
}

/**
 * Empty portfolio selection state
 */
function EmptySelectionState(): ReactNode {
  return (
    <Card className="glass-panel">
      <CardContent className="flex flex-col items-center justify-center py-16">
        <Briefcase className="h-16 w-16 text-muted-foreground mb-4" />
        <p className="text-muted-foreground text-lg">Select a portfolio to view details</p>
      </CardContent>
    </Card>
  );
}

interface PortfolioHeaderProps {
  portfolio: PortfolioWithItems;
  currentValue: number;
  totalPnL: number;
  onPortfolioDeleted?: () => void;
}

/**
 * Portfolio header with name, value, and action buttons
 */
function PortfolioHeader({ portfolio, currentValue, totalPnL, onPortfolioDeleted }: PortfolioHeaderProps) {
  return (
    <div className="px-6 py-4 gradient-primary rounded-xl">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-white">{portfolio.name}</h2>
          {portfolio.description && <p className="text-white/80">{portfolio.description}</p>}
        </div>
        <div className="flex items-center gap-4">
          <div className="text-right text-white">
            <p className="text-sm opacity-80">Current Value</p>
            <p className="text-2xl font-bold tabular-nums">{currentValue.toLocaleString()}</p>
            {totalPnL !== 0 && (
              <p className={`text-sm ${totalPnL >= 0 ? 'text-green-300' : 'text-red-300'}`}>
                {totalPnL >= 0 ? '+' : ''}
                {totalPnL.toLocaleString()}
              </p>
            )}
          </div>
        </div>
      </div>
      <div className="flex items-center gap-2 mt-4">
        <AddStockDialog portfolioId={portfolio.id} />
        <EditPortfolioDialog portfolio={portfolio} />
        <DeletePortfolioDialog portfolio={portfolio} onSuccess={onPortfolioDeleted} />
      </div>
    </div>
  );
}

interface HoldingsTableProps {
  items: PortfolioItem[];
  holdingPerformanceMap: Map<string, HoldingPerformance>;
  onNavigateToChart: (code: string) => void;
}

/**
 * Sort configuration for holdings table
 */
type SortColumn =
  | 'code'
  | 'companyName'
  | 'quantity'
  | 'purchasePrice'
  | 'currentPrice'
  | 'marketValue'
  | 'pnl'
  | 'returnRate';
type SortDirection = 'asc' | 'desc';

interface SortConfig {
  column: SortColumn | null;
  direction: SortDirection;
}

interface SortableHeaderProps {
  column: SortColumn;
  label: string;
  sortConfig: SortConfig;
  onSort: (column: SortColumn) => void;
  align?: 'left' | 'right';
}

/**
 * Sortable table header with click-to-sort functionality
 */
function SortableHeader({ column, label, sortConfig, onSort, align = 'left' }: SortableHeaderProps) {
  const isActive = sortConfig.column === column;
  const Icon = isActive ? (sortConfig.direction === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown;

  return (
    <th className={`py-3 px-4 font-medium ${align === 'right' ? 'text-right' : 'text-left'}`}>
      <button
        type="button"
        onClick={() => onSort(column)}
        className={`inline-flex items-center gap-1 hover:text-primary transition-colors ${
          isActive ? 'text-primary' : ''
        }`}
      >
        {align === 'right' && <Icon className="h-3 w-3" />}
        {label}
        {align === 'left' && <Icon className="h-3 w-3" />}
      </button>
    </th>
  );
}

/**
 * Get sortable value for a given column
 */
function getSortValue(
  item: PortfolioItem,
  performance: HoldingPerformance | undefined,
  column: SortColumn
): string | number {
  switch (column) {
    case 'code':
      return item.code;
    case 'companyName':
      return item.companyName;
    case 'quantity':
      return item.quantity;
    case 'purchasePrice':
      return item.purchasePrice;
    case 'currentPrice':
      return performance?.currentPrice ?? item.purchasePrice;
    case 'marketValue':
      return performance?.marketValue ?? item.quantity * item.purchasePrice;
    case 'pnl':
      return performance?.pnl ?? 0;
    case 'returnRate':
      return performance?.returnRate ?? 0;
  }
}

/**
 * Holdings table showing all stocks in portfolio with sortable columns
 */
function HoldingsTable({ items, holdingPerformanceMap, onNavigateToChart }: HoldingsTableProps) {
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    column: null,
    direction: 'desc',
  });

  const handleSort = (column: SortColumn) => {
    setSortConfig((prev) => {
      if (prev.column === column) {
        return {
          column,
          direction: prev.direction === 'asc' ? 'desc' : 'asc',
        };
      }
      // Default to descending for numeric columns, ascending for text
      const defaultDirection = column === 'code' || column === 'companyName' ? 'asc' : 'desc';
      return { column, direction: defaultDirection };
    });
  };

  const sortedItems = useMemo(() => {
    if (!sortConfig.column) return items;

    return [...items].sort((a, b) => {
      const aValue = getSortValue(a, holdingPerformanceMap.get(a.code), sortConfig.column as SortColumn);
      const bValue = getSortValue(b, holdingPerformanceMap.get(b.code), sortConfig.column as SortColumn);

      let comparison = 0;
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        comparison = aValue.localeCompare(bValue, 'ja');
      } else {
        comparison = (aValue as number) - (bValue as number);
      }

      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
  }, [items, holdingPerformanceMap, sortConfig]);

  return (
    <Card className="glass-panel overflow-hidden">
      <CardHeader className="border-b border-border/30">
        <CardTitle>Holdings ({items.length} stocks)</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        {items.length === 0 ? (
          <div className="py-8 text-center text-muted-foreground">
            <Briefcase className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No stocks in this portfolio</p>
            <p className="text-sm mt-1">Click "Add Stock" above to add your first holding.</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-muted/30">
                <tr>
                  <SortableHeader column="code" label="Code" sortConfig={sortConfig} onSort={handleSort} />
                  <SortableHeader column="companyName" label="Company" sortConfig={sortConfig} onSort={handleSort} />
                  <SortableHeader
                    column="quantity"
                    label="Qty"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    align="right"
                  />
                  <SortableHeader
                    column="purchasePrice"
                    label="Avg Cost"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    align="right"
                  />
                  <SortableHeader
                    column="currentPrice"
                    label="Current"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    align="right"
                  />
                  <SortableHeader
                    column="marketValue"
                    label="Value"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    align="right"
                  />
                  <SortableHeader column="pnl" label="P&L" sortConfig={sortConfig} onSort={handleSort} align="right" />
                  <SortableHeader
                    column="returnRate"
                    label="Return"
                    sortConfig={sortConfig}
                    onSort={handleSort}
                    align="right"
                  />
                  <th className="py-3 px-2 text-center font-medium w-20">Actions</th>
                </tr>
              </thead>
              <tbody>
                {sortedItems.map((item) => (
                  <StockRow
                    key={item.id}
                    item={item}
                    performance={holdingPerformanceMap.get(item.code)}
                    onNavigateToChart={onNavigateToChart}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function PortfolioDetail({ portfolio, isLoading, error, onPortfolioDeleted }: PortfolioDetailProps) {
  const navigate = useNavigate();
  const { setSelectedSymbol } = useChartStore();

  const { data: performanceData, isLoading: isPerformanceLoading } = usePortfolioPerformance(portfolio?.id ?? null);

  const holdingPerformanceMap = createHoldingPerformanceMap(performanceData?.holdings);

  const handleNavigateToChart = (code: string) => {
    setSelectedSymbol(code);
    void navigate({ to: '/charts' });
  };

  if (!portfolio && !isLoading && !error) {
    return <EmptySelectionState />;
  }

  return (
    <DataStateWrapper isLoading={isLoading} error={error} height="h-64">
      {portfolio && (
        <PortfolioDetailContent
          portfolio={portfolio}
          performanceData={performanceData}
          isPerformanceLoading={isPerformanceLoading}
          holdingPerformanceMap={holdingPerformanceMap}
          onNavigateToChart={handleNavigateToChart}
          onPortfolioDeleted={onPortfolioDeleted}
        />
      )}
    </DataStateWrapper>
  );
}

interface PortfolioDetailContentProps {
  portfolio: PortfolioWithItems;
  performanceData: ReturnType<typeof usePortfolioPerformance>['data'];
  isPerformanceLoading: boolean;
  holdingPerformanceMap: Map<string, HoldingPerformance>;
  onNavigateToChart: (code: string) => void;
  onPortfolioDeleted?: () => void;
}

function PortfolioDetailContent({
  portfolio,
  performanceData,
  isPerformanceLoading,
  holdingPerformanceMap,
  onNavigateToChart,
  onPortfolioDeleted,
}: PortfolioDetailContentProps): ReactNode {
  const currentValue =
    performanceData?.summary.currentValue ??
    portfolio.items.reduce((sum, item) => sum + item.quantity * item.purchasePrice, 0);
  const totalPnL = performanceData?.summary.totalPnL ?? 0;

  return (
    <div className="space-y-4">
      <PortfolioHeader
        portfolio={portfolio}
        currentValue={currentValue}
        totalPnL={totalPnL}
        onPortfolioDeleted={onPortfolioDeleted}
      />

      {portfolio.items.length > 0 && performanceData && (
        <PerformanceSummary
          summary={performanceData.summary}
          benchmark={performanceData.benchmark}
          isLoading={isPerformanceLoading}
        />
      )}

      <HoldingsTable
        items={portfolio.items}
        holdingPerformanceMap={holdingPerformanceMap}
        onNavigateToChart={onNavigateToChart}
      />

      {performanceData?.benchmarkTimeSeries && performanceData.benchmarkTimeSeries.length > 0 && (
        <Card className="glass-panel">
          <CardHeader className="border-b border-border/30">
            <CardTitle>Performance vs {performanceData.benchmark?.name ?? 'TOPIX'}</CardTitle>
          </CardHeader>
          <CardContent className="pt-4">
            <BenchmarkChart
              data={performanceData.benchmarkTimeSeries}
              benchmarkName={performanceData.benchmark?.name ?? 'TOPIX'}
            />
          </CardContent>
        </Card>
      )}

      {portfolio.items.length >= 2 && (
        <Card className="glass-panel">
          <CardHeader className="border-b border-border/30">
            <CardTitle>Factor Regression Analysis</CardTitle>
          </CardHeader>
          <CardContent className="pt-4">
            <PortfolioFactorRegressionPanel portfolioId={portfolio.id} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
