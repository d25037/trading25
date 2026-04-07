import { useNavigate } from '@tanstack/react-router';
import { ArrowDown, ArrowUp, ArrowUpDown, Briefcase, TrendingUp } from 'lucide-react';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import { CompactMetric, SectionEyebrow, SectionHeading, Surface } from '@/components/Layout/Workspace';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { type HoldingPerformance, usePortfolioPerformance } from '@/hooks/usePortfolioPerformance';
import type { PortfolioItem, PortfolioWithItems } from '@/types/portfolio';
import { getPositiveNegativeColor } from '@/utils/color-schemes';
import { formatCurrency, formatRate } from '@/utils/formatters';
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
    <tr className="border-b border-border/50 transition-colors hover:bg-[var(--app-surface-muted)]">
      <td className="px-4 py-3">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.code} ${item.companyName}`}
          className="flex items-center gap-2 font-medium text-primary transition-colors hover:text-primary/80"
        >
          <TrendingUp className="h-4 w-4" />
          {item.code}
        </button>
      </td>
      <td className="px-4 py-3">
        <button
          type="button"
          onClick={() => onNavigateToChart(item.code)}
          aria-label={`View chart for ${item.companyName}`}
          className="text-left transition-colors hover:text-primary"
        >
          {item.companyName}
        </button>
      </td>
      <td className="px-4 py-3 text-right tabular-nums">{item.quantity.toLocaleString()}</td>
      <td className="px-4 py-3 text-right tabular-nums">{item.purchasePrice.toLocaleString()}</td>
      <td className="px-4 py-3 text-right tabular-nums">{currentPrice.toLocaleString()}</td>
      <td className="px-4 py-3 text-right tabular-nums">{marketValue.toLocaleString()}</td>
      <td className={`px-4 py-3 text-right font-medium tabular-nums ${getPositiveNegativeColor(pnl)}`}>
        {pnl >= 0 ? '+' : ''}
        {pnl.toLocaleString()}
      </td>
      <td className={`px-4 py-3 text-right tabular-nums ${getPositiveNegativeColor(returnRate)}`}>
        {formatRate(returnRate)}
      </td>
      <td className="px-2 py-3">
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

function createHoldingPerformanceMap(holdings: HoldingPerformance[] | undefined): Map<string, HoldingPerformance> {
  const map = new Map<string, HoldingPerformance>();
  if (holdings) {
    for (const holding of holdings) {
      map.set(holding.code, holding);
    }
  }
  return map;
}

function EmptySelectionState(): ReactNode {
  return (
    <Surface className="flex min-h-[24rem] items-center justify-center px-6 py-16">
      <div className="flex flex-col items-center justify-center">
        <Briefcase className="mb-4 h-16 w-16 text-muted-foreground" />
        <p className="text-lg text-muted-foreground">Select a portfolio to view details</p>
      </div>
    </Surface>
  );
}

interface PortfolioHeaderProps {
  portfolio: PortfolioWithItems;
  currentValue: number;
  totalPnL: number;
  onPortfolioDeleted?: () => void;
}

function PortfolioHeader({ portfolio, currentValue, totalPnL, onPortfolioDeleted }: PortfolioHeaderProps) {
  const shareCount = portfolio.items.reduce((sum, item) => sum + item.quantity, 0);

  return (
    <Surface className="p-5">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="space-y-3">
          <SectionEyebrow>Selected Portfolio</SectionEyebrow>
          <div className="space-y-1">
            <h2 className="text-2xl font-semibold tracking-tight text-foreground">{portfolio.name}</h2>
            <p className="max-w-2xl text-sm text-muted-foreground">
              {portfolio.description ||
                'Review positions, compare them with the benchmark, and jump into the symbol workbench fast.'}
            </p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <AddStockDialog portfolioId={portfolio.id} />
          <EditPortfolioDialog portfolio={portfolio} />
          <DeletePortfolioDialog portfolio={portfolio} onSuccess={onPortfolioDeleted} />
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <CompactMetric
          label="Current Value"
          value={formatCurrency(currentValue)}
          detail={`${portfolio.items.length} holdings`}
        />
        <CompactMetric
          label="Unrealized P&L"
          value={`${totalPnL >= 0 ? '+' : ''}${formatCurrency(totalPnL)}`}
          detail={totalPnL === 0 ? 'No open profit or loss yet' : 'Latest marked value'}
          tone={totalPnL > 0 ? 'success' : totalPnL < 0 ? 'danger' : 'neutral'}
        />
        <CompactMetric label="Shares" value={shareCount.toLocaleString()} detail="Tracked quantity" />
        <CompactMetric label="Created" value={portfolio.createdAt.slice(0, 10)} detail="Portfolio record" />
      </div>
    </Surface>
  );
}

interface HoldingsTableProps {
  items: PortfolioItem[];
  holdingPerformanceMap: Map<string, HoldingPerformance>;
  onNavigateToChart: (code: string) => void;
}

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

function SortableHeader({ column, label, sortConfig, onSort, align = 'left' }: SortableHeaderProps) {
  const isActive = sortConfig.column === column;
  const Icon = isActive ? (sortConfig.direction === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown;

  return (
    <th
      className={`bg-[var(--app-surface-muted)] px-4 py-3 text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground ${align === 'right' ? 'text-right' : 'text-left'}`}
    >
      <button
        type="button"
        onClick={() => onSort(column)}
        className={`inline-flex items-center gap-1 transition-colors hover:text-primary ${
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
    <Surface className="flex min-h-[26rem] flex-col overflow-hidden">
      <div className="border-b border-border/60 px-5 py-4">
        <SectionHeading
          eyebrow="Results"
          title="Holdings"
          description="Sort positions by cost, value, or current performance without leaving the workspace."
          actions={
            <div className="text-sm text-muted-foreground">
              {items.length} stock{items.length === 1 ? '' : 's'}
            </div>
          }
        />
      </div>

      <div className="min-h-0 flex-1">
        {items.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center px-6 py-10 text-center text-muted-foreground">
            <Briefcase className="mx-auto mb-4 h-12 w-12 opacity-50" />
            <p>No stocks in this portfolio</p>
            <p className="mt-1 text-sm">Click "Add Stock" above to add your first holding.</p>
          </div>
        ) : (
          <div className="h-full overflow-auto">
            <table className="w-full">
              <thead className="sticky top-0 z-10">
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
                  <th className="bg-[var(--app-surface-muted)] px-2 py-3 text-center text-xs font-medium uppercase tracking-[0.14em] text-muted-foreground">
                    Actions
                  </th>
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
      </div>
    </Surface>
  );
}

export function PortfolioDetail({ portfolio, isLoading, error, onPortfolioDeleted }: PortfolioDetailProps) {
  const navigate = useNavigate();
  const { data: performanceData, isLoading: isPerformanceLoading } = usePortfolioPerformance(portfolio?.id ?? null);
  const holdingPerformanceMap = createHoldingPerformanceMap(performanceData?.holdings);

  const handleNavigateToChart = (code: string) => {
    void navigate({ to: '/symbol-workbench', search: { symbol: code } });
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
    <div className="flex min-h-0 flex-col gap-3">
      <PortfolioHeader
        portfolio={portfolio}
        currentValue={currentValue}
        totalPnL={totalPnL}
        onPortfolioDeleted={onPortfolioDeleted}
      />

      <HoldingsTable
        items={portfolio.items}
        holdingPerformanceMap={holdingPerformanceMap}
        onNavigateToChart={onNavigateToChart}
      />

      {portfolio.items.length > 0 && performanceData && (
        <Surface className="p-5">
          <SectionHeading
            eyebrow="Summary"
            title="Performance Snapshot"
            description="Keep benchmark context close without pushing the holdings table out of view."
          />
          <div className="mt-4">
            <PerformanceSummary
              summary={performanceData.summary}
              benchmark={performanceData.benchmark}
              isLoading={isPerformanceLoading}
            />
          </div>
        </Surface>
      )}

      {performanceData?.benchmarkTimeSeries && performanceData.benchmarkTimeSeries.length > 0 && (
        <Surface className="p-5">
          <SectionHeading
            eyebrow="Benchmark"
            title={`Performance vs ${performanceData.benchmark?.name ?? 'TOPIX'}`}
            description="Cumulative return path for the portfolio and its comparison benchmark."
          />
          <div className="mt-4">
            <BenchmarkChart
              data={performanceData.benchmarkTimeSeries}
              benchmarkName={performanceData.benchmark?.name ?? 'TOPIX'}
            />
          </div>
        </Surface>
      )}

      {portfolio.items.length >= 2 && (
        <Surface className="p-5">
          <SectionHeading
            eyebrow="Analysis"
            title="Factor Regression Analysis"
            description="Check the strongest index fit, concentration, and excluded names."
          />
          <div className="mt-4">
            <PortfolioFactorRegressionPanel portfolioId={portfolio.id} />
          </div>
        </Surface>
      )}
    </div>
  );
}
