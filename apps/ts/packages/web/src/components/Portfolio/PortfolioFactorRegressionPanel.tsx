import type { ApiExcludedStock, ApiIndexMatch, ApiPortfolioWeight } from '@trading25/shared/types/api-types';
import { AlertTriangle } from 'lucide-react';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { usePortfolioFactorRegression } from '@/hooks/usePortfolioFactorRegression';

interface PortfolioFactorRegressionPanelProps {
  portfolioId: number | null;
}

/**
 * Get color class for R-squared value
 */
function getRSquaredColor(rSquared: number): string {
  const pct = rSquared * 100;
  if (pct >= 30) return 'text-green-500';
  if (pct >= 10) return 'text-yellow-500';
  return 'text-muted-foreground';
}

/**
 * Get color class for beta interpretation
 */
function getBetaColor(beta: number): string {
  if (beta > 1.2) return 'text-red-500';
  if (beta > 0.8) return 'text-yellow-500';
  return 'text-green-500';
}

/**
 * Get beta interpretation label
 */
function getBetaInterpretation(beta: number): string {
  if (beta > 1.2) return 'High sensitivity';
  if (beta > 0.8) return 'Moderate sensitivity';
  return 'Low sensitivity';
}

/**
 * Index match list component
 */
function IndexMatchList({ matches }: { matches: ApiIndexMatch[] }) {
  if (matches.length === 0) {
    return <p className="text-xs text-muted-foreground">No significant matches</p>;
  }

  return (
    <div className="space-y-1">
      {matches.map((match, index) => (
        <div key={match.indexCode} className="flex items-center justify-between text-xs">
          <span className="relative group flex-1 min-w-0">
            <span className="block truncate">
              {index + 1}. {match.indexName}
            </span>
            <span className="absolute left-0 top-0 hidden group-hover:block bg-background border border-border px-2 py-1 rounded shadow-lg z-20 whitespace-nowrap">
              {index + 1}. {match.indexName}
            </span>
          </span>
          <div className="flex items-center gap-2 ml-2 shrink-0">
            <span className={getRSquaredColor(match.rSquared)}>R²={(match.rSquared * 100).toFixed(1)}%</span>
            <span className="text-muted-foreground">β={match.beta.toFixed(2)}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

/**
 * Portfolio weight summary component
 */
function WeightSummary({ weights, totalValue }: { weights: ApiPortfolioWeight[]; totalValue: number }) {
  // Sort by weight descending and take top 5
  const topWeights = [...weights].sort((a, b) => b.weight - a.weight).slice(0, 5);
  const hasMore = weights.length > 5;

  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-muted-foreground mb-1">
        <span>Stock</span>
        <span>Weight</span>
      </div>
      {topWeights.map((w) => (
        <div key={w.code} className="flex items-center justify-between text-xs">
          <span className="truncate flex-1" title={`${w.code} ${w.companyName}`}>
            {w.code} {w.companyName.slice(0, 8)}
          </span>
          <span className="text-primary font-medium ml-2 shrink-0">{(w.weight * 100).toFixed(1)}%</span>
        </div>
      ))}
      {hasMore && <div className="text-xs text-muted-foreground pt-1">+{weights.length - 5} more...</div>}
      <div className="border-t border-border/50 pt-1 mt-1">
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">Total Value</span>
          <span className="font-medium">{totalValue.toLocaleString()} 円</span>
        </div>
      </div>
    </div>
  );
}

/**
 * Excluded stocks warning component
 */
function ExcludedStocksWarning({ excludedStocks }: { excludedStocks: ApiExcludedStock[] }) {
  if (excludedStocks.length === 0) return null;

  return (
    <div className="flex items-start gap-2 text-xs text-yellow-500 bg-yellow-500/10 rounded p-2">
      <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
      <div>
        <p className="font-medium">{excludedStocks.length} stock(s) excluded from analysis</p>
        <ul className="mt-1 space-y-0.5 text-yellow-500/80">
          {excludedStocks.slice(0, 3).map((s) => (
            <li key={s.code}>
              {s.code} {s.companyName}: {s.reason}
            </li>
          ))}
          {excludedStocks.length > 3 && <li>+{excludedStocks.length - 3} more...</li>}
        </ul>
      </div>
    </div>
  );
}

export function PortfolioFactorRegressionPanel({ portfolioId }: PortfolioFactorRegressionPanelProps) {
  const { data, isLoading, error } = usePortfolioFactorRegression(portfolioId);

  if (portfolioId === null) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">ポートフォリオを選択してください</p>
      </div>
    );
  }

  const normalizedError =
    error instanceof Error ? error : error ? new Error('Failed to load portfolio factor regression data') : null;

  return (
    <DataStateWrapper
      isLoading={isLoading}
      error={normalizedError}
      isEmpty={!data}
      emptyMessage="No factor regression data available"
      loadingMessage="Analyzing portfolio factors..."
      height="h-full"
    >
      {data && <PortfolioFactorRegressionContent data={data} />}
    </DataStateWrapper>
  );
}

interface PortfolioFactorRegressionContentProps {
  data: NonNullable<ReturnType<typeof usePortfolioFactorRegression>['data']>;
}

function PortfolioFactorRegressionContent({ data }: PortfolioFactorRegressionContentProps) {
  return (
    <div className="h-full flex flex-col space-y-3">
      {/* Excluded stocks warning */}
      <ExcludedStocksWarning excludedStocks={data.excludedStocks} />

      {/* Main content grid */}
      <div className="flex-1 grid grid-cols-5 gap-3">
        {/* Portfolio Weights */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">
            Portfolio ({data.includedStockCount}/{data.stockCount})
          </h3>
          <WeightSummary weights={data.weights} totalValue={data.totalValue} />
        </div>

        {/* Stage 1: Market Regression */}
        <div className="rounded-lg bg-background/30 p-3 space-y-2">
          <h3 className="text-sm font-semibold">Stage 1: Market (TOPIX)</h3>
          <div className="space-y-2">
            <div className="flex justify-between items-center">
              <span className="text-xs text-muted-foreground">Market Beta (βm)</span>
              <span className="text-sm font-mono">{data.marketBeta.toFixed(3)}</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-xs text-muted-foreground">Market R²</span>
              <span className={`text-sm font-mono ${getRSquaredColor(data.marketRSquared)}`}>
                {(data.marketRSquared * 100).toFixed(1)}%
              </span>
            </div>
            <div className="flex justify-between items-center pt-1 border-t border-border/50">
              <span className="text-xs text-muted-foreground">Interpretation</span>
              <span className={`text-xs ${getBetaColor(data.marketBeta)}`}>
                {getBetaInterpretation(data.marketBeta)}
              </span>
            </div>
          </div>
        </div>

        {/* Stage 2: TOPIX-17 Sectors */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">TOPIX-17 Sectors</h3>
          <IndexMatchList matches={data.sector17Matches} />
        </div>

        {/* Stage 2: 33 Sectors */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">33 Sectors</h3>
          <IndexMatchList matches={data.sector33Matches} />
        </div>

        {/* Stage 2: TOPIX Size + Style */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">Size + Style</h3>
          <IndexMatchList matches={data.topixStyleMatches} />
        </div>
      </div>

      {/* Metadata footer */}
      <div className="pt-2 border-t border-border/50 flex justify-between items-center text-xs text-muted-foreground">
        <span>
          Analysis: {data.analysisDate} | Period: {data.dateRange.from} ~ {data.dateRange.to}
        </span>
        <span>Data Points: {data.dataPoints}</span>
      </div>
    </div>
  );
}
