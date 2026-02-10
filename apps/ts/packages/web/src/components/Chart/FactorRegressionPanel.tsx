import type { ApiIndexMatch } from '@trading25/shared/types/api-types';
import { DataStateWrapper } from '@/components/ui/data-state-wrapper';
import { useFactorRegression } from '@/hooks/useFactorRegression';

interface FactorRegressionPanelProps {
  symbol: string | null;
  enabled?: boolean;
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
function IndexMatchList({ title, matches }: { title: string; matches: ApiIndexMatch[] }) {
  if (matches.length === 0) {
    return (
      <div className="space-y-1">
        <h4 className="text-xs font-medium text-muted-foreground">{title}</h4>
        <p className="text-xs text-muted-foreground">No significant matches</p>
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <h4 className="text-xs font-medium text-muted-foreground">{title}</h4>
      <div className="space-y-1">
        {matches.map((match, index) => (
          <div key={match.indexCode} className="flex items-center justify-between text-xs">
            <span className="truncate flex-1">
              {index + 1}. {match.indexName}
            </span>
            <div className="flex items-center gap-2 ml-2">
              <span className={getRSquaredColor(match.rSquared)}>R²={(match.rSquared * 100).toFixed(1)}%</span>
              <span className="text-muted-foreground">β={match.beta.toFixed(2)}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function FactorRegressionPanel({ symbol, enabled = true }: FactorRegressionPanelProps) {
  const { data, isLoading, error } = useFactorRegression(symbol, { enabled });

  if (!symbol) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-sm text-muted-foreground">銘柄を選択してください</p>
      </div>
    );
  }

  const normalizedError =
    error instanceof Error ? error : error ? new Error('Failed to load factor regression data') : null;

  return (
    <DataStateWrapper
      isLoading={isLoading}
      error={normalizedError}
      isEmpty={!data}
      emptyMessage="No factor regression data available"
      loadingMessage="Analyzing factor regression..."
      height="h-full"
    >
      {data && <FactorRegressionContent data={data} />}
    </DataStateWrapper>
  );
}

interface FactorRegressionContentProps {
  data: NonNullable<ReturnType<typeof useFactorRegression>['data']>;
}

function FactorRegressionContent({ data }: FactorRegressionContentProps) {
  return (
    <div className="h-full flex flex-col">
      {/* Main content grid */}
      <div className="flex-1 grid grid-cols-4 gap-3">
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
          <IndexMatchList title="" matches={data.sector17Matches} />
        </div>

        {/* Stage 2: 33 Sectors */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">33 Sectors</h3>
          <IndexMatchList title="" matches={data.sector33Matches} />
        </div>

        {/* Stage 2: TOPIX Size + Style */}
        <div className="rounded-lg bg-background/30 p-3">
          <h3 className="text-sm font-semibold mb-2">Size + Style</h3>
          <IndexMatchList title="" matches={data.topixStyleMatches} />
        </div>
      </div>

      {/* Metadata footer */}
      <div className="mt-2 pt-2 border-t border-border/50 flex justify-between items-center text-xs text-muted-foreground">
        <span>
          Analysis: {data.analysisDate} | Period: {data.dateRange.from} ~ {data.dateRange.to}
        </span>
        <span>Data Points: {data.dataPoints}</span>
      </div>
    </div>
  );
}
