import { ArrowDown, ArrowUp, Loader2, TrendingDown, TrendingUp } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import type { BenchmarkMetrics, PortfolioSummary } from '@/hooks/usePortfolioPerformance';
import { getPositiveNegativeColor } from '@/utils/color-schemes';
import { formatCurrency, formatRate } from '@/utils/formatters';

interface PerformanceSummaryProps {
  summary: PortfolioSummary;
  benchmark: BenchmarkMetrics | null;
  isLoading?: boolean;
}

export function PerformanceSummary({ summary, benchmark, isLoading }: PerformanceSummaryProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-24">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {/* Total Value */}
      <Card className="glass-panel">
        <CardContent className="pt-4">
          <p className="text-sm text-muted-foreground mb-1">Time Value</p>
          <p className="text-2xl font-bold tabular-nums">{formatCurrency(summary.currentValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">Cost: {formatCurrency(summary.totalCost)}</p>
        </CardContent>
      </Card>

      {/* P&L */}
      <Card className="glass-panel">
        <CardContent className="pt-4">
          <p className="text-sm text-muted-foreground mb-1">Unrealized P&L</p>
          <div className="flex items-center gap-2">
            {summary.totalPnL >= 0 ? (
              <TrendingUp className={`h-5 w-5 ${getPositiveNegativeColor(summary.totalPnL)}`} />
            ) : (
              <TrendingDown className={`h-5 w-5 ${getPositiveNegativeColor(summary.totalPnL)}`} />
            )}
            <p className={`text-2xl font-bold tabular-nums ${getPositiveNegativeColor(summary.totalPnL)}`}>
              {summary.totalPnL >= 0 ? '+' : ''}
              {formatCurrency(summary.totalPnL)}
            </p>
          </div>
          <p className={`text-sm mt-1 ${getPositiveNegativeColor(summary.returnRate)}`}>
            {formatRate(summary.returnRate)}
          </p>
        </CardContent>
      </Card>

      {/* Beta (if benchmark available) */}
      {benchmark && (
        <Card className="glass-panel">
          <CardContent className="pt-4">
            <p className="text-sm text-muted-foreground mb-1">Beta (vs {benchmark.name})</p>
            <p className="text-2xl font-bold tabular-nums">{benchmark.beta.toFixed(2)}</p>
            <p className="text-xs text-muted-foreground mt-1">
              R<sup>2</sup>: {(benchmark.rSquared * 100).toFixed(1)}%
            </p>
          </CardContent>
        </Card>
      )}

      {/* Relative Return (if benchmark available) */}
      {benchmark && (
        <Card className="glass-panel">
          <CardContent className="pt-4">
            <p className="text-sm text-muted-foreground mb-1">vs {benchmark.name}</p>
            <div className="flex items-center gap-2">
              {benchmark.relativeReturn >= 0 ? (
                <ArrowUp className={`h-5 w-5 ${getPositiveNegativeColor(benchmark.relativeReturn)}`} />
              ) : (
                <ArrowDown className={`h-5 w-5 ${getPositiveNegativeColor(benchmark.relativeReturn)}`} />
              )}
              <p className={`text-2xl font-bold tabular-nums ${getPositiveNegativeColor(benchmark.relativeReturn)}`}>
                {formatRate(benchmark.relativeReturn)}
              </p>
            </div>
            <p className="text-xs text-muted-foreground mt-1">Alpha: {formatRate(benchmark.alpha)}</p>
          </CardContent>
        </Card>
      )}

      {/* Fallback cards when no benchmark */}
      {!benchmark && (
        <>
          <Card className="glass-panel">
            <CardContent className="pt-4">
              <p className="text-sm text-muted-foreground mb-1">Beta</p>
              <p className="text-lg text-muted-foreground">-</p>
              <p className="text-xs text-muted-foreground mt-1">Insufficient data</p>
            </CardContent>
          </Card>
          <Card className="glass-panel">
            <CardContent className="pt-4">
              <p className="text-sm text-muted-foreground mb-1">vs TOPIX</p>
              <p className="text-lg text-muted-foreground">-</p>
              <p className="text-xs text-muted-foreground mt-1">Insufficient data</p>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
