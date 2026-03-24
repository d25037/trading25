import { Loader2 } from 'lucide-react';
import { CompactMetric } from '@/components/Layout/Workspace';
import type { BenchmarkMetrics, PortfolioSummary } from '@/hooks/usePortfolioPerformance';
import { formatCurrency, formatRate } from '@/utils/formatters';

interface PerformanceSummaryProps {
  summary: PortfolioSummary;
  benchmark: BenchmarkMetrics | null;
  isLoading?: boolean;
}

export function PerformanceSummary({ summary, benchmark, isLoading }: PerformanceSummaryProps) {
  if (isLoading) {
    return (
      <div className="flex h-24 items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const pnlTone = summary.totalPnL > 0 ? 'success' : summary.totalPnL < 0 ? 'danger' : 'neutral';
  const relativeTone = benchmark
    ? benchmark.relativeReturn > 0
      ? 'success'
      : benchmark.relativeReturn < 0
        ? 'danger'
        : 'neutral'
    : 'neutral';

  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <CompactMetric
        label="Market Value"
        value={formatCurrency(summary.currentValue)}
        detail={`Cost ${formatCurrency(summary.totalCost)}`}
      />

      <CompactMetric
        label="Unrealized P&L"
        value={`${summary.totalPnL >= 0 ? '+' : ''}${formatCurrency(summary.totalPnL)}`}
        detail={formatRate(summary.returnRate)}
        tone={pnlTone}
      />

      {benchmark ? (
        <CompactMetric
          label={`Beta vs ${benchmark.name}`}
          value={benchmark.beta.toFixed(2)}
          detail={`R² ${(benchmark.rSquared * 100).toFixed(1)}%`}
        />
      ) : (
        <CompactMetric label="Beta" value="-" detail="Insufficient data" />
      )}

      {benchmark ? (
        <CompactMetric
          label={`vs ${benchmark.name}`}
          value={formatRate(benchmark.relativeReturn)}
          detail={`Alpha ${formatRate(benchmark.alpha)}`}
          tone={relativeTone}
        />
      ) : (
        <CompactMetric label="vs TOPIX" value="-" detail="Insufficient data" />
      )}
    </div>
  );
}
