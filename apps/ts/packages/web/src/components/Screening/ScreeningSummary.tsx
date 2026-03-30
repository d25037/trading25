import { AlertTriangle, BarChart3, TrendingUp, Trophy } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
import { formatMarketsLabel } from '@/lib/marketUtils';
import type { ScreeningSummary as Summary } from '@/types/screening';

interface ScreeningSummaryProps {
  summary: Summary | undefined;
  markets: string[];
  scopeLabel?: string;
  recentDays: number;
  referenceDate?: string;
}

export function ScreeningSummary({ summary, markets, scopeLabel, recentDays, referenceDate }: ScreeningSummaryProps) {
  if (!summary) return null;

  const resolvedScopeLabel = scopeLabel ?? formatMarketsLabel(markets);
  const marketInfo = referenceDate
    ? `${resolvedScopeLabel} / ${recentDays}d / ${referenceDate}`
    : `${resolvedScopeLabel} / ${recentDays}d`;

  const hitRate = summary.totalStocksScreened > 0 ? (summary.matchCount / summary.totalStocksScreened) * 100 : 0;

  const topStrategy = Object.entries(summary.byStrategy).sort((a, b) => b[1] - a[1])[0] || null;

  return (
    <SummaryMetrics
      columns={4}
      items={[
        {
          icon: BarChart3,
          label: 'Screened',
          value: summary.totalStocksScreened.toLocaleString(),
          meta: marketInfo,
        },
        {
          icon: TrendingUp,
          label: 'Matches',
          value: summary.matchCount.toLocaleString(),
          meta: `${hitRate.toFixed(1)}% hit rate`,
          tone: 'positive',
        },
        {
          icon: Trophy,
          label: 'Strategies',
          value: summary.strategiesEvaluated.length.toLocaleString(),
          meta: topStrategy ? `${topStrategy[0]} (${topStrategy[1]})` : 'No strategy hits',
        },
        {
          icon: AlertTriangle,
          label: 'Missing Metrics',
          value: summary.strategiesWithoutBacktestMetrics.length.toLocaleString(),
          meta: `${summary.warnings.length} warnings`,
          tone: summary.warnings.length > 0 ? 'warning' : 'default',
        },
      ]}
    />
  );
}
