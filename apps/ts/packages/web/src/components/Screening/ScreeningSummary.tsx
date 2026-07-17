import type { ScreeningSummary as Summary } from '@trading25/contracts/types/api-response-types';
import { AlertTriangle, BarChart3, TrendingUp, Trophy } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
import { formatMarketsLabel } from '@/lib/marketUtils';
import { formatCount } from '@/utils/formatters';

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
  const byStrategy = summary.byStrategy ?? {};
  const strategiesEvaluated = summary.strategiesEvaluated ?? [];
  const strategiesWithoutBacktestMetrics = summary.strategiesWithoutBacktestMetrics ?? [];
  const warnings = summary.warnings ?? [];

  const topStrategy = Object.entries(byStrategy).sort((a, b) => b[1] - a[1])[0] || null;

  return (
    <SummaryMetrics
      columns={4}
      items={[
        {
          icon: BarChart3,
          label: 'Screened',
          value: formatCount(summary.totalStocksScreened),
          meta: marketInfo,
        },
        {
          icon: TrendingUp,
          label: 'Matches',
          value: formatCount(summary.matchCount),
          meta: `${hitRate.toFixed(1)}% hit rate`,
          tone: 'positive',
        },
        {
          icon: Trophy,
          label: 'Strategies',
          value: formatCount(strategiesEvaluated.length),
          meta: topStrategy ? `${topStrategy[0]} (${topStrategy[1]})` : 'No strategy hits',
        },
        {
          icon: AlertTriangle,
          label: 'Missing Metrics',
          value: formatCount(strategiesWithoutBacktestMetrics.length),
          meta: `${warnings.length} warnings`,
          tone: warnings.length > 0 ? 'warning' : 'default',
        },
      ]}
    />
  );
}
