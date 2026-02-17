import { AlertTriangle, BarChart3, TrendingUp, Trophy } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import type { ScreeningSummary as Summary } from '@/types/screening';

interface ScreeningSummaryProps {
  summary: Summary | undefined;
  markets: string[];
  recentDays: number;
  referenceDate?: string;
}

export function ScreeningSummary({ summary, markets, recentDays, referenceDate }: ScreeningSummaryProps) {
  if (!summary) return null;

  const marketInfo = referenceDate
    ? `${markets.join(', ')} / ${recentDays}d / ${referenceDate}`
    : `${markets.join(', ')} / ${recentDays}d`;

  const hitRate = summary.totalStocksScreened > 0 ? (summary.matchCount / summary.totalStocksScreened) * 100 : 0;

  const topStrategy = Object.entries(summary.byStrategy).sort((a, b) => b[1] - a[1])[0] || null;

  return (
    <>
      <div className="grid grid-cols-4 gap-3 mb-4">
        <Card className="glass-panel">
          <CardContent className="p-3">
            <div className="flex items-center gap-2 text-muted-foreground mb-1">
              <BarChart3 className="h-4 w-4" />
              <span className="text-xs">Screened</span>
            </div>
            <p className="text-xl font-bold tabular-nums">{summary.totalStocksScreened.toLocaleString()}</p>
            <p className="text-xs text-muted-foreground mt-1">{marketInfo}</p>
          </CardContent>
        </Card>

        <Card className="glass-panel">
          <CardContent className="p-3">
            <div className="flex items-center gap-2 text-muted-foreground mb-1">
              <TrendingUp className="h-4 w-4" />
              <span className="text-xs">Matches</span>
            </div>
            <p className="text-xl font-bold tabular-nums text-green-600 dark:text-green-400">{summary.matchCount}</p>
            <p className="text-xs text-muted-foreground mt-1">{hitRate.toFixed(1)}% hit rate</p>
          </CardContent>
        </Card>

        <Card className="glass-panel">
          <CardContent className="p-3">
            <div className="flex items-center gap-2 text-muted-foreground mb-1">
              <Trophy className="h-4 w-4" />
              <span className="text-xs">Strategies</span>
            </div>
            <p className="text-xl font-bold tabular-nums">{summary.strategiesEvaluated.length}</p>
            <p className="text-xs text-muted-foreground mt-1">
              {topStrategy ? `${topStrategy[0]} (${topStrategy[1]})` : 'No strategy hits'}
            </p>
          </CardContent>
        </Card>

        <Card className="glass-panel">
          <CardContent className="p-3">
            <div className="flex items-center gap-2 text-muted-foreground mb-1">
              <AlertTriangle className="h-4 w-4" />
              <span className="text-xs">Missing Metrics</span>
            </div>
            <p className="text-xl font-bold tabular-nums">{summary.strategiesWithoutBacktestMetrics.length}</p>
            <p className="text-xs text-muted-foreground mt-1">{summary.warnings.length} warnings</p>
          </CardContent>
        </Card>
      </div>
    </>
  );
}
