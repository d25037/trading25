import { BarChart3, TrendingUp, Zap } from 'lucide-react';
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

  return (
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
          <p className="text-xs text-muted-foreground mt-1">
            {((summary.matchCount / summary.totalStocksScreened) * 100).toFixed(1)}% hit rate
          </p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <Zap className="h-4 w-4" />
            <span className="text-xs">Fast</span>
          </div>
          <p className="text-xl font-bold tabular-nums">{summary.byScreeningType.rangeBreakFast}</p>
          <p className="text-xs text-muted-foreground mt-1">EMA 30/120</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingUp className="h-4 w-4" />
            <span className="text-xs">Slow</span>
          </div>
          <p className="text-xl font-bold tabular-nums">{summary.byScreeningType.rangeBreakSlow}</p>
          <p className="text-xs text-muted-foreground mt-1">SMA 50/150</p>
        </CardContent>
      </Card>
    </div>
  );
}
