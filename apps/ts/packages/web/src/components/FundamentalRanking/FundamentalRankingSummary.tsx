import { Calendar, TrendingDown, TrendingUp } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import type { MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';

interface FundamentalRankingSummaryProps {
  data: MarketFundamentalRankingResponse | undefined;
}

function formatEps(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: 2 })}円`;
}

export function FundamentalRankingSummary({ data }: FundamentalRankingSummaryProps) {
  if (!data) return null;

  const forecastHigh = data.rankings.forecastHigh[0];
  const forecastLow = data.rankings.forecastLow[0];
  const actualHigh = data.rankings.actualHigh[0];
  const actualLow = data.rankings.actualLow[0];

  return (
    <div className="grid grid-cols-5 gap-3 mb-4">
      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <Calendar className="h-4 w-4" />
            <span className="text-xs">Date</span>
          </div>
          <p className="text-sm font-bold">{data.date}</p>
          <p className="text-xs text-muted-foreground mt-1">{data.markets.join(', ')}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingUp className="h-4 w-4 text-green-500" />
            <span className="text-xs">Top Forecast</span>
          </div>
          <p className="text-sm font-bold">{formatEps(forecastHigh?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{forecastHigh?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingDown className="h-4 w-4 text-red-500" />
            <span className="text-xs">Low Forecast</span>
          </div>
          <p className="text-sm font-bold">{formatEps(forecastLow?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{forecastLow?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingUp className="h-4 w-4 text-green-500" />
            <span className="text-xs">Top Actual</span>
          </div>
          <p className="text-sm font-bold">{formatEps(actualHigh?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{actualHigh?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingDown className="h-4 w-4 text-red-500" />
            <span className="text-xs">Low Actual</span>
          </div>
          <p className="text-sm font-bold">{formatEps(actualLow?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{actualLow?.code || '-'}</p>
        </CardContent>
      </Card>
    </div>
  );
}
