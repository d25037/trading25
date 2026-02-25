import { Calendar, TrendingDown, TrendingUp } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import type { MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';

interface FundamentalRankingSummaryProps {
  data: MarketFundamentalRankingResponse | undefined;
}

function formatRatio(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  return `${value.toLocaleString('ja-JP', { maximumFractionDigits: 4 })}x`;
}

export function FundamentalRankingSummary({ data }: FundamentalRankingSummaryProps) {
  if (!data) return null;

  const ratioHigh = data.rankings.ratioHigh[0];
  const ratioLow = data.rankings.ratioLow[0];

  return (
    <div className="grid grid-cols-1 gap-3 mb-4 md:grid-cols-3">
      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <Calendar className="h-4 w-4" />
            <span className="text-xs">Date</span>
          </div>
          <p className="text-sm font-bold">{data.date}</p>
          <p className="text-xs text-muted-foreground mt-1">{data.markets.join(', ')}</p>
          <p className="text-xs text-muted-foreground mt-1">Metric: {data.metricKey}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingUp className="h-4 w-4 text-green-500" />
            <span className="text-xs">High Ratio</span>
          </div>
          <p className="text-sm font-bold">{formatRatio(ratioHigh?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{ratioHigh?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingDown className="h-4 w-4 text-red-500" />
            <span className="text-xs">Low Ratio</span>
          </div>
          <p className="text-sm font-bold">{formatRatio(ratioLow?.epsValue)}</p>
          <p className="text-xs text-muted-foreground mt-1">{ratioLow?.code || '-'}</p>
        </CardContent>
      </Card>
    </div>
  );
}
