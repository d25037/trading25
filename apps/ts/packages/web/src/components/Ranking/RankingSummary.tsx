import { Calendar, DollarSign, TrendingDown, TrendingUp } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import type { MarketRankingResponse } from '@/types/ranking';

interface RankingSummaryProps {
  data: MarketRankingResponse | undefined;
}

export function RankingSummary({ data }: RankingSummaryProps) {
  if (!data) return null;

  const topGainer = data.rankings.gainers[0];
  const topLoser = data.rankings.losers[0];
  const topVolume = data.rankings.tradingValue[0];

  return (
    <div className="grid grid-cols-4 gap-3 mb-4">
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
            <DollarSign className="h-4 w-4" />
            <span className="text-xs">Top Volume</span>
          </div>
          <p className="text-sm font-bold truncate">{topVolume?.companyName || '-'}</p>
          <p className="text-xs text-muted-foreground mt-1">{topVolume?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingUp className="h-4 w-4 text-green-500" />
            <span className="text-xs">Top Gainer</span>
          </div>
          <p className="text-sm font-bold text-green-600 dark:text-green-400">
            +{topGainer?.changePercentage?.toFixed(2) || 0}%
          </p>
          <p className="text-xs text-muted-foreground mt-1">{topGainer?.code || '-'}</p>
        </CardContent>
      </Card>

      <Card className="glass-panel">
        <CardContent className="p-3">
          <div className="flex items-center gap-2 text-muted-foreground mb-1">
            <TrendingDown className="h-4 w-4 text-red-500" />
            <span className="text-xs">Top Loser</span>
          </div>
          <p className="text-sm font-bold text-red-600 dark:text-red-400">
            {topLoser?.changePercentage?.toFixed(2) || 0}%
          </p>
          <p className="text-xs text-muted-foreground mt-1">{topLoser?.code || '-'}</p>
        </CardContent>
      </Card>
    </div>
  );
}
