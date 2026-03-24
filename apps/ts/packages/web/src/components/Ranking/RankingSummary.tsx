import { Calendar, DollarSign, TrendingDown, TrendingUp } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
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
    <SummaryMetrics
      columns={4}
      items={[
        {
          icon: Calendar,
          label: 'Date',
          value: data.date,
          meta: data.markets.join(', '),
        },
        {
          icon: DollarSign,
          label: 'Top Volume',
          value: topVolume?.companyName || '-',
          meta: topVolume?.code || '-',
        },
        {
          icon: TrendingUp,
          label: 'Top Gainer',
          value: `+${topGainer?.changePercentage?.toFixed(2) || 0}%`,
          meta: topGainer?.code || '-',
          tone: 'positive',
        },
        {
          icon: TrendingDown,
          label: 'Top Loser',
          value: `${topLoser?.changePercentage?.toFixed(2) || 0}%`,
          meta: topLoser?.code || '-',
          tone: 'negative',
        },
      ]}
    />
  );
}
