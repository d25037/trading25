import { Calendar, TrendingDown, TrendingUp } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
import type { MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';

export function FundamentalRankingSummary({ data }: { data: MarketFundamentalRankingResponse | undefined }) {
  if (!data) return null;
  const forecastHigh = data.rankings.forecastHigh?.[0];
  const forecastLow = data.rankings.forecastLow?.[0];
  const actualHigh = data.rankings.actualHigh?.[0];
  const actualLow = data.rankings.actualLow?.[0];

  return (
    <SummaryMetrics
      columns={4}
      items={[
        { icon: Calendar, label: 'Date', value: data.date, meta: `${data.markets.join(', ')} / ${data.metricKey}` },
        {
          icon: TrendingUp,
          label: 'Forecast High',
          value: forecastHigh?.forecastEps?.toLocaleString('ja-JP') ?? '-',
          meta: forecastHigh?.code ?? '-',
          tone: 'positive',
        },
        {
          icon: TrendingDown,
          label: 'Forecast Low',
          value: forecastLow?.forecastEps?.toLocaleString('ja-JP') ?? '-',
          meta: forecastLow?.code ?? '-',
          tone: 'negative',
        },
        {
          icon: TrendingUp,
          label: 'Actual High',
          value: actualHigh?.actualEps?.toLocaleString('ja-JP') ?? '-',
          meta: actualHigh?.code ?? '-',
          tone: 'positive',
        },
        {
          icon: TrendingDown,
          label: 'Actual Low',
          value: actualLow?.actualEps?.toLocaleString('ja-JP') ?? '-',
          meta: actualLow?.code ?? '-',
          tone: 'negative',
        },
      ]}
    />
  );
}
