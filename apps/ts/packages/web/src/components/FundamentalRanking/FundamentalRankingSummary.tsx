import { Calendar, TrendingDown, TrendingUp } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
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
    <SummaryMetrics
      columns={3}
      items={[
        {
          icon: Calendar,
          label: 'Date',
          value: data.date,
          meta: `${data.markets.join(', ')} / ${data.metricKey}`,
        },
        {
          icon: TrendingUp,
          label: 'High Ratio',
          value: formatRatio(ratioHigh?.epsValue),
          meta: ratioHigh?.code || '-',
          tone: 'positive',
        },
        {
          icon: TrendingDown,
          label: 'Low Ratio',
          value: formatRatio(ratioLow?.epsValue),
          meta: ratioLow?.code || '-',
          tone: 'negative',
        },
      ]}
    />
  );
}
