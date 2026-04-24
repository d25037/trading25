import { Calendar, Scale, TrendingUp } from 'lucide-react';
import { SummaryMetrics } from '@/components/shared/SummaryMetrics';
import type { ValueCompositeRankingResponse } from '@/types/valueCompositeRanking';

interface ValueCompositeRankingSummaryProps {
  data: ValueCompositeRankingResponse | undefined;
}

function formatScore(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) return '-';
  return value.toLocaleString('ja-JP', { maximumFractionDigits: 3 });
}

function formatWeights(weights: Record<string, number>): string {
  const size = weights.smallMarketCap ?? 0;
  const pbr = weights.lowPbr ?? 0;
  const forwardPer = weights.lowForwardPer ?? 0;
  return `${Math.round(size * 100)}/${Math.round(pbr * 100)}/${Math.round(forwardPer * 100)}`;
}

export function ValueCompositeRankingSummary({ data }: ValueCompositeRankingSummaryProps) {
  if (!data) return null;

  const topItem = data.items[0];

  return (
    <SummaryMetrics
      columns={3}
      items={[
        {
          icon: Calendar,
          label: 'Date',
          value: data.date,
          meta: data.markets.join(', '),
        },
        {
          icon: TrendingUp,
          label: 'Top Score',
          value: formatScore(topItem?.score),
          meta: topItem?.code || '-',
          tone: 'positive',
        },
        {
          icon: Scale,
          label: 'Weights',
          value: formatWeights(data.weights),
          meta: 'size / PBR / fwd PER',
        },
      ]}
    />
  );
}
