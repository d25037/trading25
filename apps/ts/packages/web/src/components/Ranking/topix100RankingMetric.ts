import type { Topix100RankingMetric } from '@/types/ranking';

export const DEFAULT_TOPIX100_RANKING_METRIC: Topix100RankingMetric = 'price_vs_sma20_gap';

export const TOPIX100_RANKING_METRIC_OPTIONS: { value: Topix100RankingMetric; label: string }[] = [
  { value: DEFAULT_TOPIX100_RANKING_METRIC, label: 'Price / SMA20 Gap' },
  { value: 'price_sma_20_80', label: 'Price SMA 20/80' },
];

export function resolveTopix100RankingMetric(
  metric: Topix100RankingMetric | undefined
): Topix100RankingMetric {
  return metric ?? DEFAULT_TOPIX100_RANKING_METRIC;
}

export function getTopix100RankingMetricLabel(metric: Topix100RankingMetric): string {
  return TOPIX100_RANKING_METRIC_OPTIONS.find((option) => option.value === metric)?.label ?? 'Price / SMA20 Gap';
}

export function getTopix100RankingMetricDescription(metric: Topix100RankingMetric): string {
  if (metric === 'price_sma_20_80') {
    return 'Legacy medium-term price SMA 20/80 ranking with volume SMA 20/80 sidecar buckets.';
  }

  return 'Default short-term price / SMA20 gap ranking with volume SMA 20/80 sidecar buckets.';
}
