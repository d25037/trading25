import type { Topix100PriceSmaWindow, Topix100RankingMetric } from '@/types/ranking';

export const DEFAULT_TOPIX100_RANKING_METRIC: Topix100RankingMetric = 'price_vs_sma_gap';
export const DEFAULT_TOPIX100_PRICE_SMA_WINDOW: Topix100PriceSmaWindow = 50;
export const TOPIX100_PRICE_SMA_WINDOW_OPTIONS: { value: Topix100PriceSmaWindow; label: string }[] = [
  { value: 50, label: 'SMA50' },
  { value: 100, label: 'SMA100' },
  { value: 20, label: 'SMA20' },
];

export const TOPIX100_RANKING_METRIC_OPTIONS: { value: Topix100RankingMetric; label: string }[] = [
  { value: DEFAULT_TOPIX100_RANKING_METRIC, label: 'Price / SMA Gap' },
  { value: 'price_sma_20_80', label: 'Price SMA 20/80' },
];

export function resolveTopix100RankingMetric(
  metric: Topix100RankingMetric | undefined
): Topix100RankingMetric {
  return metric ?? DEFAULT_TOPIX100_RANKING_METRIC;
}

export function resolveTopix100PriceSmaWindow(
  smaWindow: Topix100PriceSmaWindow | undefined
): Topix100PriceSmaWindow {
  return smaWindow ?? DEFAULT_TOPIX100_PRICE_SMA_WINDOW;
}

export function getTopix100RankingMetricLabel(
  metric: Topix100RankingMetric,
  smaWindow: Topix100PriceSmaWindow = DEFAULT_TOPIX100_PRICE_SMA_WINDOW
): string {
  if (metric === 'price_sma_20_80') {
    return 'Price SMA 20/80';
  }
  return `Price / SMA${smaWindow} Gap`;
}

export function getTopix100RankingMetricDescription(
  metric: Topix100RankingMetric,
  smaWindow: Topix100PriceSmaWindow = DEFAULT_TOPIX100_PRICE_SMA_WINDOW
): string {
  if (metric === 'price_sma_20_80') {
    return 'Legacy SMA 20/80 comparison view.';
  }

  return `SMA${smaWindow} baseline. Q10 = below SMA; Volume Low first.`;
}
