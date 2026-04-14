import { describe, expect, it } from 'vitest';
import type { Topix100RankingMetric } from '@/types/ranking';
import {
  DEFAULT_TOPIX100_PRICE_SMA_WINDOW,
  DEFAULT_TOPIX100_RANKING_METRIC,
  getTopix100PriceBucketLabel,
  getTopix100RankingMetricDescription,
  getTopix100RankingMetricLabel,
  resolveTopix100PriceSmaWindow,
  resolveTopix100RankingMetric,
  TOPIX100_PRICE_SMA_WINDOW_OPTIONS,
  TOPIX100_RANKING_METRIC_OPTIONS,
} from './topix100RankingMetric';

describe('topix100RankingMetric', () => {
  it('exposes supported metric and sma window defaults', () => {
    expect(DEFAULT_TOPIX100_RANKING_METRIC).toBe('price_vs_sma_gap');
    expect(DEFAULT_TOPIX100_PRICE_SMA_WINDOW).toBe(50);
    expect(TOPIX100_RANKING_METRIC_OPTIONS).toEqual([
      { value: 'price_vs_sma_gap', label: 'Price / SMA Gap' },
      { value: 'price_sma_20_80', label: 'Price SMA 20/80' },
    ]);
    expect(TOPIX100_PRICE_SMA_WINDOW_OPTIONS).toEqual([
      { value: 50, label: 'SMA50' },
      { value: 100, label: 'SMA100' },
      { value: 20, label: 'SMA20' },
    ]);
  });

  it('returns metric labels and descriptions for both modes', () => {
    expect(getTopix100RankingMetricLabel('price_vs_sma_gap', 50)).toBe('Price / SMA50 Gap');
    expect(getTopix100RankingMetricDescription('price_vs_sma_gap', 100)).toBe(
      'SMA100 baseline. Q10 = below SMA; Q2-4 = trough; volume SMA 5/20 stays continuous.'
    );
    expect(getTopix100RankingMetricLabel('price_sma_20_80')).toBe('Price SMA 20/80');
    expect(getTopix100RankingMetricDescription('price_sma_20_80')).toBe('Legacy SMA 20/80 comparison view.');
  });

  it('returns bucket labels for ranking filters and table badges', () => {
    expect(getTopix100PriceBucketLabel('all')).toBe('All Buckets');
    expect(getTopix100PriceBucketLabel('q10')).toBe('Q10 Below SMA');
    expect(getTopix100PriceBucketLabel('q234')).toBe('Q2-4 Trough');
    expect(getTopix100PriceBucketLabel('q1')).toBe('Q1 Above SMA');
    expect(getTopix100PriceBucketLabel('other')).toBe('Other');
  });

  it('falls back to the default label for unknown metric values', () => {
    expect(getTopix100RankingMetricLabel('unexpected' as Topix100RankingMetric)).toBe('Price / SMA50 Gap');
  });

  it('resolves undefined values to defaults', () => {
    expect(resolveTopix100RankingMetric(undefined)).toBe('price_vs_sma_gap');
    expect(resolveTopix100PriceSmaWindow(undefined)).toBe(50);
    expect(resolveTopix100RankingMetric('price_sma_20_80')).toBe('price_sma_20_80');
  });
});
