import { describe, expect, it } from 'vitest';
import type { Topix100RankingMetric } from '@/types/ranking';
import {
  DEFAULT_TOPIX100_RANKING_METRIC,
  getTopix100RankingMetricDescription,
  getTopix100RankingMetricLabel,
  resolveTopix100RankingMetric,
  TOPIX100_RANKING_METRIC_OPTIONS,
} from './topix100RankingMetric';

describe('topix100RankingMetric', () => {
  it('exposes both supported metric options', () => {
    expect(DEFAULT_TOPIX100_RANKING_METRIC).toBe('price_vs_sma20_gap');
    expect(TOPIX100_RANKING_METRIC_OPTIONS).toEqual([
      { value: 'price_vs_sma20_gap', label: 'Price / SMA20 Gap' },
      { value: 'price_sma_20_80', label: 'Price SMA 20/80' },
    ]);
  });

  it('returns metric labels and descriptions for both modes', () => {
    expect(getTopix100RankingMetricLabel('price_vs_sma20_gap')).toBe('Price / SMA20 Gap');
    expect(getTopix100RankingMetricDescription('price_vs_sma20_gap')).toContain('Default short-term');
    expect(getTopix100RankingMetricLabel('price_sma_20_80')).toBe('Price SMA 20/80');
    expect(getTopix100RankingMetricDescription('price_sma_20_80')).toContain('Legacy medium-term');
  });

  it('falls back to the default label for unknown metric values', () => {
    expect(getTopix100RankingMetricLabel('unexpected' as Topix100RankingMetric)).toBe('Price / SMA20 Gap');
  });

  it('resolves undefined metrics to the default value', () => {
    expect(resolveTopix100RankingMetric(undefined)).toBe('price_vs_sma20_gap');
    expect(resolveTopix100RankingMetric('price_sma_20_80')).toBe('price_sma_20_80');
  });
});
