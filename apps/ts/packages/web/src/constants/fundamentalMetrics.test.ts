import { describe, expect, it } from 'vitest';
import {
  countVisibleFundamentalMetrics,
  DEFAULT_FUNDAMENTAL_METRIC_ORDER,
  DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
  FUNDAMENTAL_METRIC_IDS,
  isFundamentalMetricId,
  normalizeFundamentalMetricOrder,
  normalizeFundamentalMetricVisibility,
  resolveFundamentalsPanelHeightPx,
} from './fundamentalMetrics';

describe('fundamentalMetrics', () => {
  it('includes payout ratio metric id', () => {
    expect(FUNDAMENTAL_METRIC_IDS).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTAL_METRIC_ORDER).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY.payoutRatio).toBe(true);
  });

  it('validates metric id', () => {
    expect(isFundamentalMetricId('per')).toBe(true);
    expect(isFundamentalMetricId('payoutRatio')).toBe(true);
    expect(isFundamentalMetricId('unknownMetric')).toBe(false);
    expect(isFundamentalMetricId(123)).toBe(false);
  });

  it('normalizes metric order with dedupe and fallback', () => {
    const normalized = normalizeFundamentalMetricOrder(['payoutRatio', 'eps', 'eps', 'invalid']);
    expect(normalized[0]).toBe('payoutRatio');
    expect(normalized[1]).toBe('eps');
    expect(new Set(normalized).size).toBe(DEFAULT_FUNDAMENTAL_METRIC_ORDER.length);
    expect(normalized).toEqual(expect.arrayContaining(DEFAULT_FUNDAMENTAL_METRIC_ORDER));
  });

  it('falls back to default order for non-array input', () => {
    expect(normalizeFundamentalMetricOrder(null)).toEqual(DEFAULT_FUNDAMENTAL_METRIC_ORDER);
    expect(normalizeFundamentalMetricOrder('bad')).toEqual(DEFAULT_FUNDAMENTAL_METRIC_ORDER);
  });

  it('normalizes metric visibility', () => {
    const normalized = normalizeFundamentalMetricVisibility({
      payoutRatio: false,
      per: true,
      eps: 'not-bool',
    });
    expect(normalized.payoutRatio).toBe(false);
    expect(normalized.per).toBe(true);
    expect(normalized.eps).toBe(DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY.eps);
  });

  it('falls back to default visibility for invalid input', () => {
    expect(normalizeFundamentalMetricVisibility(null)).toEqual(DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY);
    expect(normalizeFundamentalMetricVisibility('bad')).toEqual(DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY);
  });

  it('counts visible metrics by order', () => {
    const order = ['eps', 'payoutRatio', 'per'] as const;
    const visibility = {
      ...DEFAULT_FUNDAMENTAL_METRIC_VISIBILITY,
      eps: true,
      payoutRatio: false,
      per: true,
    };
    expect(countVisibleFundamentalMetrics([...order], visibility)).toBe(2);
  });

  it('resolves panel height by visible metric count', () => {
    const oneRowHeight = resolveFundamentalsPanelHeightPx(1);
    const twoRowsHeight = resolveFundamentalsPanelHeightPx(9);
    expect(twoRowsHeight).toBeGreaterThan(oneRowHeight);
    expect(resolveFundamentalsPanelHeightPx(0)).toBe(oneRowHeight);
  });
});
