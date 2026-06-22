import { describe, expect, it } from 'vitest';
import {
  countVisibleFundamentalsHistoryMetrics,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
  FUNDAMENTALS_HISTORY_METRIC_IDS,
  isFundamentalsHistoryMetricId,
  normalizeFundamentalsHistoryMetricOrder,
  normalizeFundamentalsHistoryMetricVisibility,
} from './fundamentalsHistoryMetrics';

describe('fundamentalsHistoryMetrics', () => {
  it('includes compact operating and optional payout metrics in ids/defaults', () => {
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('forecastSales');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('operatingProfit');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('forecastOperatingProfit');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('operatingMargin');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('forecastSales');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('operatingProfit');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('forecastOperatingProfit');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('operatingMargin');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.forecastSales).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.operatingProfit).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.forecastOperatingProfit).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.operatingMargin).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.payoutRatio).toBe(false);
  });

  it('validates metric ids', () => {
    expect(isFundamentalsHistoryMetricId('eps')).toBe(true);
    expect(isFundamentalsHistoryMetricId('netSales')).toBe(true);
    expect(isFundamentalsHistoryMetricId('forecastSales')).toBe(true);
    expect(isFundamentalsHistoryMetricId('forecastPayoutRatio')).toBe(true);
    expect(isFundamentalsHistoryMetricId('invalid')).toBe(false);
  });

  it('normalizes order by filtering invalid/duplicate ids and appending defaults', () => {
    const normalized = normalizeFundamentalsHistoryMetricOrder(['payoutRatio', 'eps', 'eps', 'invalid']);
    expect(normalized[0]).toBe('payoutRatio');
    expect(normalized[1]).toBe('eps');
    expect(normalized).toHaveLength(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.length);
    expect(normalized).toEqual(expect.arrayContaining(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER));
  });

  it('normalizes visibility while preserving booleans only', () => {
    const normalized = normalizeFundamentalsHistoryMetricVisibility({
      payoutRatio: false,
      eps: false,
      invalid: true,
      forecastEps: 'true',
    });

    expect(normalized.payoutRatio).toBe(false);
    expect(normalized.eps).toBe(false);
    expect(normalized.forecastEps).toBe(true);
    expect((normalized as Record<string, boolean>).invalid).toBeUndefined();
  });

  it('counts visible metrics from order + visibility', () => {
    const order = ['eps', 'payoutRatio', 'roe'] as const;
    const visibility = {
      ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
      eps: true,
      payoutRatio: false,
      roe: true,
    };
    expect(countVisibleFundamentalsHistoryMetrics([...order], visibility)).toBe(2);
  });
});
