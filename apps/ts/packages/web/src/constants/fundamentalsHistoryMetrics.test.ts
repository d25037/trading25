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
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('operatingProfit');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('operatingMargin');
    expect(FUNDAMENTALS_HISTORY_METRIC_IDS).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('operatingProfit');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('operatingMargin');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER).toContain('payoutRatio');
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.operatingProfit).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.operatingMargin).toBe(true);
    expect(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY.payoutRatio).toBe(false);
  });

  it('validates metric ids', () => {
    expect(isFundamentalsHistoryMetricId('eps')).toBe(true);
    expect(isFundamentalsHistoryMetricId('netSales')).toBe(true);
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

  it('migrates the combined operating metric order to split operating columns', () => {
    const normalized = normalizeFundamentalsHistoryMetricOrder(['eps', 'operatingProfitMargin', 'roe']);
    expect(normalized[0]).toBe('eps');
    expect(normalized[1]).toBe('operatingProfit');
    expect(normalized[2]).toBe('operatingMargin');
    expect(normalized[3]).toBe('roe');
  });

  it('migrates the persisted legacy default order to the compact default order', () => {
    const normalized = normalizeFundamentalsHistoryMetricOrder([
      'eps',
      'forecastEps',
      'bps',
      'dividendPerShare',
      'forecastDividendPerShare',
      'payoutRatio',
      'forecastPayoutRatio',
      'roe',
      'cashFlowOperating',
      'cashFlowInvesting',
      'cashFlowFinancing',
    ]);

    expect(normalized).toEqual(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER);
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

  it('migrates the combined operating metric visibility to split operating columns', () => {
    const normalized = normalizeFundamentalsHistoryMetricVisibility({
      operatingProfitMargin: false,
    });

    expect(normalized.operatingProfit).toBe(false);
    expect(normalized.operatingMargin).toBe(false);
  });

  it('migrates the persisted legacy all-visible defaults to compact visibility', () => {
    const normalized = normalizeFundamentalsHistoryMetricVisibility({
      eps: true,
      forecastEps: true,
      bps: true,
      dividendPerShare: true,
      forecastDividendPerShare: true,
      payoutRatio: true,
      forecastPayoutRatio: true,
      roe: true,
      cashFlowOperating: true,
      cashFlowInvesting: true,
      cashFlowFinancing: true,
    });

    expect(normalized).toEqual(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY);
  });

  it('migrates the intermediate wide default after compact metrics were appended', () => {
    const normalized = normalizeFundamentalsHistoryMetricVisibility({
      eps: true,
      forecastEps: true,
      netSales: true,
      operatingProfitMargin: true,
      roe: true,
      dividendPerShare: true,
      bps: true,
      cashFlowOperating: true,
      forecastDividendPerShare: true,
      payoutRatio: true,
      forecastPayoutRatio: true,
      cashFlowInvesting: true,
      cashFlowFinancing: true,
    });

    expect(normalized).toEqual(DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY);
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
