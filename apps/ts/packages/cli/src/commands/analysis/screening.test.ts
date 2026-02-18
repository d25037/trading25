import { describe, expect, it } from 'bun:test';
import { buildApiParams } from './screening';

describe('analysis screening buildApiParams', () => {
  it('builds strategy-driven query params without backtest metric', () => {
    const params = buildApiParams({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: '15',
      date: '2026-01-15',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: '30',
    });

    expect(params).toEqual({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: 15,
      date: '2026-01-15',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: 30,
    });
    expect('backtestMetric' in params).toBe(false);
  });

  it('defaults sortBy to matchedDate and order to desc', () => {
    const params = buildApiParams({
      recentDays: '10',
    });

    expect(params.sortBy).toBe('matchedDate');
    expect(params.order).toBe('desc');
    expect('rangeBreakFast' in params).toBe(false);
    expect('rangeBreakSlow' in params).toBe(false);
    expect('minBreakPercentage' in params).toBe(false);
    expect('minVolumeRatio' in params).toBe(false);
  });
});
