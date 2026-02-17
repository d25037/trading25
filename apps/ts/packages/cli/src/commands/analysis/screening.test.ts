import { describe, expect, it } from 'bun:test';
import { buildApiParams } from './screening';

describe('analysis screening buildApiParams', () => {
  it('builds strategy-driven query params', () => {
    const params = buildApiParams({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: '15',
      date: '2026-01-15',
      backtestMetric: 'calmar_ratio',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: '30',
    });

    expect(params).toEqual({
      markets: 'prime,standard',
      strategies: 'range_break_v15,forward_eps_driven',
      recentDays: 15,
      date: '2026-01-15',
      backtestMetric: 'calmar_ratio',
      sortBy: 'matchedDate',
      order: 'asc',
      limit: 30,
    });
  });

  it('does not include removed fast/slow options', () => {
    const params = buildApiParams({
      recentDays: '10',
    });

    expect('rangeBreakFast' in params).toBe(false);
    expect('rangeBreakSlow' in params).toBe(false);
    expect('minBreakPercentage' in params).toBe(false);
    expect('minVolumeRatio' in params).toBe(false);
  });
});
