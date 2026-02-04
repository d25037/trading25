import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockRunScreening = mock();

mock.module('../../services/market/market-screening-service', () => ({
  MarketScreeningService: class {
    runScreening = mockRunScreening;
    close = () => {};
  },
}));

import screeningApp from '../analytics/screening';

describe('Market Screening Routes', () => {
  beforeEach(() => {
    mockRunScreening.mockReset();
  });

  it('returns 500 when service throws', async () => {
    mockRunScreening.mockRejectedValue(new Error('Database not available'));

    const res = await screeningApp.request('/api/analytics/screening?markets=prime');

    expect(res.status).toBeGreaterThanOrEqual(400);
  });

  it('returns screening results and passes parsed query to service', async () => {
    mockRunScreening.mockResolvedValue({
      summary: { total: 0 },
      results: [],
      lastUpdated: new Date().toISOString(),
    });

    const res = await screeningApp.request(
      '/api/analytics/screening?markets=prime&rangeBreakFast=true&rangeBreakSlow=false&recentDays=20&date=2025-01-15&minBreakPercentage=3&minVolumeRatio=1.2&sortBy=breakPercentage&order=desc&limit=25'
    );

    expect(res.status).toBe(200);
    expect(mockRunScreening).toHaveBeenCalledWith({
      markets: 'prime',
      rangeBreakFast: true,
      rangeBreakSlow: false,
      recentDays: 20,
      referenceDate: '2025-01-15',
      minBreakPercentage: 3,
      minVolumeRatio: 1.2,
      sortBy: 'breakPercentage',
      order: 'desc',
      limit: 25,
    });
  });
});
