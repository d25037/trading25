import { afterAll, beforeEach, describe, expect, it, mock } from 'bun:test';

const mockBtGet = mock();

mock.module('../../services/bt-api-proxy', () => ({
  btGet: mockBtGet,
}));

import rankingApp from '../analytics/ranking';

describe('Market Ranking Routes', () => {
  beforeEach(() => {
    mockBtGet.mockReset();
  });

  afterAll(() => {
    mock.restore();
  });

  it('returns rankings and passes parsed query to service', async () => {
    mockBtGet.mockResolvedValue({
      date: '2025-01-15',
      markets: ['prime', 'standard'],
      lookbackDays: 3,
      periodDays: 10,
      rankings: {
        tradingValue: [],
        gainers: [],
        losers: [],
        periodHigh: [],
        periodLow: [],
      },
      lastUpdated: new Date().toISOString(),
    });

    const res = await rankingApp.request(
      '/api/analytics/ranking?date=2025-01-15&limit=5&markets=prime,standard&lookbackDays=3&periodDays=10'
    );

    expect(res.status).toBe(200);
    expect(mockBtGet).toHaveBeenCalledWith('/api/analytics/ranking', {
      date: '2025-01-15',
      limit: 5,
      markets: 'prime,standard',
      lookbackDays: 3,
      periodDays: 10,
    });
  });
});
