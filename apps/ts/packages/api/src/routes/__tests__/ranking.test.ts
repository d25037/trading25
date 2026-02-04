import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockGetRankings = mock();

mock.module('../../services/market/market-ranking-service', () => ({
  MarketRankingService: class {
    getRankings = mockGetRankings;
    close = () => {};
  },
}));

import rankingApp from '../analytics/ranking';

describe('Market Ranking Routes', () => {
  beforeEach(() => {
    mockGetRankings.mockReset();
  });

  it('returns rankings and passes parsed query to service', async () => {
    mockGetRankings.mockResolvedValue({
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
    expect(mockGetRankings).toHaveBeenCalledWith({
      date: '2025-01-15',
      limit: 5,
      markets: 'prime,standard',
      lookbackDays: 3,
      periodDays: 10,
    });
  });
});
