import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockRefreshStocks = mock();

mock.module('../../services/market/market-refresh-service', () => ({
  MarketRefreshService: class {
    refreshStocks = mockRefreshStocks;
    close = () => {};
  },
}));

let marketRefreshApp: typeof import('../db/refresh').default;

describe('DB Refresh Routes', () => {
  beforeEach(async () => {
    mockRefreshStocks.mockReset();
    marketRefreshApp = (await import('../db/refresh')).default;
  });

  describe('POST /api/db/stocks/refresh', () => {
    it('refreshes stocks successfully', async () => {
      mockRefreshStocks.mockResolvedValue({
        refreshed: ['72030'],
        failed: [],
        totalRefreshed: 1,
      });

      const res = await marketRefreshApp.request('/api/db/stocks/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes: ['72030'] }),
      });

      expect(res.status).toBe(200);
    });

    it('returns 422 when database not initialized', async () => {
      mockRefreshStocks.mockRejectedValue(new Error('no such table: stocks'));

      const res = await marketRefreshApp.request('/api/db/stocks/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes: ['72030'] }),
      });

      expect(res.status).toBe(422);
    });

    it('returns 500 on generic error', async () => {
      mockRefreshStocks.mockRejectedValue(new Error('unexpected'));

      const res = await marketRefreshApp.request('/api/db/stocks/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ codes: ['72030'] }),
      });

      expect(res.status).toBe(500);
    });
  });
});
