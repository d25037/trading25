import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockGetStocks = mock();

mock.module('../../services/market/sector-stocks-service', () => ({
  SectorStocksService: class {
    getStocks = mockGetStocks;
    close = () => {};
  },
}));

import sectorStocksApp from '../analytics/sector-stocks';

describe('Sector Stocks Routes', () => {
  beforeEach(() => {
    mockGetStocks.mockReset();
  });

  it('returns sector stocks data', async () => {
    mockGetStocks.mockResolvedValue({
      stocks: [],
      totalCount: 0,
    });

    const res = await sectorStocksApp.request('/api/analytics/sector-stocks?sector33Name=%E9%8A%80%E8%A1%8C%E6%A5%AD');
    expect(res.status).toBe(200);
    expect(mockGetStocks).toHaveBeenCalled();
  });

  it('returns 422 when database is not ready', async () => {
    mockGetStocks.mockRejectedValue(new Error('Database not initialized'));

    const res = await sectorStocksApp.request('/api/analytics/sector-stocks?sector33Name=%E9%8A%80%E8%A1%8C%E6%A5%AD');
    expect(res.status).toBe(422);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Unprocessable Entity');
  });

  it('returns 500 when service throws generic error', async () => {
    mockGetStocks.mockRejectedValue(new Error('Something went wrong'));

    const res = await sectorStocksApp.request('/api/analytics/sector-stocks?sector33Name=%E9%8A%80%E8%A1%8C%E6%A5%AD');
    expect(res.status).toBe(500);
  });
});
