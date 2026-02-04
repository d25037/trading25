import { beforeEach, describe, expect, it, mock } from 'bun:test';

// Mock functions for MarketDataReader
const mockGetStockData = mock();
const mockGetLatestTradingDate = mock();
const mockGetPreviousTradingDate = mock();
const mockReaderClose = mock();

mock.module('@trading25/shared/market-sync', () => ({
  MarketDataReader: class MockMarketDataReader {
    getStockData = mockGetStockData;
    getLatestTradingDate = mockGetLatestTradingDate;
    getPreviousTradingDate = mockGetPreviousTradingDate;
    close = mockReaderClose;
  },
}));

mock.module('@trading25/shared', () => ({
  getMarketDbPath: () => '/tmp/mock-market.db',
  getPortfolioDbPath: () => '/tmp/mock-portfolio.db',
}));

// Mock WatchlistService
const mockGetWatchlistWithItems = mock();
const mockWatchlistService = {
  getWatchlistWithItems: mockGetWatchlistWithItems,
} as never;

import { createWatchlistPricesRoutes } from '../watchlist/watchlist-prices';

const app = createWatchlistPricesRoutes(() => mockWatchlistService);

describe('Watchlist Prices Routes', () => {
  beforeEach(() => {
    mockGetStockData.mockReset();
    mockGetLatestTradingDate.mockReset();
    mockGetPreviousTradingDate.mockReset();
    mockReaderClose.mockReset();
    mockGetWatchlistWithItems.mockReset();
  });

  describe('GET /api/watchlist/{id}/prices', () => {
    it('should return stock prices for watchlist', async () => {
      const latestDate = new Date('2024-01-16');
      const prevDate = new Date('2024-01-15');

      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Tech Stocks',
        items: [{ code: '7203' }, { code: '9984' }],
      });

      mockGetLatestTradingDate.mockReturnValue(latestDate);
      mockGetPreviousTradingDate.mockReturnValue(prevDate);
      const stockData: Record<string, { latest: unknown[]; prev: unknown[] }> = {
        '72030': {
          latest: [{ close: 2530, volume: 1000000, date: latestDate }],
          prev: [{ close: 2500, volume: 900000, date: prevDate }],
        },
        '99840': {
          latest: [{ close: 3200, volume: 500000, date: latestDate }],
          prev: [{ close: 3100, volume: 400000, date: prevDate }],
        },
      };
      mockGetStockData.mockImplementation((code: string, range: { from: Date }) => {
        const data = stockData[code];
        if (!data) return [];
        return range.from.getTime() === latestDate.getTime() ? data.latest : data.prev;
      });

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: Array<{ code: string; close: number }> };
      expect(body.prices).toHaveLength(2);
      expect(body.prices[0]).toHaveProperty('code', '7203');
      expect(body.prices[0]).toHaveProperty('close', 2530);
    });

    it('should return empty prices when watchlist has no items', async () => {
      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Empty List',
        items: [],
      });

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: unknown[] };
      expect(body.prices).toHaveLength(0);
    });

    it('should return empty prices when no latest trading date', async () => {
      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Test',
        items: [{ code: '7203' }],
      });

      mockGetLatestTradingDate.mockReturnValue(null);

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: unknown[] };
      expect(body.prices).toHaveLength(0);
    });

    it('should handle market database errors gracefully', async () => {
      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Test',
        items: [{ code: '7203' }],
      });

      mockGetLatestTradingDate.mockImplementation(() => {
        throw new Error('DB connection failed');
      });

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: unknown[] };
      expect(body.prices).toHaveLength(0);
    });

    it('should return 404 when watchlist not found', async () => {
      const { WatchlistNotFoundError } = await import('@trading25/shared/watchlist');
      mockGetWatchlistWithItems.mockRejectedValue(new WatchlistNotFoundError(999));

      const res = await app.request('/api/watchlist/999/prices');

      expect(res.status).toBe(404);
    });

    it('should calculate change percent correctly', async () => {
      const latestDate = new Date('2024-01-16');
      const prevDate = new Date('2024-01-15');

      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Test',
        items: [{ code: '7203' }],
      });

      mockGetLatestTradingDate.mockReturnValue(latestDate);
      mockGetPreviousTradingDate.mockReturnValue(prevDate);
      mockGetStockData.mockImplementation((_code: string, range: { from: Date }) => {
        if (range.from.getTime() === latestDate.getTime()) {
          return [{ close: 2550, volume: 1000000, date: latestDate }];
        }
        return [{ close: 2500, volume: 900000, date: prevDate }];
      });

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: Array<{ changePercent: number | null }> };
      expect(body.prices[0]?.changePercent).toBe(2);
    });

    it('should handle null previous close', async () => {
      const latestDate = new Date('2024-01-16');

      mockGetWatchlistWithItems.mockResolvedValue({
        id: 1,
        name: 'Test',
        items: [{ code: '7203' }],
      });

      mockGetLatestTradingDate.mockReturnValue(latestDate);
      mockGetPreviousTradingDate.mockReturnValue(null);
      mockGetStockData.mockImplementation((_code: string, range: { from: Date }) => {
        if (range.from.getTime() === latestDate.getTime()) {
          return [{ close: 2550, volume: 1000000, date: latestDate }];
        }
        return [];
      });

      const res = await app.request('/api/watchlist/1/prices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { prices: Array<{ prevClose: number | null; changePercent: number | null }> };
      expect(body.prices[0]?.prevClose).toBeNull();
      expect(body.prices[0]?.changePercent).toBeNull();
    });
  });
});
