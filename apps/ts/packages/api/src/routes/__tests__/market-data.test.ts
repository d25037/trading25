import { afterAll, beforeEach, describe, expect, it, mock } from 'bun:test';
import { marketDataService } from '../../services/market/market-data-service';

// Mock the service before importing the app
const mockGetStockOHLCV = mock();
const mockGetAllStocks = mock();
const mockGetTopix = mock();

const originalMarketDataService = { ...marketDataService };

// Import app after mocking
import marketDataApp from '../market/data';

describe('Market Data Routes', () => {
  beforeEach(() => {
    mockGetStockOHLCV.mockReset();
    mockGetAllStocks.mockReset();
    mockGetTopix.mockReset();

    marketDataService.getStockOHLCV = mockGetStockOHLCV as typeof marketDataService.getStockOHLCV;
    marketDataService.getAllStocks = mockGetAllStocks as typeof marketDataService.getAllStocks;
    marketDataService.getTopix = mockGetTopix as typeof marketDataService.getTopix;
  });

  afterAll(() => {
    Object.assign(marketDataService, originalMarketDataService);
  });

  // ===== Stock OHLCV =====
  describe('GET /api/market/stocks/{code}/ohlcv', () => {
    it('should return stock OHLCV data', async () => {
      mockGetStockOHLCV.mockReturnValue([
        { date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530, volume: 1000000 },
      ]);

      const res = await marketDataApp.request('/api/market/stocks/7203/ohlcv');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ date: string; volume: number }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data[0]).toHaveProperty('date', '2024-01-15');
      expect(data[0]).toHaveProperty('volume', 1000000);
    });

    it('should return 404 when stock not found', async () => {
      mockGetStockOHLCV.mockReturnValue(null);

      const res = await marketDataApp.request('/api/market/stocks/NOTFOUND/ohlcv');

      expect(res.status).toBe(404);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Not Found');
    });

    it('should accept date range query parameters', async () => {
      mockGetStockOHLCV.mockReturnValue([]);

      const res = await marketDataApp.request(
        '/api/market/stocks/7203/ohlcv?start_date=2024-01-01&end_date=2024-12-31'
      );

      expect(res.status).toBe(200);
      expect(mockGetStockOHLCV).toHaveBeenCalledWith('7203', {
        start_date: '2024-01-01',
        end_date: '2024-12-31',
      });
    });
  });

  // ===== All Stocks (Screening) =====
  describe('GET /api/market/stocks', () => {
    it('should return all stocks data', async () => {
      mockGetAllStocks.mockReturnValue([
        {
          code: '7203',
          company_name: 'トヨタ自動車',
          data: [{ date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530, volume: 1000000 }],
        },
      ]);

      const res = await marketDataApp.request('/api/market/stocks');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ code: string; data: unknown[] }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data[0]).toHaveProperty('code', '7203');
      expect(data[0]).toHaveProperty('data');
    });

    it('should return 404 when market database not found', async () => {
      mockGetAllStocks.mockReturnValue(null);

      const res = await marketDataApp.request('/api/market/stocks');

      expect(res.status).toBe(404);
    });

    it('should accept market and history_days parameters', async () => {
      mockGetAllStocks.mockReturnValue([]);

      const res = await marketDataApp.request('/api/market/stocks?market=standard&history_days=100');

      expect(res.status).toBe(200);
      expect(mockGetAllStocks).toHaveBeenCalledWith({
        market: 'standard',
        history_days: 100,
      });
    });

    it('should use default values for parameters', async () => {
      mockGetAllStocks.mockReturnValue([]);

      const res = await marketDataApp.request('/api/market/stocks');

      expect(res.status).toBe(200);
      expect(mockGetAllStocks).toHaveBeenCalledWith({
        market: 'prime',
        history_days: 300,
      });
    });
  });

  // ===== TOPIX =====
  describe('GET /api/market/topix', () => {
    it('should return TOPIX data', async () => {
      mockGetTopix.mockReturnValue([{ date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530 }]);

      const res = await marketDataApp.request('/api/market/topix');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ close: number }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data[0]).toHaveProperty('close', 2530);
    });

    it('should return 404 when TOPIX data not available', async () => {
      mockGetTopix.mockReturnValue(null);

      const res = await marketDataApp.request('/api/market/topix');

      expect(res.status).toBe(404);
    });

    it('should accept date range query parameters', async () => {
      mockGetTopix.mockReturnValue([]);

      const res = await marketDataApp.request('/api/market/topix?start_date=2024-01-01&end_date=2024-12-31');

      expect(res.status).toBe(200);
      expect(mockGetTopix).toHaveBeenCalledWith({
        start_date: '2024-01-01',
        end_date: '2024-12-31',
      });
    });
  });
});
