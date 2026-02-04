import { beforeEach, describe, expect, it, mock } from 'bun:test';

// Mock data
const mockIndicesRows = [
  { code: '0000', name: 'TOPIX', name_english: 'TOPIX', category: 'topix', data_start_date: '2008-05-07' },
  {
    code: '0001',
    name: 'TOPIX Core30',
    name_english: 'TOPIX Core30',
    category: 'topix',
    data_start_date: '2008-05-07',
  },
];

const mockIndexDataRows = [
  { date: '2024-01-15', open: 2500.5, high: 2520.0, low: 2480.0, close: 2510.0 },
  { date: '2024-01-16', open: 2510.0, high: 2530.0, low: 2490.0, close: 2525.0 },
];

const mockAll = mock();
const mockGet = mock();
const mockQuery = mock(() => ({ all: mockAll, get: mockGet }));
const mockClose = mock();

mock.module('bun:sqlite', () => ({
  Database: class MockDatabase {
    query = mockQuery;
    close = mockClose;
  },
}));

mock.module('@trading25/shared/utils/dataset-paths', () => ({
  getMarketDbPath: () => '/tmp/mock-market.db',
  getPortfolioDbPath: () => '/tmp/mock-portfolio.db',
  getDatasetPath: (name: string) => `/tmp/datasets/${name}`,
  getDatasetDir: () => '/tmp/datasets',
}));

import indicesApp from '../chart/indices';

describe('Chart Indices Routes', () => {
  beforeEach(() => {
    mockQuery.mockClear();
    mockAll.mockClear();
    mockGet.mockClear();
    mockClose.mockClear();
    // Reset default implementations
    mockQuery.mockImplementation(() => ({ all: mockAll, get: mockGet }));
  });

  describe('GET /api/chart/indices', () => {
    it('should return list of indices', async () => {
      mockAll.mockReturnValue(mockIndicesRows);

      const res = await indicesApp.request('/api/chart/indices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { indices: Array<{ code: string; name: string }>; lastUpdated: string };
      expect(body.indices).toHaveLength(2);
      expect(body.indices[0]).toHaveProperty('code', '0000');
      expect(body.indices[0]).toHaveProperty('name', 'TOPIX');
      expect(body.indices[0]).toHaveProperty('nameEnglish', 'TOPIX');
      expect(body.indices[0]).toHaveProperty('category', 'topix');
      expect(body.indices[0]).toHaveProperty('dataStartDate', '2008-05-07');
      expect(body.lastUpdated).toBeDefined();
    });

    it('should return empty list when no indices', async () => {
      mockAll.mockReturnValue([]);

      const res = await indicesApp.request('/api/chart/indices');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { indices: unknown[] };
      expect(body.indices).toHaveLength(0);
    });

    it('should return 500 when database error occurs', async () => {
      mockQuery.mockImplementation(() => {
        throw new Error('Database connection failed');
      });

      const res = await indicesApp.request('/api/chart/indices');

      expect(res.status).toBe(500);
      const body = (await res.json()) as { error: string; message: string };
      expect(body.error).toBe('Internal Server Error');
      expect(body.message).toBe('Database connection failed');
    });
  });

  describe('GET /api/chart/indices/{code}', () => {
    it('should return index data for a valid code', async () => {
      const indexInfo = mockIndicesRows[0];
      mockGet.mockReturnValue(indexInfo);
      mockAll.mockReturnValue(mockIndexDataRows);

      const res = await indicesApp.request('/api/chart/indices/0000');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { code: string; name: string; data: Array<{ date: string }> };
      expect(body.code).toBe('0000');
      expect(body.name).toBe('TOPIX');
      expect(body.data).toHaveLength(2);
      // Data should be in chronological order (reversed from DB order)
      expect(body.data[0]).toHaveProperty('date', '2024-01-16');
      expect(body.data[1]).toHaveProperty('date', '2024-01-15');
    });

    it('should filter out rows with null values', async () => {
      const indexInfo = mockIndicesRows[0];
      mockGet.mockReturnValue(indexInfo);
      mockAll.mockReturnValue([
        { date: '2024-01-15', open: 2500.5, high: 2520.0, low: 2480.0, close: 2510.0 },
        { date: '2024-01-16', open: null, high: 2530.0, low: 2490.0, close: 2525.0 },
      ]);

      const res = await indicesApp.request('/api/chart/indices/0000');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { data: Array<{ date: string }> };
      expect(body.data).toHaveLength(1);
      expect(body.data[0]).toHaveProperty('date', '2024-01-15');
    });

    it('should return 404 when index not found', async () => {
      mockGet.mockReturnValue(null);

      const res = await indicesApp.request('/api/chart/indices/9999');

      expect(res.status).toBe(404);
      const body = (await res.json()) as { error: string; message: string };
      expect(body.error).toBe('Not Found');
      expect(body.message).toContain('9999');
    });

    it('should return 500 when database error occurs', async () => {
      mockQuery.mockImplementation(() => {
        throw new Error('Query failed');
      });

      const res = await indicesApp.request('/api/chart/indices/0000');

      expect(res.status).toBe(500);
      const body = (await res.json()) as { error: string };
      expect(body.error).toBe('Internal Server Error');
    });

    it('should return empty data array for index with no data', async () => {
      const indexInfo = mockIndicesRows[0];
      mockGet.mockReturnValue(indexInfo);
      mockAll.mockReturnValue([]);

      const res = await indicesApp.request('/api/chart/indices/0000');

      expect(res.status).toBe(200);
      const body = (await res.json()) as { data: unknown[] };
      expect(body.data).toHaveLength(0);
    });
  });
});
