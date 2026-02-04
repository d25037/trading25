import { afterAll, beforeEach, describe, expect, it, mock } from 'bun:test';
import { datasetDataService } from '../../services/dataset/dataset-data-service';

// Mock the service before importing the app
const mockGetStockOHLCV = mock();
const mockGetStockOHLCVBatch = mock();
const mockGetStockList = mock();
const mockGetTopix = mock();
const mockGetIndex = mock();
const mockGetIndexList = mock();
const mockGetMargin = mock();
const mockGetMarginList = mock();
const mockGetStatements = mock();
const mockGetSectorMapping = mock();
const mockGetStockSectorMapping = mock();
const mockGetSectorStocks = mock();
const mockGetSectorsWithCount = mock();
const mockGetMarginBatch = mock();
const mockGetStatementsBatch = mock();

const originalDatasetDataService = { ...datasetDataService };

// Import app after mocking
import datasetDataApp from '../dataset/data';

describe('Dataset Data Routes', () => {
  beforeEach(() => {
    mockGetStockOHLCV.mockReset();
    mockGetStockOHLCVBatch.mockReset();
    mockGetStockList.mockReset();
    mockGetTopix.mockReset();
    mockGetIndex.mockReset();
    mockGetIndexList.mockReset();
    mockGetMargin.mockReset();
    mockGetMarginList.mockReset();
    mockGetStatements.mockReset();
    mockGetSectorMapping.mockReset();
    mockGetStockSectorMapping.mockReset();
    mockGetSectorStocks.mockReset();
    mockGetSectorsWithCount.mockReset();
    mockGetMarginBatch.mockReset();
    mockGetStatementsBatch.mockReset();

    datasetDataService.getStockOHLCV = mockGetStockOHLCV as typeof datasetDataService.getStockOHLCV;
    datasetDataService.getStockOHLCVBatch = mockGetStockOHLCVBatch as typeof datasetDataService.getStockOHLCVBatch;
    datasetDataService.getStockList = mockGetStockList as typeof datasetDataService.getStockList;
    datasetDataService.getTopix = mockGetTopix as typeof datasetDataService.getTopix;
    datasetDataService.getIndex = mockGetIndex as typeof datasetDataService.getIndex;
    datasetDataService.getIndexList = mockGetIndexList as typeof datasetDataService.getIndexList;
    datasetDataService.getMargin = mockGetMargin as typeof datasetDataService.getMargin;
    datasetDataService.getMarginList = mockGetMarginList as typeof datasetDataService.getMarginList;
    datasetDataService.getStatements = mockGetStatements as typeof datasetDataService.getStatements;
    datasetDataService.getSectorMapping = mockGetSectorMapping as typeof datasetDataService.getSectorMapping;
    datasetDataService.getStockSectorMapping =
      mockGetStockSectorMapping as typeof datasetDataService.getStockSectorMapping;
    datasetDataService.getSectorStocks = mockGetSectorStocks as typeof datasetDataService.getSectorStocks;
    datasetDataService.getSectorsWithCount = mockGetSectorsWithCount as typeof datasetDataService.getSectorsWithCount;
    datasetDataService.getMarginBatch = mockGetMarginBatch as typeof datasetDataService.getMarginBatch;
    datasetDataService.getStatementsBatch = mockGetStatementsBatch as typeof datasetDataService.getStatementsBatch;
  });

  afterAll(() => {
    Object.assign(datasetDataService, originalDatasetDataService);
  });

  // ===== Stock OHLCV =====
  describe('GET /api/dataset/{name}/stocks/{code}/ohlcv', () => {
    it('should return stock OHLCV data', async () => {
      mockGetStockOHLCV.mockResolvedValue([
        { date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530, volume: 1000000 },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/7203/ohlcv');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ date: string; close: number }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data[0]).toHaveProperty('date', '2024-01-15');
      expect(data[0]).toHaveProperty('close', 2530);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStockOHLCV.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/stocks/7203/ohlcv');

      expect(res.status).toBe(404);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Not Found');
    });

    it('should accept date range query parameters', async () => {
      mockGetStockOHLCV.mockResolvedValue([]);

      const res = await datasetDataApp.request(
        '/api/dataset/sampleA/stocks/7203/ohlcv?start_date=2024-01-01&end_date=2024-12-31'
      );

      expect(res.status).toBe(200);
      expect(mockGetStockOHLCV).toHaveBeenCalledWith('sampleA', '7203', {
        start_date: '2024-01-01',
        end_date: '2024-12-31',
        timeframe: 'daily',
      });
    });
  });

  // ===== Stock OHLCV Batch =====
  describe('GET /api/dataset/{name}/stocks/ohlcv/batch', () => {
    it('should return batch stock OHLCV data', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({
        '7203': [{ date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530, volume: 1000000 }],
        '9984': [{ date: '2024-01-15', open: 3200, high: 3250, low: 3180, close: 3230, volume: 500000 }],
      });

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/ohlcv/batch?codes=7203,9984');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Record<string, Array<{ date: string; close: number }>>;
      expect(data).toHaveProperty('7203');
      expect(data).toHaveProperty('9984');
      expect(data['7203']?.[0]).toHaveProperty('date', '2024-01-15');
      expect(data['9984']?.[0]).toHaveProperty('close', 3230);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/stocks/ohlcv/batch?codes=7203');

      expect(res.status).toBe(404);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Not Found');
    });

    it('should return empty object when no codes found', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({});

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/ohlcv/batch?codes=NOTFOUND');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(data).toEqual({});
    });

    it('should accept date range and timeframe query parameters', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({});

      const res = await datasetDataApp.request(
        '/api/dataset/sampleA/stocks/ohlcv/batch?codes=7203,9984&start_date=2024-01-01&end_date=2024-12-31&timeframe=weekly'
      );

      expect(res.status).toBe(200);
      expect(mockGetStockOHLCVBatch).toHaveBeenCalledWith('sampleA', {
        codes: '7203,9984',
        start_date: '2024-01-01',
        end_date: '2024-12-31',
        timeframe: 'weekly',
      });
    });

    it('should return partial success when some codes not found', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({
        '7203': [{ date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530, volume: 1000000 }],
      });

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/ohlcv/batch?codes=7203,NOTFOUND');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Record<string, unknown>;
      expect(data).toHaveProperty('7203');
      expect(data).not.toHaveProperty('NOTFOUND');
    });
  });

  // ===== Stock List =====
  describe('GET /api/dataset/{name}/stocks', () => {
    it('should return stock list', async () => {
      mockGetStockList.mockResolvedValue([
        { stockCode: '7203', record_count: 500 },
        { stockCode: '9984', record_count: 450 },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
      expect(data).toHaveLength(2);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStockList.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/stocks');

      expect(res.status).toBe(404);
    });

    it('should accept query parameters', async () => {
      mockGetStockList.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks?limit=10&min_records=50&detail=true');

      expect(res.status).toBe(200);
      expect(mockGetStockList).toHaveBeenCalledWith('sampleA', {
        limit: 10,
        min_records: 50,
        detail: 'true',
      });
    });
  });

  // ===== TOPIX =====
  describe('GET /api/dataset/{name}/topix', () => {
    it('should return TOPIX data', async () => {
      mockGetTopix.mockResolvedValue([{ date: '2024-01-15', open: 2500, high: 2550, low: 2480, close: 2530 }]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/topix');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetTopix.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/topix');

      expect(res.status).toBe(404);
    });
  });

  // ===== Index Data =====
  describe('GET /api/dataset/{name}/indices/{code}', () => {
    it('should return index data', async () => {
      mockGetIndex.mockResolvedValue([{ date: '2024-01-15', open: 1000, high: 1050, low: 980, close: 1030 }]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/indices/I1001');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 400 when index code is invalid format', async () => {
      const res = await datasetDataApp.request('/api/dataset/sampleA/indices/NOTFOUND');

      // NOTFOUND doesn't match the index code pattern (e.g., I1001 or 3650)
      expect(res.status).toBe(400);
    });

    it('should return 404 when valid index not found', async () => {
      mockGetIndex.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/sampleA/indices/I9999');

      expect(res.status).toBe(404);
    });
  });

  // ===== Index List =====
  describe('GET /api/dataset/{name}/indices', () => {
    it('should return index list', async () => {
      mockGetIndexList.mockResolvedValue([
        {
          indexCode: 'I1001',
          indexName: '電気機器',
          record_count: 300,
          start_date: '2020-01-06',
          end_date: '2024-12-27',
        },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/indices');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetIndexList.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/indices');

      expect(res.status).toBe(404);
    });

    it('should accept codes filter', async () => {
      mockGetIndexList.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/indices?codes=I1001,I1002');

      expect(res.status).toBe(200);
      expect(mockGetIndexList).toHaveBeenCalledWith('sampleA', {
        min_records: 100,
        codes: 'I1001,I1002',
      });
    });
  });

  // ===== Margin Data =====
  describe('GET /api/dataset/{name}/margin/{code}', () => {
    it('should return margin data', async () => {
      mockGetMargin.mockResolvedValue([{ date: '2024-01-15', longMarginVolume: 10000, shortMarginVolume: 5000 }]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/margin/7203');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 400 when stock code is invalid format', async () => {
      const res = await datasetDataApp.request('/api/dataset/sampleA/margin/NOTFOUND');

      // NOTFOUND doesn't match the stock code pattern (4-5 digits)
      expect(res.status).toBe(400);
    });

    it('should return 404 when valid stock not found', async () => {
      mockGetMargin.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/sampleA/margin/9999');

      expect(res.status).toBe(404);
    });
  });

  // ===== Margin List =====
  describe('GET /api/dataset/{name}/margin', () => {
    it('should return margin list', async () => {
      mockGetMarginList.mockResolvedValue([
        { stockCode: '7203', record_count: 100, avg_long_margin: 10000, avg_short_margin: 5000 },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/margin');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetMarginList.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/margin');

      expect(res.status).toBe(404);
    });
  });

  // ===== Statements =====
  describe('GET /api/dataset/{name}/statements/{code}', () => {
    it('should return statements data', async () => {
      mockGetStatements.mockResolvedValue([
        {
          disclosedDate: '2024-05-10',
          typeOfCurrentPeriod: 'FY',
          typeOfDocument: 'FYFinancialStatements_Consolidated_JP',
          earningsPerShare: 150.5,
          profit: 1000000000,
          equity: 5000000000,
          nextYearForecastEarningsPerShare: 160.0,
          bps: 2500.0,
          sales: 30000000000,
          operatingProfit: 2000000000,
          ordinaryProfit: 2100000000,
          operatingCashFlow: 1500000000,
          dividendFY: 50.0,
          forecastEps: 155.0,
        },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/statements/7203');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ disclosedDate: string; earningsPerShare: number }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data[0]).toHaveProperty('disclosedDate', '2024-05-10');
      expect(data[0]).toHaveProperty('earningsPerShare', 150.5);
      expect(data[0]).toHaveProperty('bps', 2500.0);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStatements.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/statements/7203');

      expect(res.status).toBe(404);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Not Found');
    });

    it('should return empty array when stock has no statements', async () => {
      mockGetStatements.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/statements/7203');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<unknown>;
      expect(Array.isArray(data)).toBe(true);
      expect(data).toHaveLength(0);
    });
  });

  // ===== Validation Tests =====
  describe('Parameter Validation', () => {
    it('should reject dataset names with path traversal characters', async () => {
      // The .. in the path makes the route not match, so we test a valid path with invalid dataset name
      const res = await datasetDataApp.request('/api/dataset/sample..test/stocks');

      expect(res.status).toBe(400);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Bad Request');
    });

    it('should reject dataset names with slashes', async () => {
      // URL-encoded slash in dataset name
      const res = await datasetDataApp.request('/api/dataset/sample%2Ftest/stocks');

      expect(res.status).toBe(400);
    });

    it('should reject invalid stock codes', async () => {
      mockGetStockOHLCV.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/abc/ohlcv');

      expect(res.status).toBe(400);
      const data = (await res.json()) as { error: string };
      expect(data.error).toBe('Bad Request');
    });

    it('should reject stock codes with SQL injection attempt', async () => {
      mockGetStockOHLCV.mockResolvedValue([]);

      const res = await datasetDataApp.request("/api/dataset/sampleA/stocks/'; DROP TABLE--/ohlcv");

      expect(res.status).toBe(400);
    });

    it('should reject batch requests exceeding max codes', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({});

      // Create 101 codes (exceeds max of 100)
      const codes = Array.from({ length: 101 }, (_, i) => `${7000 + i}`).join(',');
      const res = await datasetDataApp.request(`/api/dataset/sampleA/stocks/ohlcv/batch?codes=${codes}`);

      expect(res.status).toBe(400);
      const data = (await res.json()) as { error: string; details: Array<{ message: string }> };
      expect(data.error).toBe('Bad Request');
      expect(data.details[0]?.message).toContain('Maximum 100');
    });

    it('should accept valid dataset names with underscores and hyphens', async () => {
      mockGetStockList.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sample_A-test/stocks');

      expect(res.status).toBe(200);
    });

    it('should accept valid 4-digit stock codes', async () => {
      mockGetStockOHLCV.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/7203/ohlcv');

      expect(res.status).toBe(200);
    });

    it('should accept valid 5-digit stock codes', async () => {
      mockGetStockOHLCV.mockResolvedValue([]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/stocks/12345/ohlcv');

      expect(res.status).toBe(200);
    });

    it('should accept exactly 100 codes in batch request', async () => {
      mockGetStockOHLCVBatch.mockResolvedValue({});

      const codes = Array.from({ length: 100 }, (_, i) => `${7000 + i}`).join(',');
      const res = await datasetDataApp.request(`/api/dataset/sampleA/stocks/ohlcv/batch?codes=${codes}`);

      expect(res.status).toBe(200);
    });
  });

  // ===== Sector Mapping =====
  describe('GET /api/dataset/{name}/sectors/mapping', () => {
    it('should return sector mapping', async () => {
      mockGetSectorMapping.mockResolvedValue([
        { sector_code: '3650', sector_name: '電気機器', index_code: 'I1001', index_name: '電気機器指数' },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/sectors/mapping');

      expect(res.status).toBe(200);
      const data = await res.json();
      expect(Array.isArray(data)).toBe(true);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetSectorMapping.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/sectors/mapping');

      expect(res.status).toBe(404);
    });
  });

  // ===== Stock-Sector Mapping =====
  describe('GET /api/dataset/{name}/sectors/stock-mapping', () => {
    it('should return stock to sector mapping', async () => {
      mockGetStockSectorMapping.mockResolvedValue([
        { code: '7203', sector33Name: '輸送用機器' },
        { code: '6758', sector33Name: '電気機器' },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/sectors/stock-mapping');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ code: string; sector33Name: string }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data).toHaveLength(2);
      expect(data[0]).toHaveProperty('code', '7203');
      expect(data[0]).toHaveProperty('sector33Name', '輸送用機器');
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStockSectorMapping.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/sectors/stock-mapping');

      expect(res.status).toBe(404);
    });
  });

  // ===== Sector Stocks =====
  describe('GET /api/dataset/{name}/sectors/{sectorName}/stocks', () => {
    it('should return stock codes for a sector', async () => {
      mockGetSectorStocks.mockResolvedValue(['7203', '7267', '7269']);

      const res = await datasetDataApp.request(
        `/api/dataset/sampleA/sectors/${encodeURIComponent('輸送用機器')}/stocks`
      );

      expect(res.status).toBe(200);
      const data = (await res.json()) as string[];
      expect(Array.isArray(data)).toBe(true);
      expect(data).toHaveLength(3);
      expect(data[0]).toBe('7203');
    });

    it('should return empty array when no stocks found', async () => {
      mockGetSectorStocks.mockResolvedValue([]);

      const res = await datasetDataApp.request(
        `/api/dataset/sampleA/sectors/${encodeURIComponent('存在しないセクター')}/stocks`
      );

      expect(res.status).toBe(200);
      const data = (await res.json()) as string[];
      expect(data).toHaveLength(0);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetSectorStocks.mockResolvedValue(null);

      const res = await datasetDataApp.request(
        `/api/dataset/notfound/sectors/${encodeURIComponent('電気機器')}/stocks`
      );

      expect(res.status).toBe(404);
    });
  });

  // ===== Sectors with Count =====
  describe('GET /api/dataset/{name}/sectors', () => {
    it('should return sectors with stock count', async () => {
      mockGetSectorsWithCount.mockResolvedValue([
        {
          sector_code: '3650',
          sector_name: '電気機器',
          index_code: '3650',
          index_name: '電気機器',
          stock_count: 45,
        },
        {
          sector_code: '3700',
          sector_name: '輸送用機器',
          index_code: '3700',
          index_name: '輸送用機器',
          stock_count: 30,
        },
      ]);

      const res = await datasetDataApp.request('/api/dataset/sampleA/sectors');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Array<{ sector_name: string; stock_count: number }>;
      expect(Array.isArray(data)).toBe(true);
      expect(data).toHaveLength(2);
      expect(data[0]).toHaveProperty('stock_count', 45);
    });

    it('should return 404 when dataset not found', async () => {
      mockGetSectorsWithCount.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/sectors');

      expect(res.status).toBe(404);
    });
  });

  // ===== Margin Batch =====
  describe('GET /api/dataset/{name}/margin/batch', () => {
    it('should return batch margin data', async () => {
      mockGetMarginBatch.mockResolvedValue({
        '7203': [{ date: '2024-01-15', longMarginVolume: 10000, shortMarginVolume: 5000 }],
        '9984': [{ date: '2024-01-15', longMarginVolume: 8000, shortMarginVolume: 3000 }],
      });

      const res = await datasetDataApp.request('/api/dataset/sampleA/margin/batch?codes=7203,9984');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Record<string, unknown[]>;
      expect(data).toHaveProperty('7203');
      expect(data).toHaveProperty('9984');
    });

    it('should return 404 when dataset not found', async () => {
      mockGetMarginBatch.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/margin/batch?codes=7203');

      expect(res.status).toBe(404);
    });
  });

  // ===== Statements Batch =====
  describe('GET /api/dataset/{name}/statements/batch', () => {
    it('should return batch statements data', async () => {
      mockGetStatementsBatch.mockResolvedValue({
        '7203': [{ disclosedDate: '2024-05-10', earningsPerShare: 150.5 }],
      });

      const res = await datasetDataApp.request('/api/dataset/sampleA/statements/batch?codes=7203');

      expect(res.status).toBe(200);
      const data = (await res.json()) as Record<string, unknown[]>;
      expect(data).toHaveProperty('7203');
    });

    it('should return 404 when dataset not found', async () => {
      mockGetStatementsBatch.mockResolvedValue(null);

      const res = await datasetDataApp.request('/api/dataset/notfound/statements/batch?codes=7203');

      expect(res.status).toBe(404);
    });
  });
});
