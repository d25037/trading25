/**
 * Stock History Refetcher Tests
 * Test stock history refetch functionality with TOPIX range filtering
 */

import { Database } from 'bun:sqlite';
import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import type { BatchExecutor } from '../clients/base/BatchExecutor';
import type { ApiClient } from '../dataset/api-client';
import type { StockData } from '../dataset/types';
import type { DrizzleMarketDatabase as MarketDatabase } from '../db/drizzle-market-database';
import { toISODateString } from '../utils/date-helpers';
import { StockHistoryRefetcher } from './stock-history-refetcher';

/**
 * Create mock database
 */
function createMockDatabase(): MarketDatabase {
  const db = new Database(':memory:');

  // Initialize schema
  db.exec(`
    CREATE TABLE IF NOT EXISTS topix_data (
      date DATE PRIMARY KEY,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS stocks (
      code TEXT PRIMARY KEY,
      company_name TEXT NOT NULL,
      company_name_english TEXT,
      market_code TEXT NOT NULL,
      market_name TEXT NOT NULL,
      sector17_code TEXT NOT NULL,
      sector17_name TEXT NOT NULL,
      sector33_code TEXT NOT NULL,
      sector33_name TEXT NOT NULL,
      scale_category TEXT,
      listed_date DATE NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS stock_data (
      code TEXT NOT NULL,
      date DATE NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume INTEGER NOT NULL,
      adjustment_factor REAL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (code, date),
      FOREIGN KEY (code) REFERENCES stocks(code)
    );

    CREATE TABLE IF NOT EXISTS sync_metadata (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Insert TOPIX date range (100 days)
  const startDate = new Date('2024-01-01');
  for (let i = 0; i < 100; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);
    const dateStr = toISODateString(date);

    db.prepare('INSERT INTO topix_data (date, open, high, low, close) VALUES (?, 100, 105, 95, 102)').run(dateStr);
  }

  // Insert test stocks
  const stockInsert = db.prepare(`
    INSERT INTO stocks (
      code, company_name, company_name_english, market_code, market_name,
      sector17_code, sector17_name, sector33_code, sector33_name,
      scale_category, listed_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  stockInsert.run(
    '7203',
    'トヨタ自動車',
    'Toyota Motor Corporation',
    '0111',
    'Prime',
    '1',
    'Automobiles & Transportation Equipment',
    '3050',
    'Land Transportation',
    'Large',
    '1970-01-01'
  );

  stockInsert.run(
    '6758',
    'ソニーグループ',
    'Sony Group Corporation',
    '0111',
    'Prime',
    '17',
    'Electric Appliances',
    '3250',
    'Electric Appliances',
    'Large',
    '1970-01-01'
  );

  // Create mock object with required methods
  const mockDb = {
    close: () => db.close(),

    getTopixDateRange: () => {
      const stmt = db.prepare('SELECT MIN(date) as min, MAX(date) as max, COUNT(*) as count FROM topix_data');
      const row = stmt.get() as { min: string | null; max: string | null; count: number };

      return {
        min: row.min ? new Date(row.min) : null,
        max: row.max ? new Date(row.max) : null,
        count: row.count,
      };
    },

    insertStockDataBulk: (code: string, data: StockData[]) => {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO stock_data
        (code, date, open, high, low, close, volume, adjustment_factor)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      const insertMany = db.transaction((items: StockData[]) => {
        for (const item of items) {
          stmt.run(
            code,
            toISODateString(item.date),
            item.open,
            item.high,
            item.low,
            item.close,
            item.volume,
            item.adjustmentFactor || 1.0
          );
        }
      });

      insertMany(data);
    },

    markStockRefreshed: (code: string) => {
      const now = new Date().toISOString();
      db.prepare(`
        INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)
        VALUES (?, ?, ?)
      `).run(`stock_refreshed_${code}`, now, now);
    },
  } as unknown as MarketDatabase;

  return mockDb;
}

/**
 * Create mock API client
 */
function createMockApiClient(responseData: StockData[]): ApiClient {
  return {
    client: {
      getDailyQuotes: mock().mockResolvedValue({
        data: responseData.map((item) => ({
          Code: item.code,
          Date: toISODateString(item.date),
          O: item.open,
          H: item.high,
          L: item.low,
          C: item.close,
          Vo: item.volume,
          AdjO: item.open,
          AdjH: item.high,
          AdjL: item.low,
          AdjC: item.close,
          AdjVo: item.volume,
          AdjFactor: item.adjustmentFactor || 1.0,
        })),
      }),
    },
  } as unknown as ApiClient;
}

/**
 * Create mock rate limiter
 */
function createMockBatchExecutor(): BatchExecutor {
  return {
    execute: mock().mockImplementation(async <T>(fn: () => Promise<T>) => fn()),
  } as unknown as BatchExecutor;
}

/**
 * Generate stock data
 */
function generateStockData(code: string, days: number, startDate: Date): StockData[] {
  const data: StockData[] = [];

  for (let i = 0; i < days; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);

    data.push({
      code,
      date,
      open: 1000 + i,
      high: 1010 + i,
      low: 990 + i,
      close: 1005 + i,
      volume: 1000000 + i * 1000,
      adjustmentFactor: 1.0,
    });
  }

  return data;
}

describe('StockHistoryRefetcher', () => {
  let mockDb: MarketDatabase;
  let mockApiClient: ApiClient;
  let mockBatchExecutor: BatchExecutor;

  beforeEach(() => {
    mockDb = createMockDatabase();
    mockBatchExecutor = createMockBatchExecutor();
  });

  afterEach(() => {
    if (mockDb && typeof mockDb.close === 'function') {
      mockDb.close();
    }
  });

  describe('refetchStockHistory', () => {
    it('should successfully refetch stock history within TOPIX range', async () => {
      // Generate 150 days of data (extends beyond TOPIX range)
      const stockData = generateStockData('7203', 150, new Date('2023-12-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.success).toBe(true);
      expect(result.code).toBe('7203');
      expect(result.recordsFetched).toBe(150);
      // Only records within TOPIX range should be stored (100 days)
      expect(result.recordsStored).toBeLessThanOrEqual(100);
      expect(result.error).toBeUndefined();
    });

    it('should filter data to TOPIX date range', async () => {
      // Data from 2023-11-01 to 2024-04-30 (180 days)
      const stockData = generateStockData('7203', 180, new Date('2023-11-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, true);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.success).toBe(true);
      expect(result.recordsFetched).toBe(180);
      // TOPIX range is 2024-01-01 to 2024-04-09 (100 days)
      expect(result.recordsStored).toBe(100);
    });

    it('should handle empty API response', async () => {
      mockApiClient = createMockApiClient([]);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.success).toBe(true);
      expect(result.recordsFetched).toBe(0);
      expect(result.recordsStored).toBe(0);
    });

    it('should handle API errors gracefully', async () => {
      const errorApiClient = {
        client: {
          getDailyQuotes: mock().mockRejectedValue(new Error('API Error: Rate limit exceeded')),
        },
      } as unknown as ApiClient;

      const refetcher = new StockHistoryRefetcher(mockDb, errorApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.success).toBe(false);
      expect(result.code).toBe('7203');
      expect(result.recordsFetched).toBe(0);
      expect(result.recordsStored).toBe(0);
      expect(result.error).toContain('API Error');
    });

    it('should mark stock as refreshed on success', async () => {
      const stockData = generateStockData('7203', 100, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const markStockRefreshed = mock();
      const dbWithSpy = {
        ...mockDb,
        markStockRefreshed,
      };

      const refetcher = new StockHistoryRefetcher(
        dbWithSpy as unknown as MarketDatabase,
        mockApiClient,
        mockBatchExecutor,
        false
      );
      await refetcher.refetchStockHistory('7203');

      expect(markStockRefreshed).toHaveBeenCalledWith('7203');
    });

    it('should use rate limiter for API calls', async () => {
      const stockData = generateStockData('7203', 50, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      await refetcher.refetchStockHistory('7203');

      expect(mockBatchExecutor.execute).toHaveBeenCalled();
    });
  });

  describe('refetchMultipleStocks', () => {
    it('should refetch multiple stocks successfully', async () => {
      const stockData7203 = generateStockData('7203', 100, new Date('2024-01-01'));
      const stockData6758 = generateStockData('6758', 100, new Date('2024-01-01'));

      const mockGetDailyQuotesMulti = mock()
        .mockResolvedValueOnce({
          daily_quotes: stockData7203.map((item) => ({
            Code: item.code,
            Date: toISODateString(item.date),
            Open: item.open,
            High: item.high,
            Low: item.low,
            Close: item.close,
            Volume: item.volume,
            AdjustmentClose: item.close,
            AdjustmentFactor: 1.0,
          })),
        })
        .mockResolvedValueOnce({
          daily_quotes: stockData6758.map((item) => ({
            Code: item.code,
            Date: toISODateString(item.date),
            Open: item.open,
            High: item.high,
            Low: item.low,
            Close: item.close,
            Volume: item.volume,
            AdjustmentClose: item.close,
            AdjustmentFactor: 1.0,
          })),
        });

      const multiApiClient = {
        client: {
          getDailyQuotes: mockGetDailyQuotesMulti,
        },
      } as unknown as ApiClient;

      const refetcher = new StockHistoryRefetcher(mockDb, multiApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchMultipleStocks(['7203', '6758']);

      expect(result.totalStocks).toBe(2);
      expect(result.successCount).toBe(2);
      expect(result.failedCount).toBe(0);
      expect(result.totalApiCalls).toBe(2);
      expect(result.errors).toHaveLength(0);
    });

    it('should handle partial failures', async () => {
      const stockData = generateStockData('7203', 100, new Date('2024-01-01'));

      const mockGetDailyQuotes = mock()
        .mockResolvedValueOnce({
          daily_quotes: stockData.map((item) => ({
            Code: item.code,
            Date: toISODateString(item.date),
            Open: item.open,
            High: item.high,
            Low: item.low,
            Close: item.close,
            Volume: item.volume,
            AdjustmentClose: item.close,
            AdjustmentFactor: 1.0,
          })),
        })
        .mockRejectedValueOnce(new Error('API Error'));

      const partialApiClient = {
        client: {
          getDailyQuotes: mockGetDailyQuotes,
        },
      } as unknown as ApiClient;

      const refetcher = new StockHistoryRefetcher(mockDb, partialApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchMultipleStocks(['7203', '6758']);

      expect(result.totalStocks).toBe(2);
      expect(result.successCount).toBe(1);
      expect(result.failedCount).toBe(1);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]).toContain('6758');
    });

    it('should call progress callback', async () => {
      const stockData = generateStockData('7203', 100, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const progressCallback = mock();
      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);

      await refetcher.refetchMultipleStocks(['7203'], progressCallback);

      expect(progressCallback).toHaveBeenCalledWith(1, 1, '7203');
    });

    it('should handle empty stock list', async () => {
      mockApiClient = createMockApiClient([]);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchMultipleStocks([]);

      expect(result.totalStocks).toBe(0);
      expect(result.successCount).toBe(0);
      expect(result.failedCount).toBe(0);
      expect(result.totalApiCalls).toBe(0);
    });

    it('should skip undefined stock codes', async () => {
      const stockData = generateStockData('7203', 100, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchMultipleStocks(['7203', undefined as unknown as string, '']);

      // Should process only valid code
      expect(result.totalStocks).toBe(3);
      expect(result.totalApiCalls).toBeLessThanOrEqual(1);
    });
  });

  describe('TOPIX range filtering', () => {
    it('should filter out data before TOPIX start date', async () => {
      // Data starts before TOPIX range
      const stockData = generateStockData('7203', 120, new Date('2023-12-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, true);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.recordsFetched).toBe(120);
      // Should filter to TOPIX range (2024-01-01 onwards)
      expect(result.recordsStored).toBeLessThan(120);
    });

    it('should filter out data after TOPIX end date', async () => {
      // Data extends beyond TOPIX range
      const stockData = generateStockData('7203', 150, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, true);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.recordsFetched).toBe(150);
      // Should filter to TOPIX range (100 days)
      expect(result.recordsStored).toBe(100);
    });

    it('should handle data completely outside TOPIX range', async () => {
      // Data from 2023 (before TOPIX range)
      const stockData = generateStockData('7203', 100, new Date('2023-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      const result = await refetcher.refetchStockHistory('7203');

      expect(result.recordsFetched).toBe(100);
      // All data should be filtered out
      expect(result.recordsStored).toBe(0);
    });
  });

  describe('Debug mode', () => {
    it('should enable debug logging when debug=true', async () => {
      const consoleSpy = spyOn(console, 'error').mockImplementation(() => {});
      const stockData = generateStockData('7203', 50, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, true);
      await refetcher.refetchStockHistory('7203');

      expect(consoleSpy).toHaveBeenCalled();
      consoleSpy.mockRestore();
    });

    it('should not log in debug mode when debug=false', async () => {
      const consoleSpy = spyOn(console, 'error').mockImplementation(() => {});
      const stockData = generateStockData('7203', 50, new Date('2024-01-01'));
      mockApiClient = createMockApiClient(stockData);

      const refetcher = new StockHistoryRefetcher(mockDb, mockApiClient, mockBatchExecutor, false);
      await refetcher.refetchStockHistory('7203');

      // Should not have debug logs
      const debugCalls = consoleSpy.mock.calls.filter(
        (call) => call[0] && typeof call[0] === 'string' && call[0].includes('[REFETCH DEBUG]')
      );
      expect(debugCalls).toHaveLength(0);
      consoleSpy.mockRestore();
    });
  });
});
