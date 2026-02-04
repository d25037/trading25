/**
 * DrizzleDatasetDatabase Tests
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, unlinkSync } from 'node:fs';
import type { MarginData, SectorData, StatementsData, StockData, StockInfo, TopixData } from '../dataset/types';
import { DrizzleDatasetDatabase } from './drizzle-dataset-database';

const TEST_DB_PATH = '/tmp/test-dataset-drizzle.db';

function cleanupTestDb(): void {
  for (const ext of ['', '-shm', '-wal']) {
    const file = `${TEST_DB_PATH}${ext}`;
    if (existsSync(file)) {
      unlinkSync(file);
    }
  }
}

function createTestStock(code: string, marketCode = '0111'): StockInfo {
  return {
    code,
    companyName: `Test Company ${code}`,
    companyNameEnglish: `Test Company ${code} EN`,
    marketCode,
    marketName: marketCode === '0111' ? 'プライム' : 'スタンダード',
    sector17Code: '01',
    sector17Name: '食料品',
    sector33Code: '0050',
    sector33Name: '水産・農林業',
    scaleCategory: 'TOPIX Core30',
    listedDate: new Date('2020-01-01'),
  };
}

function createTestQuote(code: string, dateStr: string): StockData {
  return {
    code,
    date: new Date(dateStr),
    open: 100,
    high: 110,
    low: 95,
    close: 105,
    volume: 10000,
    adjustmentFactor: 1.0,
  };
}

function createTestTopix(dateStr: string): TopixData {
  return {
    date: new Date(dateStr),
    open: 2000,
    high: 2050,
    low: 1980,
    close: 2030,
  };
}

function createTestSector(sectorCode: string, dateStr: string): SectorData {
  return {
    sectorCode,
    sectorName: `Sector ${sectorCode}`,
    date: new Date(dateStr),
    open: 500,
    high: 510,
    low: 495,
    close: 505,
  };
}

function createTestMargin(code: string, dateStr: string): MarginData {
  return {
    code,
    date: new Date(dateStr),
    longMarginVolume: 1000,
    shortMarginVolume: 500,
  };
}

function createTestStatement(code: string, dateStr: string): StatementsData {
  return {
    code,
    disclosedDate: new Date(dateStr),
    earningsPerShare: 150.5,
    profit: 1000000000,
    equity: 5000000000,
    typeOfCurrentPeriod: 'FY',
    typeOfDocument: '有価証券報告書',
    nextYearForecastEarningsPerShare: 160.0,
    // Extended financial metrics
    bps: 2500.0,
    sales: 35000000000,
    operatingProfit: 3000000000,
    ordinaryProfit: 3200000000,
    operatingCashFlow: 2000000000,
    dividendFY: 60.0,
    forecastEps: 155.0,
    // Cash flow extended metrics
    investingCashFlow: -1000000000,
    financingCashFlow: -500000000,
    cashAndEquivalents: 8000000000,
    totalAssets: 20000000000,
    sharesOutstanding: 100000000,
    treasuryShares: 5000000,
  };
}

describe('DrizzleDatasetDatabase', () => {
  let db: DrizzleDatasetDatabase;

  beforeEach(() => {
    cleanupTestDb();
    db = new DrizzleDatasetDatabase(TEST_DB_PATH);
  });

  afterEach(async () => {
    await db.close();
    cleanupTestDb();
  });

  describe('Initialization', () => {
    test('should create database with schema version', () => {
      const version = db.getSchemaVersion();
      expect(version).toBe('2.3.0');
    });

    test('should allow setting and getting metadata', () => {
      db.setMetadata('test_key', 'test_value');
      expect(db.getMetadata('test_key')).toBe('test_value');
    });

    test('should return null for non-existent metadata', () => {
      expect(db.getMetadata('non_existent')).toBeNull();
    });
  });

  describe('Stock Operations', () => {
    test('should insert and retrieve a single stock', async () => {
      const stock = createTestStock('7203');
      await db.insertStock(stock);

      const stocks = db.getStockList();
      expect(stocks).toHaveLength(1);
      expect(stocks[0]?.code).toBe('7203');
      expect(stocks[0]?.companyName).toBe('Test Company 7203');
    });

    test('should normalize 5-digit stock codes to 4-digit', async () => {
      const stock = createTestStock('72030'); // 5-digit code
      await db.insertStock(stock);

      const stocks = db.getStockList();
      expect(stocks).toHaveLength(1);
      expect(stocks[0]?.code).toBe('7203'); // Should be normalized to 4-digit
    });

    test('should insert multiple stocks in transaction', async () => {
      const stocks = [
        createTestStock('7203', '0111'),
        createTestStock('9984', '0111'),
        createTestStock('6758', '0112'),
      ];
      await db.insertStocks(stocks);

      const allStocks = db.getStockList();
      expect(allStocks).toHaveLength(3);
    });

    test('should filter stocks by market code', async () => {
      const stocks = [
        createTestStock('7203', '0111'),
        createTestStock('9984', '0111'),
        createTestStock('6758', '0112'),
      ];
      await db.insertStocks(stocks);

      const primeStocks = db.getStockList(['0111']);
      expect(primeStocks).toHaveLength(2);

      const standardStocks = db.getStockList(['0112']);
      expect(standardStocks).toHaveLength(1);
    });

    test('should update existing stock on conflict', async () => {
      const stock1 = createTestStock('7203');
      await db.insertStock(stock1);

      const stock2 = { ...createTestStock('7203'), companyName: 'Updated Company' };
      await db.insertStock(stock2);

      const stocks = db.getStockList();
      expect(stocks).toHaveLength(1);
      expect(stocks[0]?.companyName).toBe('Updated Company');
    });
  });

  describe('Quote Operations', () => {
    test('should insert and retrieve quotes', async () => {
      await db.insertStock(createTestStock('7203'));
      const quotes = [createTestQuote('7203', '2024-01-01'), createTestQuote('7203', '2024-01-02')];
      await db.insertStockData('7203', quotes);

      const retrieved = db.getStockData('7203');
      expect(retrieved).toHaveLength(2);
    });

    test('should filter quotes by date range', async () => {
      await db.insertStock(createTestStock('7203'));
      const quotes = [
        createTestQuote('7203', '2024-01-01'),
        createTestQuote('7203', '2024-01-15'),
        createTestQuote('7203', '2024-02-01'),
      ];
      await db.insertStockData('7203', quotes);

      const filtered = db.getStockData('7203', {
        from: new Date('2024-01-10'),
        to: new Date('2024-01-20'),
      });
      expect(filtered).toHaveLength(1);
      expect(filtered[0]?.date.toISOString().slice(0, 10)).toBe('2024-01-15');
    });

    test('should normalize stock code in quotes', async () => {
      await db.insertStock(createTestStock('7203'));
      await db.insertStockData('72030', [createTestQuote('72030', '2024-01-01')]);

      const quotes = db.getStockData('7203');
      expect(quotes).toHaveLength(1);
    });
  });

  describe('TOPIX Operations', () => {
    test('should insert and retrieve TOPIX data', async () => {
      const topixData = [createTestTopix('2024-01-01'), createTestTopix('2024-01-02')];
      await db.insertTopixData(topixData);

      const retrieved = db.getTopixData();
      expect(retrieved).toHaveLength(2);
    });

    test('should filter TOPIX data by date range', async () => {
      const topixData = [createTestTopix('2024-01-01'), createTestTopix('2024-01-15'), createTestTopix('2024-02-01')];
      await db.insertTopixData(topixData);

      const filtered = db.getTopixData({
        from: new Date('2024-01-10'),
        to: new Date('2024-01-20'),
      });
      expect(filtered).toHaveLength(1);
    });
  });

  describe('Sector Operations', () => {
    test('should insert and retrieve sector data', async () => {
      const sectorData = [createTestSector('0050', '2024-01-01'), createTestSector('0051', '2024-01-01')];
      await db.insertSectorData(sectorData);

      const retrieved = db.getSectorData();
      expect(retrieved).toHaveLength(2);
    });

    test('should filter sector data by sector code', async () => {
      const sectorData = [
        createTestSector('0050', '2024-01-01'),
        createTestSector('0050', '2024-01-02'),
        createTestSector('0051', '2024-01-01'),
      ];
      await db.insertSectorData(sectorData);

      const filtered = db.getSectorData('0050');
      expect(filtered).toHaveLength(2);
    });

    test('should filter sector data by date range', async () => {
      const sectorData = [
        createTestSector('0050', '2024-01-01'),
        createTestSector('0050', '2024-01-15'),
        createTestSector('0050', '2024-02-01'),
      ];
      await db.insertSectorData(sectorData);

      const filtered = db.getSectorData(undefined, {
        from: new Date('2024-01-10'),
        to: new Date('2024-01-20'),
      });
      expect(filtered).toHaveLength(1);
    });
  });

  describe('Margin Operations', () => {
    test('should insert and retrieve margin data', async () => {
      await db.insertStock(createTestStock('7203'));
      const marginData = [createTestMargin('7203', '2024-01-01'), createTestMargin('7203', '2024-01-02')];
      await db.insertMarginData('7203', marginData);

      const retrieved = db.getMarginData('7203');
      expect(retrieved).toHaveLength(2);
    });

    test('should filter margin data by date range', async () => {
      await db.insertStock(createTestStock('7203'));
      const marginData = [
        createTestMargin('7203', '2024-01-01'),
        createTestMargin('7203', '2024-01-15'),
        createTestMargin('7203', '2024-02-01'),
      ];
      await db.insertMarginData('7203', marginData);

      const filtered = db.getMarginData('7203', {
        from: new Date('2024-01-10'),
        to: new Date('2024-01-20'),
      });
      expect(filtered).toHaveLength(1);
    });
  });

  describe('Statements Operations', () => {
    test('should insert and retrieve statements data', async () => {
      await db.insertStock(createTestStock('7203'));
      const statements = [createTestStatement('7203', '2024-03-31'), createTestStatement('7203', '2024-06-30')];
      await db.insertStatementsData('7203', statements);

      const retrieved = db.getStatementsData('7203');
      expect(retrieved).toHaveLength(2);
    });

    test('should retrieve all statements without filter', async () => {
      await db.insertStock(createTestStock('7203'));
      await db.insertStock(createTestStock('9984'));

      await db.insertStatementsData('7203', [createTestStatement('7203', '2024-03-31')]);
      await db.insertStatementsData('9984', [createTestStatement('9984', '2024-03-31')]);

      const all = db.getStatementsData();
      expect(all).toHaveLength(2);
    });

    test('should return statements field coverage statistics', async () => {
      await db.insertStock(createTestStock('7203'));
      await db.insertStock(createTestStock('9984'));

      // FY statement with all fields
      const fyStatement: StatementsData = {
        ...createTestStatement('7203', '2024-03-31'),
        typeOfCurrentPeriod: 'FY',
      };
      // 2Q statement (half-year) - some fields may be null
      const q2Statement: StatementsData = {
        ...createTestStatement('7203', '2023-09-30'),
        typeOfCurrentPeriod: '2Q',
        nextYearForecastEarningsPerShare: null, // FY-only field
        bps: null, // FY-only field
        dividendFY: null, // FY-only field
      };
      // 1Q statement
      const q1Statement: StatementsData = {
        ...createTestStatement('9984', '2024-06-30'),
        typeOfCurrentPeriod: '1Q',
        nextYearForecastEarningsPerShare: null,
        bps: null,
        dividendFY: null,
        operatingCashFlow: null, // FY+2Q only
      };

      await db.insertStatementsData('7203', [fyStatement, q2Statement]);
      await db.insertStatementsData('9984', [q1Statement]);

      const coverage = db.getStatementsFieldCoverage();

      // Total counts
      expect(coverage.total).toBe(3);
      expect(coverage.totalFY).toBe(1); // Only FY statement
      expect(coverage.totalHalf).toBe(2); // FY + 2Q statements

      // Schema has extended fields
      expect(coverage.hasExtendedFields).toBe(true);

      // Core fields (all 3 statements have them)
      expect(coverage.earningsPerShare).toBe(3);
      expect(coverage.profit).toBe(3);
      expect(coverage.equity).toBe(3);

      // FY-only field: nextYearForecastEps
      expect(coverage.nextYearForecastEps).toBe(1); // Only FY statement

      // Extended fields
      expect(coverage.bps).toBe(1); // FY-only
      expect(coverage.sales).toBe(3);
      expect(coverage.operatingProfit).toBe(3);
      expect(coverage.ordinaryProfit).toBe(3);
      expect(coverage.operatingCashFlow).toBe(2); // FY + 2Q
      expect(coverage.dividendFY).toBe(1); // FY-only
      expect(coverage.forecastEps).toBe(3);
    });

    test('should return zero coverage for empty statements table', () => {
      const coverage = db.getStatementsFieldCoverage();

      expect(coverage.total).toBe(0);
      expect(coverage.totalFY).toBe(0);
      expect(coverage.totalHalf).toBe(0);
      expect(coverage.hasExtendedFields).toBe(true);
      expect(coverage.earningsPerShare).toBe(0);
    });
  });

  describe('Statistics', () => {
    test('should return dataset statistics', async () => {
      await db.insertStock(createTestStock('7203'));
      await db.insertStockData('7203', [createTestQuote('7203', '2024-01-01'), createTestQuote('7203', '2024-01-02')]);
      await db.insertTopixData([createTestTopix('2024-01-01')]);
      await db.insertSectorData([createTestSector('0050', '2024-01-01')]);
      await db.insertMarginData('7203', [createTestMargin('7203', '2024-01-01')]);
      await db.insertStatementsData('7203', [createTestStatement('7203', '2024-03-31')]);

      const stats = await db.getDatasetStats();

      expect(stats.totalStocks).toBe(1);
      expect(stats.totalQuotes).toBe(2);
      expect(stats.markets).toContain('プライム');
      expect(stats.hasTOPIXData).toBe(true);
      expect(stats.hasSectorData).toBe(true);
      expect(stats.hasMarginData).toBe(true);
      expect(stats.hasStatementsData).toBe(true);
      expect(stats.databaseSize).toBeGreaterThan(0);
    });

    test('should return empty statistics for new database', async () => {
      const stats = await db.getDatasetStats();

      expect(stats.totalStocks).toBe(0);
      expect(stats.totalQuotes).toBe(0);
      expect(stats.hasTOPIXData).toBe(false);
      expect(stats.hasSectorData).toBe(false);
      expect(stats.hasMarginData).toBe(false);
      expect(stats.hasStatementsData).toBe(false);
    });
  });

  describe('Transaction Support', () => {
    test('should execute operations within transaction', () => {
      const result = db.withTransaction(() => {
        db.setMetadata('key1', 'value1');
        db.setMetadata('key2', 'value2');
        return 'success';
      });

      expect(result).toBe('success');
      expect(db.getMetadata('key1')).toBe('value1');
      expect(db.getMetadata('key2')).toBe('value2');
    });
  });
});
