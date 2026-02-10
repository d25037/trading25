/**
 * Dataset - Integration Tests
 * Comprehensive tests for the simplified dataset architecture
 */

import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { BatchExecutor } from '@trading25/clients-ts/base/BatchExecutor';
import { DrizzleDatasetDatabase } from '../../db';
import { createConfig, presets } from '../config';
import { ConsoleProgressFormatter, ProgressTracker } from '../progress';
import { DatasetReader } from '../reader';
import type { MarginData, ProgressInfo, SectorData, StatementsData, StockData, StockInfo, TopixData } from '../types';

/**
 * Clean up database files including WAL (Write-Ahead Logging) files
 * SQLite in WAL mode creates .db-wal and .db-shm files that must be cleaned up
 */
function cleanupDatabase(dbPath: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    const file = `${dbPath}${suffix}`;
    if (fs.existsSync(file)) {
      try {
        fs.unlinkSync(file);
      } catch {
        // Ignore cleanup errors
      }
    }
  }
}

/**
 * Generate unique test database path
 */
function getTestDbPath(): string {
  return path.join(os.tmpdir(), `integration-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

describe('Dataset V2 Integration Tests', () => {
  let testDbPath: string;

  beforeEach(() => {
    // Generate unique path and clean up
    testDbPath = getTestDbPath();
    cleanupDatabase(testDbPath);
  });

  afterEach(() => {
    // Clean up test database (including WAL files)
    cleanupDatabase(testDbPath);
  });

  describe('Complete Data Pipeline', () => {
    it('should create a complete dataset with all data types', async () => {
      const db = new DrizzleDatasetDatabase(testDbPath);

      try {
        // Insert test stocks
        const stocks: StockInfo[] = [
          {
            code: '7203',
            companyName: 'トヨタ自動車',
            companyNameEnglish: 'Toyota Motor Corporation',
            marketCode: '0111',
            marketName: 'Prime',
            sector17Code: '0001',
            sector17Name: '製造業',
            sector33Code: '0050',
            sector33Name: '輸送用機器',
            scaleCategory: 'TOPIX Core30',
            listedDate: new Date('1949-05-16'),
          },
          {
            code: '9984',
            companyName: 'ソフトバンクグループ',
            companyNameEnglish: 'SoftBank Group Corp.',
            marketCode: '0111',
            marketName: 'Prime',
            sector17Code: '0009',
            sector17Name: '情報・通信業',
            sector33Code: '1050',
            sector33Name: '情報・通信業',
            scaleCategory: 'TOPIX Core30',
            listedDate: new Date('1994-07-19'),
          },
        ];
        await db.insertStocks(stocks);

        // Insert stock quotes
        const quotes7203: StockData[] = [
          {
            code: '7203',
            date: new Date('2023-01-04'),
            open: 1840,
            high: 1860,
            low: 1820,
            close: 1850,
            volume: 15000000,
          },
        ];
        await db.insertStockData('7203', quotes7203);

        const quotes9984: StockData[] = [
          {
            code: '9984',
            date: new Date('2023-01-04'),
            open: 5150,
            high: 5200,
            low: 5100,
            close: 5180,
            volume: 8000000,
          },
        ];
        await db.insertStockData('9984', quotes9984);

        // Insert margin data
        const marginData: MarginData[] = [
          {
            code: '7203',
            date: new Date('2023-01-04'),
            longMarginVolume: 50000,
            shortMarginVolume: 25000,
          },
        ];
        await db.insertMarginData('7203', marginData);

        // Insert TOPIX data
        const topixData: TopixData[] = [
          {
            date: new Date('2023-01-04'),
            open: 1950.5,
            high: 1965.2,
            low: 1945.8,
            close: 1960.1,
          },
        ];
        await db.insertTopixData(topixData);

        // Insert sector data
        const sectorData: SectorData[] = [
          {
            sectorCode: '0050',
            sectorName: '輸送用機器',
            date: new Date('2023-01-04'),
            open: 850.2,
            high: 855.8,
            low: 845.1,
            close: 852.5,
          },
        ];
        await db.insertSectorData(sectorData);

        // Insert statements data
        const statementsData: StatementsData[] = [
          {
            code: '7203',
            disclosedDate: new Date('2023-05-10'),
            earningsPerShare: 285.5,
            profit: 1500000,
            equity: 15000000,
            typeOfCurrentPeriod: 'Annual',
            typeOfDocument: 'AnnualReport',
            nextYearForecastEarningsPerShare: 300.0,
            // Extended financial metrics
            bps: 2500.0,
            sales: 35000000,
            operatingProfit: 3000000,
            ordinaryProfit: 3200000,
            operatingCashFlow: 2000000,
            dividendFY: 60.0,
            forecastEps: 295.0,
            // Cash flow extended metrics
            investingCashFlow: -1000000,
            financingCashFlow: -500000,
            cashAndEquivalents: 8000000,
            totalAssets: 20000000,
            sharesOutstanding: 100000000,
            treasuryShares: 5000000,
          },
        ];
        await db.insertStatementsData('7203', statementsData);

        // Verify all data was inserted
        const stats = await db.getDatasetStats();
        expect(stats.totalStocks).toBe(2);
        expect(stats.totalQuotes).toBe(2);
        expect(stats.hasMarginData).toBe(true);
        expect(stats.hasTOPIXData).toBe(true);
        expect(stats.hasSectorData).toBe(true);
        expect(stats.hasStatementsData).toBe(true);

        // Test data retrieval
        const retrievedStocks = db.getStockList();
        expect(retrievedStocks).toHaveLength(2);
        expect(retrievedStocks.find((s) => s.code === '7203')).toBeDefined();
        expect(retrievedStocks.find((s) => s.code === '9984')).toBeDefined();

        const toyotaQuotes = db.getStockData('7203');
        expect(toyotaQuotes).toHaveLength(1);
        const firstQuote = toyotaQuotes[0];
        expect(firstQuote).toBeDefined();
        expect(firstQuote?.close).toBe(1850);

        const marginInfo = db.getMarginData('7203');
        expect(marginInfo).toHaveLength(1);
        const firstMargin = marginInfo[0];
        expect(firstMargin).toBeDefined();
        expect(firstMargin?.longMarginVolume).toBe(50000);

        const topix = db.getTopixData();
        expect(topix).toHaveLength(1);
        const firstTopix = topix[0];
        expect(firstTopix).toBeDefined();
        expect(firstTopix?.close).toBe(1960.1);

        const sectors = db.getSectorData('0050');
        expect(sectors).toHaveLength(1);
        const firstSector = sectors[0];
        expect(firstSector).toBeDefined();
        expect(firstSector?.close).toBe(852.5);
      } finally {
        await db.close();
      }
    });
  });

  describe('Dataset Reader Integration', () => {
    it('should read data from a complete dataset', async () => {
      // First create a dataset
      const db = new DrizzleDatasetDatabase(testDbPath);

      const testStock: StockInfo = {
        code: '7203',
        companyName: 'トヨタ自動車',
        companyNameEnglish: 'Toyota Motor Corporation',
        marketCode: '0111',
        marketName: 'Prime',
        sector17Code: '0001',
        sector17Name: '製造業',
        sector33Code: '0050',
        sector33Name: '輸送用機器',
        scaleCategory: 'TOPIX Core30',
        listedDate: new Date('1949-05-16'),
      };
      await db.insertStock(testStock);

      const quotes: StockData[] = [
        {
          code: '7203',
          date: new Date('2023-01-04'),
          open: 1840,
          high: 1860,
          low: 1820,
          close: 1850,
          volume: 15000000,
        },
        {
          code: '7203',
          date: new Date('2023-01-05'),
          open: 1850,
          high: 1880,
          low: 1840,
          close: 1870,
          volume: 12000000,
        },
      ];
      await db.insertStockData('7203', quotes);
      await db.close();

      // Now test the reader
      const reader = new DatasetReader(testDbPath);

      try {
        // Test connection
        const canConnect = await reader.testConnection();
        expect(canConnect).toBe(true);

        // Test stock listing
        const stocks = await reader.getStockList();
        expect(stocks).toHaveLength(1);
        const firstStock = stocks[0];
        expect(firstStock).toBeDefined();
        expect(firstStock?.code).toBe('7203');

        // Test quote data retrieval
        const toyotaData = await reader.getStockData('7203');
        expect(toyotaData).toHaveLength(2);
        const firstQuote = toyotaData[0];
        expect(firstQuote).toBeDefined();
        expect(firstQuote?.close).toBe(1850);
        const secondQuote = toyotaData[1];
        expect(secondQuote).toBeDefined();
        expect(secondQuote?.close).toBe(1870);

        // Test date range filtering
        const singleDayData = await reader.getStockData('7203', {
          from: new Date('2023-01-04'),
          to: new Date('2023-01-04'),
        });
        expect(singleDayData).toHaveLength(1);
        const firstSingleDayQuote = singleDayData[0];
        expect(firstSingleDayQuote).toBeDefined();
        expect(firstSingleDayQuote?.close).toBe(1850);

        // Test dataset statistics
        const stats = await reader.getDatasetStats();
        expect(stats.totalStocks).toBe(1);
        expect(stats.totalQuotes).toBe(2);

        // Test market filtering
        const primeStocks = await reader.getStockList(['0111']);
        expect(primeStocks).toHaveLength(1);
      } finally {
        await reader.close();
      }
    });
  });

  describe('Configuration and Presets', () => {
    it('should create valid configurations with different presets', () => {
      // Test primeMarket preset
      const primeConfig = presets.primeMarket(testDbPath);
      expect(primeConfig.markets).toEqual(['prime']);
      expect(primeConfig.includeMargin).toBe(true);
      expect(primeConfig.includeTOPIX).toBe(true);
      expect(primeConfig.includeSectorIndices).toBe(true);

      // Test fullMarket preset
      const fullConfig = presets.fullMarket(testDbPath);
      expect(fullConfig.markets).toEqual(['prime', 'standard', 'growth']);
      expect(fullConfig.includeStatements).toBe(true);

      // Test quickTesting preset
      const testingConfig = presets.quickTesting(testDbPath);
      expect(testingConfig.maxStocks).toBe(3);
      expect(testingConfig.includeStatements).toBe(true);

      // Test custom configuration
      const customConfig = createConfig({
        outputPath: testDbPath,
        markets: ['prime'],
        includeMargin: false,
        maxStocks: 5,
      });
      expect(customConfig.includeMargin).toBe(false);
      expect(customConfig.maxStocks).toBe(5);
      expect(customConfig.includeTOPIX).toBe(true); // Should keep defaults
    });
  });

  describe('Progress Reporting', () => {
    it('should track and report progress correctly', async () => {
      const progressEvents: ProgressInfo[] = [];

      const tracker = new ProgressTracker((progress) => {
        progressEvents.push({ ...progress });
      });

      // Simulate progress tracking
      tracker.startStage('stocks', 5);
      expect(tracker.getProgress().stage).toBe('stocks');

      tracker.setCurrentItem('Processing Toyota');
      tracker.updateProgress('Toyota processed successfully');

      tracker.updateProgress('Sony processed successfully');

      expect(progressEvents.length).toBeGreaterThan(0);
      const firstProgressEvent = progressEvents[0];
      expect(firstProgressEvent).toBeDefined();
      expect(firstProgressEvent?.stage).toBe('stocks');
      expect(firstProgressEvent?.total).toBe(5);
    });

    it('should format console progress correctly', () => {
      const formatter = new ConsoleProgressFormatter();

      const progress = {
        stage: 'quotes' as const,
        processed: 50,
        total: 100,
        currentItem: 'Processing 7203',
        errors: [],
      };

      const formatted = formatter.format(progress);
      expect(formatted).toContain('Quotes');
      expect(formatted).toContain('50/100');
      expect(formatted).toContain('7203');
    });
  });

  describe('Batch Execution', () => {
    it('should execute multiple operations', async () => {
      const executor = new BatchExecutor({
        maxRetries: 1,
        retryDelayMs: 100,
        maxRetryDelayMs: 1000,
      });

      const operations = [
        () => Promise.resolve('result1'),
        () => Promise.resolve('result2'),
        () => Promise.resolve('result3'),
      ];

      const results = await executor.executeAll(operations);

      expect(results).toEqual(['result1', 'result2', 'result3']);
    });

    it('should retry failed operations using executeAll', async () => {
      const executor = new BatchExecutor({
        maxRetries: 2,
        retryDelayMs: 10,
        maxRetryDelayMs: 100,
      });

      let attempts = 0;
      const flakyOperation = () => {
        attempts++;
        if (attempts < 3) {
          throw new Error('Temporary failure');
        }
        return Promise.resolve('success');
      };

      const results = await executor.executeAll([flakyOperation]);
      expect(results).toEqual(['success']);
      expect(attempts).toBe(3);
    });
  });

  describe('Error Handling', () => {
    it('should handle database errors gracefully', async () => {
      const db = new DrizzleDatasetDatabase(testDbPath);

      try {
        // Try to insert quotes without stock (foreign key violation)
        const badQuotes: StockData[] = [
          {
            code: 'INVALID',
            date: new Date('2023-01-01'),
            open: 100,
            high: 100,
            low: 100,
            close: 100,
            volume: 1000,
          },
        ];

        await expect(db.insertStockData('INVALID', badQuotes)).rejects.toThrow();
      } finally {
        await db.close();
      }
    });

    it('should handle reader errors gracefully', async () => {
      const reader = new DatasetReader('/nonexistent/path.db');

      await expect(reader.testConnection()).resolves.toBe(false);
      await expect(reader.getStockList()).rejects.toThrow();

      // Should not throw when closing non-existent connection
      await expect(reader.close()).resolves.toBeUndefined();
    });
  });

  describe('Statements Data Integration', () => {
    it('should verify statements data is included when enabled in testing preset', async () => {
      const testingConfig = presets.quickTesting(testDbPath);

      // Verify that testing preset has statements enabled
      expect(testingConfig.includeStatements).toBe(true);

      // Test database operations with statements data
      const db = new DrizzleDatasetDatabase(testDbPath);

      try {
        // Insert sample stock
        const testStock: StockInfo = {
          code: '7203',
          companyName: 'Toyota Motor Corp',
          companyNameEnglish: 'Toyota Motor Corporation',
          marketCode: 'T',
          marketName: 'Standard',
          sector17Code: '5000',
          sector17Name: 'Transportation Equipment',
          sector33Code: '5050',
          sector33Name: 'Auto & Truck Manufacturers',
          scaleCategory: 'TOPIX Large70',
          listedDate: new Date('1949-05-16'),
        };
        await db.insertStocks([testStock]);

        // Insert sample statements data
        const statementsData: StatementsData[] = [
          {
            code: '7203',
            disclosedDate: new Date('2023-05-10'),
            earningsPerShare: 285.5,
            profit: 1500000,
            equity: 15000000,
            typeOfCurrentPeriod: 'Annual',
            typeOfDocument: 'AnnualReport',
            nextYearForecastEarningsPerShare: 300.0,
            // Extended financial metrics
            bps: 2500.0,
            sales: 35000000,
            operatingProfit: 3000000,
            ordinaryProfit: 3200000,
            operatingCashFlow: 2000000,
            dividendFY: 60.0,
            forecastEps: 295.0,
            // Cash flow extended metrics
            investingCashFlow: -1000000,
            financingCashFlow: -500000,
            cashAndEquivalents: 8000000,
            totalAssets: 20000000,
            sharesOutstanding: 100000000,
            treasuryShares: 5000000,
          },
          {
            code: '7203',
            disclosedDate: new Date('2023-02-10'),
            earningsPerShare: 140.2,
            profit: 375000,
            equity: 14800000,
            typeOfCurrentPeriod: 'Quarterly',
            typeOfDocument: 'QuarterlyReport',
            nextYearForecastEarningsPerShare: null,
            // Extended financial metrics
            bps: 2450.0,
            sales: null,
            operatingProfit: null,
            ordinaryProfit: null,
            operatingCashFlow: null,
            dividendFY: null,
            forecastEps: null,
            // Cash flow extended metrics
            investingCashFlow: null,
            financingCashFlow: null,
            cashAndEquivalents: null,
            totalAssets: null,
            sharesOutstanding: null,
            treasuryShares: null,
          },
        ];

        // Test insertion
        await db.insertStatementsData('7203', statementsData);

        // Verify data was stored correctly
        const stats = await db.getDatasetStats();
        expect(stats.hasStatementsData).toBe(true);

        // Test data retrieval with reader
        const reader = new DatasetReader(testDbPath);

        try {
          // Verify statements table exists and has data
          await expect(reader.testConnection()).resolves.toBe(true);

          // Check if we can query statements (this will throw if table doesn't exist)
          const stockList = await reader.getStockList();
          expect(stockList).toHaveLength(1);
          expect(stockList[0]?.code).toBe('7203');
        } finally {
          await reader.close();
        }
      } finally {
        await db.close();
      }
    });

    it('should handle empty statements data gracefully', async () => {
      const db = new DrizzleDatasetDatabase(testDbPath);

      try {
        // Insert sample stock
        const testStock: StockInfo = {
          code: '8316',
          companyName: 'Test Bank',
          companyNameEnglish: 'Test Bank Ltd',
          marketCode: 'T',
          marketName: 'Standard',
          sector17Code: '7000',
          sector17Name: 'Banking',
          sector33Code: '7050',
          sector33Name: 'Banking',
          scaleCategory: 'TOPIX Large70',
          listedDate: new Date('2000-01-01'),
        };
        await db.insertStocks([testStock]);

        // Insert empty statements array (should not throw)
        await db.insertStatementsData('8316', []);

        // Verify stats show no statements data for empty insertion
        const stats = await db.getDatasetStats();
        expect(stats.hasStatementsData).toBe(false);
      } finally {
        await db.close();
      }
    });
  });
});
