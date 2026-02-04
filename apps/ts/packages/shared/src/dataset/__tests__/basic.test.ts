/**
 * Dataset - Basic Integration Tests
 * Tests for core functionality to ensure implementation works
 */

import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { DrizzleDatasetDatabase } from '../../db';
import { presets, validateConfig } from '../config';
import { DatasetReader } from '../reader';
import type { StockData, StockInfo } from '../types';
import { validateStockData, validateStockInfo } from '../validators';

/**
 * Clean up database files including WAL (Write-Ahead Logging) files
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
  return path.join(os.tmpdir(), `test-dataset-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

describe('Dataset V2 Basic Functionality', () => {
  let testDbPath: string;

  beforeEach(() => {
    // Generate unique path for this test
    testDbPath = getTestDbPath();
    // Clean up any existing test database
    cleanupDatabase(testDbPath);
  });

  afterEach(() => {
    // Clean up test database
    cleanupDatabase(testDbPath);
  });

  describe('Configuration', () => {
    it('should create valid preset configurations', () => {
      const primeConfig = presets.primeMarket('./test.db');
      expect(primeConfig.markets).toEqual(['prime']);
      expect(primeConfig.includeMargin).toBe(true);
      expect(primeConfig.includeTOPIX).toBe(true);

      const fullConfig = presets.fullMarket('./test.db');
      expect(fullConfig.markets).toEqual(['prime', 'standard', 'growth']);
      expect(fullConfig.includeStatements).toBe(true);

      const testingConfig = presets.quickTesting('./test.db');
      expect(testingConfig.maxStocks).toBe(3);
    });

    it('should validate configuration correctly', () => {
      const validConfig = presets.primeMarket('./test.db');
      expect(() => validateConfig(validConfig)).not.toThrow();

      const invalidConfig = {
        ...presets.primeMarket('./test.db'),
        outputPath: '', // Invalid empty path
      };
      expect(() => validateConfig(invalidConfig)).toThrow();
    });
  });

  describe('Database Operations', () => {
    it('should initialize database with proper schema', async () => {
      const db = new DrizzleDatasetDatabase(testDbPath);

      // Verify database is initialized and functional by checking stats
      const stats = await db.getDatasetStats();
      expect(stats.totalStocks).toBe(0);
      expect(stats.totalQuotes).toBe(0);

      await db.close();
    });

    it('should insert and retrieve stock data', async () => {
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

      const retrievedStocks = db.getStockList();
      expect(retrievedStocks).toHaveLength(1);
      const firstStock = retrievedStocks[0];
      expect(firstStock).toBeDefined();
      expect(firstStock?.code).toBe('7203');
      expect(firstStock?.companyName).toBe('トヨタ自動車');

      await db.close();
    });

    it('should insert and retrieve quote data', async () => {
      const db = new DrizzleDatasetDatabase(testDbPath);

      // First insert the stock (required for foreign key constraint)
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

      const testQuotes: StockData[] = [
        {
          code: '7203',
          date: new Date('2023-01-01'),
          open: 1000,
          high: 1100,
          low: 950,
          close: 1050,
          volume: 1000000,
          adjustmentFactor: 1.0,
        },
        {
          code: '7203',
          date: new Date('2023-01-02'),
          open: 1050,
          high: 1150,
          low: 1000,
          close: 1100,
          volume: 1200000,
          adjustmentFactor: 0.95,
        },
      ];

      await db.insertStockData('7203', testQuotes);

      const retrievedQuotes = db.getStockData('7203');
      expect(retrievedQuotes).toHaveLength(2);
      const firstQuote = retrievedQuotes[0];
      expect(firstQuote).toBeDefined();
      expect(firstQuote?.close).toBe(1050);
      const secondQuote = retrievedQuotes[1];
      expect(secondQuote).toBeDefined();
      expect(secondQuote?.close).toBe(1100);

      await db.close();
    });
  });

  describe('Dataset Reader', () => {
    it('should create reader and test connection', async () => {
      // First create a database with some data
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
      await db.close();

      // Now test the reader
      const reader = new DatasetReader(testDbPath);

      const canConnect = await reader.testConnection();
      expect(canConnect).toBe(true);

      const stocks = await reader.getStockList();
      expect(stocks).toHaveLength(1);
      const firstStock = stocks[0];
      expect(firstStock).toBeDefined();
      expect(firstStock?.code).toBe('7203');

      const stats = await reader.getDatasetStats();
      expect(stats.totalStocks).toBe(1);

      await reader.close();
    });
  });

  describe('Adjusted OHLC Data Handling', () => {
    it('should handle adjusted OHLC data correctly', async () => {
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

      const adjustedQuote: StockData = {
        code: '7203',
        date: new Date('2023-01-01'),
        open: 2000, // Adjusted price
        high: 2200, // Adjusted price
        low: 1900, // Adjusted price
        close: 2100, // Adjusted price
        volume: 1500000, // Adjusted volume
        adjustmentFactor: 0.5, // 2:1 stock split adjustment
      };

      await db.insertStockData('7203', [adjustedQuote]);

      const retrievedQuotes = db.getStockData('7203');
      expect(retrievedQuotes).toHaveLength(1);
      const quote = retrievedQuotes[0];
      expect(quote).toBeDefined();
      expect(quote?.adjustmentFactor).toBe(0.5);
      expect(quote?.open).toBe(2000);
      expect(quote?.high).toBe(2200);
      expect(quote?.low).toBe(1900);
      expect(quote?.close).toBe(2100);
      expect(quote?.volume).toBe(1500000);

      await db.close();
    });

    it('should validate adjustment factor correctly', () => {
      const validAdjustedData: StockData = {
        code: '7203',
        date: new Date('2023-01-01'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 1000000,
        adjustmentFactor: 0.5,
      };

      const errors = validateStockData(validAdjustedData);
      expect(errors).toHaveLength(0);

      const dataWithoutAdjustment: StockData = {
        code: '7203',
        date: new Date('2023-01-01'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 1000000,
      };

      const noAdjustmentErrors = validateStockData(dataWithoutAdjustment);
      expect(noAdjustmentErrors).toHaveLength(0);
    });
  });

  describe('Validation', () => {
    it('should validate stock info correctly', () => {
      const validStock: StockInfo = {
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

      const errors = validateStockInfo(validStock);
      expect(errors).toHaveLength(0);

      const invalidStock: StockInfo = {
        ...validStock,
        code: 'INVALID', // Should be 4 characters
      };

      const invalidErrors = validateStockInfo(invalidStock);
      expect(invalidErrors.length).toBeGreaterThan(0);
      expect(invalidErrors[0]).toContain('4 characters');
    });

    it('should validate stock data correctly', () => {
      const validData: StockData = {
        code: '7203',
        date: new Date('2023-01-01'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 1000000,
        adjustmentFactor: 1.0,
      };

      const errors = validateStockData(validData);
      expect(errors).toHaveLength(0);

      const invalidData: StockData = {
        ...validData,
        high: 900, // High should be >= low
      };

      const invalidErrors = validateStockData(invalidData);
      expect(invalidErrors.length).toBeGreaterThan(0);
    });
  });

  describe('Module Exports', () => {
    it('should export all main classes', async () => {
      const exports = await import('../index');

      expect(exports.DatasetBuilder).toBeDefined();
      expect(exports.DatasetReader).toBeDefined();
      expect(exports.Database).toBeDefined();
      expect(exports.presets).toBeDefined();
      expect(exports.validateConfig).toBeDefined();
    });
  });
});
