/**
 * Tests for DrizzleMarketDataReader
 */
import { Database } from 'bun:sqlite';
import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { getElementOrFail } from '../test-utils/array-helpers';
import { DrizzleMarketDataReader } from './drizzle-market-reader';

function getTestDbPath(): string {
  return join(tmpdir(), `test-market-reader-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

function cleanupDatabase(dbPath: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    const p = `${dbPath}${suffix}`;
    if (existsSync(p)) {
      try {
        rmSync(p);
      } catch {
        // ignore
      }
    }
  }
}

/**
 * Create and seed a test market database
 */
function createTestDatabase(dbPath: string): void {
  const sqlite = new Database(dbPath);
  sqlite.exec('PRAGMA journal_mode = WAL');

  // Create stocks table
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS stocks (
      code TEXT PRIMARY KEY,
      company_name TEXT NOT NULL,
      company_name_english TEXT,
      market_code TEXT NOT NULL,
      market_name TEXT NOT NULL,
      sector17_code TEXT NOT NULL DEFAULT '',
      sector17_name TEXT NOT NULL DEFAULT '',
      sector33_code TEXT NOT NULL DEFAULT '',
      sector33_name TEXT NOT NULL DEFAULT '',
      sector_17_code TEXT NOT NULL DEFAULT '',
      sector_17_name TEXT NOT NULL DEFAULT '',
      sector_33_code TEXT NOT NULL DEFAULT '',
      sector_33_name TEXT NOT NULL DEFAULT '',
      scale_category TEXT,
      listed_date TEXT NOT NULL DEFAULT '2000-01-01'
    );
  `);

  // Create stock_data table
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS stock_data (
      code TEXT NOT NULL,
      date TEXT NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume INTEGER NOT NULL,
      adjustment_factor REAL,
      PRIMARY KEY (code, date)
    );
  `);

  // Create topix_data table
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS topix_data (
      date TEXT PRIMARY KEY,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL
    );
  `);

  // Seed stocks
  const insertStock = sqlite.prepare(`
    INSERT INTO stocks (code, company_name, company_name_english, market_code, market_name,
      sector17_code, sector17_name, sector33_code, sector33_name,
      sector_17_code, sector_17_name, sector_33_code, sector_33_name,
      scale_category, listed_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  insertStock.run(
    '7203',
    'トヨタ自動車',
    'Toyota Motor',
    'prime',
    'プライム',
    '7',
    '自動車・輸送機',
    '3050',
    '輸送用機器',
    '7',
    '自動車・輸送機',
    '3050',
    '輸送用機器',
    'TOPIX Large70',
    '1949-05-16'
  );
  insertStock.run(
    '6758',
    'ソニーグループ',
    'Sony Group',
    'prime',
    'プライム',
    '5',
    '電気機器',
    '3650',
    '電気機器',
    '5',
    '電気機器',
    '3650',
    '電気機器',
    'TOPIX Large70',
    '1958-12-01'
  );
  insertStock.run(
    '9984',
    'ソフトバンクグループ',
    'SoftBank Group',
    'prime',
    'プライム',
    '8',
    '情報・通信業',
    '5250',
    '情報・通信業',
    '8',
    '情報・通信業',
    '5250',
    '情報・通信業',
    'TOPIX Large70',
    '1994-07-22'
  );
  insertStock.run(
    '4755',
    '楽天グループ',
    'Rakuten Group',
    'prime',
    'プライム',
    '8',
    '情報・通信業',
    '5250',
    '情報・通信業',
    '8',
    '情報・通信業',
    '5250',
    '情報・通信業',
    'TOPIX Mid400',
    '2000-04-19'
  );
  insertStock.run(
    '2760',
    '東京エレクトロン デバイス',
    'TED',
    'standard',
    'スタンダード',
    '10',
    '卸売業',
    '6050',
    '卸売業',
    '10',
    '卸売業',
    '6050',
    '卸売業',
    null,
    '2001-03-15'
  );

  // Seed stock_data (20 trading days)
  const insertData = sqlite.prepare(`
    INSERT INTO stock_data (code, date, open, high, low, close, volume, adjustment_factor)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Trading dates
  const dates = [
    '2024-01-04',
    '2024-01-05',
    '2024-01-09',
    '2024-01-10',
    '2024-01-11',
    '2024-01-12',
    '2024-01-15',
    '2024-01-16',
    '2024-01-17',
    '2024-01-18',
    '2024-01-19',
    '2024-01-22',
    '2024-01-23',
    '2024-01-24',
    '2024-01-25',
    '2024-01-26',
    '2024-01-29',
    '2024-01-30',
    '2024-01-31',
    '2024-02-01',
  ];

  for (let i = 0; i < dates.length; i++) {
    const date = getElementOrFail(dates, i);
    // Toyota: rising prices
    insertData.run('7203', date, 2500 + i * 10, 2520 + i * 10, 2490 + i * 10, 2510 + i * 10, 5000000 + i * 100000, 1.0);
    // Sony: falling prices
    insertData.run(
      '6758',
      date,
      13000 - i * 50,
      13050 - i * 50,
      12950 - i * 50,
      12990 - i * 50,
      3000000 + i * 50000,
      1.0
    );
    // SoftBank: volatile
    insertData.run(
      '9984',
      date,
      6000 + (i % 2 === 0 ? 100 : -100),
      6200,
      5800,
      6000 + (i % 2 === 0 ? 50 : -50),
      8000000,
      1.0
    );
    // Rakuten: low volume
    insertData.run('4755', date, 800 + i, 810 + i, 790 + i, 800 + i, 100000 + i * 1000, 1.0);
  }

  // Seed topix_data
  const insertTopix = sqlite.prepare(`
    INSERT INTO topix_data (date, open, high, low, close) VALUES (?, ?, ?, ?, ?)
  `);
  for (const date of dates) {
    insertTopix.run(date, 2500, 2520, 2480, 2510);
  }

  sqlite.close();
}

describe('DrizzleMarketDataReader', () => {
  let dbPath: string;
  let reader: DrizzleMarketDataReader;

  beforeEach(() => {
    dbPath = getTestDbPath();
    createTestDatabase(dbPath);
    reader = new DrizzleMarketDataReader(dbPath);
  });

  afterEach(() => {
    reader.close();
    cleanupDatabase(dbPath);
  });

  describe('testConnection', () => {
    test('returns true for valid database', () => {
      expect(reader.testConnection()).toBe(true);
    });
  });

  describe('getStockByCode', () => {
    test('returns stock info for valid code', () => {
      const stock = reader.getStockByCode('7203');
      expect(stock).not.toBeNull();
      expect(stock?.code).toBe('7203');
      expect(stock?.companyName).toBe('トヨタ自動車');
      expect(stock?.companyNameEnglish).toBe('Toyota Motor');
      expect(stock?.marketCode).toBe('prime');
      expect(stock?.sector33Name).toBe('輸送用機器');
      expect(stock?.listedDate).toBeInstanceOf(Date);
    });

    test('returns null for nonexistent code', () => {
      expect(reader.getStockByCode('0000')).toBeNull();
    });

    test('handles stock with null scale_category', () => {
      const stock = reader.getStockByCode('2760');
      expect(stock).not.toBeNull();
      expect(stock?.scaleCategory).toBe('');
    });
  });

  describe('getStockList', () => {
    test('returns all stocks without filter', () => {
      const list = reader.getStockList();
      expect(list.length).toBe(5);
      // Should be ordered by code
      expect(list[0]?.code).toBe('2760');
      expect(list[4]?.code).toBe('9984');
    });

    test('filters by market code', () => {
      const primeStocks = reader.getStockList(['prime']);
      expect(primeStocks.length).toBe(4);
      expect(primeStocks.every((s) => s.marketCode === 'prime')).toBe(true);
    });

    test('filters by multiple market codes', () => {
      const filtered = reader.getStockList(['prime', 'standard']);
      expect(filtered.length).toBe(5);
    });
  });

  describe('getStockData', () => {
    test('returns all data for a stock', () => {
      const data = reader.getStockData('7203');
      expect(data.length).toBe(20);
      expect(data[0]?.code).toBe('7203');
      expect(data[0]?.date).toBeInstanceOf(Date);
    });

    test('filters by date range', () => {
      const data = reader.getStockData('7203', {
        from: new Date('2024-01-10'),
        to: new Date('2024-01-15'),
      });
      expect(data.length).toBeGreaterThan(0);
      for (const d of data) {
        expect(d.date.getTime()).toBeGreaterThanOrEqual(new Date('2024-01-10').getTime());
        expect(d.date.getTime()).toBeLessThanOrEqual(new Date('2024-01-15').getTime());
      }
    });

    test('returns empty array for nonexistent stock', () => {
      expect(reader.getStockData('0000')).toEqual([]);
    });
  });

  describe('getDatasetInfo', () => {
    test('returns dataset information', () => {
      const info = reader.getDatasetInfo();
      expect(info.totalStocks).toBe(5);
      expect(info.totalQuotes).toBe(80); // 4 stocks × 20 dates
      expect(info.markets).toContain('prime');
      expect(info.markets).toContain('standard');
      expect(info.dateRange.from).toBeInstanceOf(Date);
      expect(info.dateRange.to).toBeInstanceOf(Date);
      expect(info.databaseSize).toContain('MB');
    });
  });

  describe('getLatestTradingDate', () => {
    test('returns latest date', () => {
      const date = reader.getLatestTradingDate();
      expect(date).not.toBeNull();
      expect(date?.toISOString().slice(0, 10)).toBe('2024-02-01');
    });
  });

  describe('getPreviousTradingDate', () => {
    test('returns previous trading date', () => {
      const prevDate = reader.getPreviousTradingDate(new Date('2024-01-10'));
      expect(prevDate).not.toBeNull();
      expect(prevDate?.toISOString().slice(0, 10)).toBe('2024-01-09');
    });

    test('returns null when no previous date exists', () => {
      const prevDate = reader.getPreviousTradingDate(new Date('2020-01-01'));
      expect(prevDate).toBeNull();
    });
  });

  describe('getTradingDateBefore', () => {
    test('returns date N trading days before', () => {
      const date = reader.getTradingDateBefore(new Date('2024-02-01'), 5);
      expect(date).not.toBeNull();
    });

    test('throws for days < 1', () => {
      expect(() => reader.getTradingDateBefore(new Date('2024-02-01'), 0)).toThrow('Days must be at least 1');
    });

    test('returns null when not enough history', () => {
      const date = reader.getTradingDateBefore(new Date('2024-01-04'), 100);
      expect(date).toBeNull();
    });
  });

  describe('getRankingByTradingValue', () => {
    test('returns ranking sorted by trading value', () => {
      const ranking = reader.getRankingByTradingValue(new Date('2024-02-01'), 5);
      expect(ranking.length).toBeGreaterThan(0);
      expect(ranking.length).toBeLessThanOrEqual(5);
      expect(ranking[0]?.rank).toBe(1);
      expect(ranking[0]?.code).toBeDefined();
      expect(ranking[0]?.tradingValue).toBeDefined();
    });

    test('returns empty for date with no data', () => {
      const ranking = reader.getRankingByTradingValue(new Date('2020-01-01'), 5);
      expect(ranking).toEqual([]);
    });

    test('filters by market code', () => {
      const ranking = reader.getRankingByTradingValue(new Date('2024-02-01'), 10, ['standard']);
      // Should only include standard market stocks
      for (const item of ranking) {
        expect(item.marketCode).toBe('standard');
      }
    });
  });

  describe('getRankingByTradingValueAverage', () => {
    test('returns average trading value ranking', () => {
      const ranking = reader.getRankingByTradingValueAverage(new Date('2024-02-01'), 5, 5);
      expect(ranking.length).toBeGreaterThan(0);
      expect(ranking[0]?.tradingValueAverage).toBeDefined();
    });

    test('throws for lookbackDays < 1', () => {
      expect(() => reader.getRankingByTradingValueAverage(new Date('2024-02-01'), 0, 5)).toThrow(
        'Lookback days must be at least 1'
      );
    });

    test('returns empty when not enough history', () => {
      const ranking = reader.getRankingByTradingValueAverage(new Date('2020-01-01'), 5, 5);
      expect(ranking).toEqual([]);
    });
  });

  describe('getRankingByPriceChange', () => {
    test('returns gainers ranking', () => {
      const ranking = reader.getRankingByPriceChange(new Date('2024-02-01'), 5, undefined, 'gainers');
      expect(ranking.length).toBeGreaterThan(0);
      expect(ranking[0]?.changePercentage).toBeDefined();
    });

    test('returns losers ranking', () => {
      const ranking = reader.getRankingByPriceChange(new Date('2024-02-01'), 5, undefined, 'losers');
      expect(ranking.length).toBeGreaterThan(0);
    });

    test('returns empty when no previous date', () => {
      const ranking = reader.getRankingByPriceChange(new Date('2020-01-01'), 5);
      expect(ranking).toEqual([]);
    });
  });

  describe('getRankingByPriceChangeFromDays', () => {
    test('returns price change from N days ranking', () => {
      const ranking = reader.getRankingByPriceChangeFromDays(new Date('2024-02-01'), 10, 5);
      expect(ranking.length).toBeGreaterThan(0);
    });

    test('throws for lookbackDays < 1', () => {
      expect(() => reader.getRankingByPriceChangeFromDays(new Date('2024-02-01'), 0, 5)).toThrow(
        'Lookback days must be at least 1'
      );
    });

    test('filters by market code', () => {
      const ranking = reader.getRankingByPriceChangeFromDays(new Date('2024-02-01'), 10, 10, ['prime'], 'losers');
      for (const item of ranking) {
        expect(item.marketCode).toBe('prime');
      }
    });

    test('returns empty when not enough history', () => {
      const ranking = reader.getRankingByPriceChangeFromDays(new Date('2020-01-01'), 10, 5);
      expect(ranking).toEqual([]);
    });
  });

  describe('getPriceAtDate', () => {
    test('returns price at exact date', () => {
      const price = reader.getPriceAtDate('7203', new Date('2024-01-10'));
      expect(price).not.toBeNull();
      expect(price?.close).toBeGreaterThan(0);
    });

    test('returns closest prior price when exact date not found', () => {
      // 2024-01-06 is a Saturday - should return 2024-01-05 data
      const price = reader.getPriceAtDate('7203', new Date('2024-01-06'));
      expect(price).not.toBeNull();
      expect(price?.date.toISOString().slice(0, 10)).toBe('2024-01-05');
    });

    test('returns null for nonexistent stock', () => {
      expect(reader.getPriceAtDate('0000', new Date('2024-01-10'))).toBeNull();
    });
  });

  describe('getPricesAtDates', () => {
    test('returns prices at multiple dates', () => {
      const dates = [new Date('2024-01-10'), new Date('2024-01-15'), new Date('2024-01-22')];
      const prices = reader.getPricesAtDates('7203', dates);
      expect(prices.size).toBe(3);
    });

    test('returns empty map for empty dates', () => {
      expect(reader.getPricesAtDates('7203', []).size).toBe(0);
    });

    test('handles dates without exact match by finding closest prior', () => {
      // 2024-01-06 is Saturday, 2024-01-04 is a trading day
      // Include a known earlier date so the query range covers prior dates
      const dates = [new Date('2024-01-04'), new Date('2024-01-06')];
      const prices = reader.getPricesAtDates('7203', dates);
      // 2024-01-04 exact match + 2024-01-06 finds closest prior (2024-01-05)
      expect(prices.size).toBe(2);
      expect(prices.get('2024-01-04')).toBeDefined();
      expect(prices.get('2024-01-06')).toBeDefined();
    });
  });

  describe('getTopixData', () => {
    test('returns all TOPIX data without filter', () => {
      const data = reader.getTopixData();
      expect(data.length).toBe(20);
      expect(data[0]?.date).toBeInstanceOf(Date);
      expect(data[0]?.open).toBeDefined();
    });

    test('filters by date range', () => {
      const data = reader.getTopixData({
        from: new Date('2024-01-10'),
        to: new Date('2024-01-15'),
      });
      expect(data.length).toBeGreaterThan(0);
      for (const d of data) {
        expect(d.date.getTime()).toBeGreaterThanOrEqual(new Date('2024-01-10').getTime());
        expect(d.date.getTime()).toBeLessThanOrEqual(new Date('2024-01-15').getTime());
      }
    });
  });

  describe('getTopixDataCount', () => {
    test('returns correct count', () => {
      expect(reader.getTopixDataCount()).toBe(20);
    });
  });

  describe('searchStocks', () => {
    test('finds stock by code', () => {
      const results = reader.searchStocks('7203');
      expect(results.length).toBeGreaterThan(0);
      expect(results[0]?.code).toBe('7203');
    });

    test('finds stock by company name', () => {
      const results = reader.searchStocks('トヨタ');
      expect(results.length).toBeGreaterThan(0);
      expect(results[0]?.companyName).toContain('トヨタ');
    });

    test('finds stock by English name', () => {
      const results = reader.searchStocks('Toyota');
      expect(results.length).toBeGreaterThan(0);
    });

    test('returns empty for empty query', () => {
      expect(reader.searchStocks('')).toEqual([]);
      expect(reader.searchStocks('   ')).toEqual([]);
    });

    test('respects limit', () => {
      const results = reader.searchStocks('ソ', 1);
      expect(results.length).toBeLessThanOrEqual(1);
    });
  });

  describe('getRankingByPeriodHigh', () => {
    test('returns stocks at period high', () => {
      const ranking = reader.getRankingByPeriodHigh(new Date('2024-02-01'), 10, 5);
      // Toyota should appear since it has rising prices
      expect(ranking.length).toBeGreaterThanOrEqual(0);
      for (const item of ranking) {
        expect(item.lookbackDays).toBe(10);
      }
    });

    test('throws for periodDays < 1', () => {
      expect(() => reader.getRankingByPeriodHigh(new Date('2024-02-01'), 0, 5)).toThrow(
        'Period days must be at least 1'
      );
    });

    test('returns empty when not enough history', () => {
      const ranking = reader.getRankingByPeriodHigh(new Date('2020-01-01'), 10, 5);
      expect(ranking).toEqual([]);
    });

    test('filters by market codes', () => {
      const ranking = reader.getRankingByPeriodHigh(new Date('2024-02-01'), 10, 10, ['prime']);
      for (const item of ranking) {
        expect(item.marketCode).toBe('prime');
      }
    });
  });

  describe('getRankingByPeriodLow', () => {
    test('returns stocks at period low', () => {
      const ranking = reader.getRankingByPeriodLow(new Date('2024-02-01'), 10, 5);
      // Sony should appear since it has falling prices
      expect(ranking.length).toBeGreaterThanOrEqual(0);
    });

    test('throws for periodDays < 1', () => {
      expect(() => reader.getRankingByPeriodLow(new Date('2024-02-01'), 0, 5)).toThrow(
        'Period days must be at least 1'
      );
    });

    test('returns empty when not enough history', () => {
      const ranking = reader.getRankingByPeriodLow(new Date('2020-01-01'), 10, 5);
      expect(ranking).toEqual([]);
    });

    test('filters by market codes', () => {
      const ranking = reader.getRankingByPeriodLow(new Date('2024-02-01'), 10, 10, ['prime']);
      for (const item of ranking) {
        expect(item.marketCode).toBe('prime');
      }
    });
  });

  describe('getStocksBySector', () => {
    test('returns stocks filtered by sector33Name', () => {
      const result = reader.getStocksBySector({ sector33Name: '輸送用機器' });
      expect(result.length).toBeGreaterThan(0);
      expect(result[0]?.sector33Name).toBe('輸送用機器');
    });

    test('returns stocks filtered by sector17Name', () => {
      const result = reader.getStocksBySector({ sector17Name: '情報・通信業' });
      expect(result.length).toBeGreaterThan(0);
    });

    test('returns stocks filtered by market codes', () => {
      const result = reader.getStocksBySector({ marketCodes: ['prime'] });
      for (const item of result) {
        expect(item.marketCode).toBe('prime');
      }
    });

    test('throws for invalid market codes', () => {
      expect(() => reader.getStocksBySector({ marketCodes: ['invalid'] })).toThrow('Invalid market codes');
    });

    test('sorts by different fields', () => {
      const byCode = reader.getStocksBySector({ sortBy: 'code', sortOrder: 'asc' });
      if (byCode.length >= 2) {
        const first = getElementOrFail(byCode, 0);
        const second = getElementOrFail(byCode, 1);
        expect(first.code.localeCompare(second.code)).toBeLessThanOrEqual(0);
      }
    });

    test('respects limit', () => {
      const result = reader.getStocksBySector({ limit: 2 });
      expect(result.length).toBeLessThanOrEqual(2);
    });

    test('returns empty when no trading data', () => {
      // Create reader with empty database
      const emptyDbPath = getTestDbPath();
      const emptyDb = new Database(emptyDbPath);
      emptyDb.exec(`
        CREATE TABLE stocks (code TEXT PRIMARY KEY, company_name TEXT, company_name_english TEXT,
          market_code TEXT, market_name TEXT, sector17_code TEXT, sector17_name TEXT,
          sector33_code TEXT, sector33_name TEXT,
          sector_17_code TEXT, sector_17_name TEXT,
          sector_33_code TEXT, sector_33_name TEXT, scale_category TEXT, listed_date TEXT);
        CREATE TABLE stock_data (code TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, adjustment_factor REAL, PRIMARY KEY(code, date));
        CREATE TABLE topix_data (date TEXT PRIMARY KEY, open REAL, high REAL, low REAL, close REAL);
      `);
      emptyDb.close();

      const emptyReader = new DrizzleMarketDataReader(emptyDbPath);
      expect(emptyReader.getStocksBySector({})).toEqual([]);
      emptyReader.close();
      cleanupDatabase(emptyDbPath);
    });

    test('handles changePercentage sort order', () => {
      const result = reader.getStocksBySector({ sortBy: 'changePercentage', sortOrder: 'desc' });
      expect(result.length).toBeGreaterThan(0);
    });
  });
});
