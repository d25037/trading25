import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { StockData, StockInfo, TopixData } from '../dataset/types';
import { toISODateString } from '../utils/date-helpers';
import { DrizzleMarketDatabase, METADATA_KEYS } from './drizzle-market-database';
import { MARKET_SCHEMA_VERSION } from './schema/market-schema';

function cleanupDatabase(dbPath: string) {
  for (const suffix of ['', '-wal', '-shm']) {
    const filePath = `${dbPath}${suffix}`;
    if (fs.existsSync(filePath)) {
      try {
        fs.unlinkSync(filePath);
      } catch {
        // ignore cleanup errors
      }
    }
  }
}

function getTestDbPath(): string {
  return path.join(os.tmpdir(), `test-market-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);
}

describe('DrizzleMarketDatabase', () => {
  let dbPath: string;
  let db: DrizzleMarketDatabase | null = null;

  beforeEach(() => {
    dbPath = getTestDbPath();
    cleanupDatabase(dbPath);
  });

  afterEach(() => {
    if (db) {
      db.close();
      db = null;
    }
    cleanupDatabase(dbPath);
  });

  it('initializes schema and stores schema version metadata', () => {
    db = new DrizzleMarketDatabase(dbPath);
    const version = db.getMetadata('schema_version');
    expect(version).toBe(MARKET_SCHEMA_VERSION);
  });

  it('marks initialization and sets last sync date', () => {
    db = new DrizzleMarketDatabase(dbPath);
    db.markInitialized();

    expect(db.isInitialized()).toBe(true);
    expect(db.getMetadata(METADATA_KEYS.LAST_SYNC_DATE)).not.toBeNull();
  });

  it('tracks failed dates uniquely and clears them', () => {
    db = new DrizzleMarketDatabase(dbPath);
    const target = new Date('2025-01-02');

    db.recordFailedDate(target);
    db.recordFailedDate(target);

    const failed = db.getFailedDates();
    expect(failed).toHaveLength(1);
    expect(toISODateString(failed[0] ?? new Date())).toBe(toISODateString(target));

    db.clearFailedDate(target);
    expect(db.getFailedDates()).toHaveLength(0);
  });

  it('updates stock list and counts by market', () => {
    db = new DrizzleMarketDatabase(dbPath);

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
        code: '6758',
        companyName: 'ソニーグループ',
        companyNameEnglish: 'Sony Group Corporation',
        marketCode: '0112',
        marketName: 'Standard',
        sector17Code: '0002',
        sector17Name: '電気機器',
        sector33Code: '0050',
        sector33Name: '輸送用機器',
        scaleCategory: 'TOPIX Large70',
        listedDate: new Date('1958-12-01'),
      },
    ];

    db.updateStocksList(stocks);
    const counts = db.getStockCount();

    expect(counts.total).toBe(2);
    expect(counts.byMarket['0111']).toBe(1);
    expect(counts.byMarket['0112']).toBe(1);
  });

  it('inserts TOPIX data and detects missing trading days', () => {
    db = new DrizzleMarketDatabase(dbPath);

    const topixData: TopixData[] = [
      {
        date: new Date('2025-01-02'),
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
      {
        date: new Date('2025-01-03'),
        open: 2050,
        high: 2080,
        low: 2000,
        close: 2020,
      },
    ];

    db.insertTopixData(topixData);

    const tradingDays = db.getTradingDays();
    expect(tradingDays).toHaveLength(2);

    const missing = db.getMissingTradingDays([new Date('2025-01-02'), new Date('2025-01-03'), new Date('2025-01-06')]);
    expect(missing).toHaveLength(1);
    expect(toISODateString(missing[0] ?? new Date())).toBe('2025-01-06');
  });

  it('detects missing stock data dates against trading days', () => {
    db = new DrizzleMarketDatabase(dbPath);

    db.updateStocksList([
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
    ]);

    db.insertTopixData([
      {
        date: new Date('2025-01-02'),
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
      {
        date: new Date('2025-01-03'),
        open: 2050,
        high: 2080,
        low: 2000,
        close: 2020,
      },
    ]);

    const stockData: StockData[] = [
      {
        code: '7203',
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
        adjustmentFactor: 1.0,
      },
    ];

    db.insertStockDataForDate(new Date('2025-01-02'), stockData);

    const missing = db.getMissingStockDataDates();
    expect(missing).toHaveLength(1);
    expect(toISODateString(missing[0] ?? new Date())).toBe('2025-01-03');
  });

  it('tracks adjustment events and refetch status', () => {
    db = new DrizzleMarketDatabase(dbPath);

    db.updateStocksList([
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
    ]);

    db.insertStockDataBulk('7203', [
      {
        code: '7203',
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
        adjustmentFactor: 0.5,
      },
      {
        code: '7203',
        date: new Date('2025-01-03'),
        open: 1050,
        high: 1150,
        low: 1000,
        close: 1100,
        volume: 120000,
        adjustmentFactor: 1.0,
      },
    ]);

    const events = db.getAdjustmentEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events[0]?.adjustmentFactor).toBe(0.5);

    const needsRefresh = db.getStocksNeedingRefresh();
    expect(needsRefresh).toContain('7203');

    expect(db.getLatestAdjustmentFactor('7203')).toBe(0.5);
    expect(db.needsRefetch('7203')).toBe(true);

    db.markStockRefreshed('7203', 0.5, new Date('2025-01-04'));
    expect(db.needsRefetch('7203')).toBe(false);
  });

  it('initializes index master and inserts indices data', () => {
    db = new DrizzleMarketDatabase(dbPath);

    expect(db.isIndexMasterInitialized()).toBe(false);
    db.initializeIndexMaster();
    expect(db.isIndexMasterInitialized()).toBe(true);
    expect(db.getIndexMasterCount()).toBeGreaterThan(0);

    const codes = db.getIndexCodes();
    const firstCode = codes[0];
    expect(firstCode).toBeDefined();

    db.insertIndicesDataForDate(new Date('2025-01-02'), [
      {
        code: firstCode ?? '0000',
        open: 100,
        high: 110,
        low: 90,
        close: 105,
      },
    ]);

    const range = db.getIndicesDataRange();
    expect(range.count).toBe(1);

    const data = db.getIndicesDataByCode(firstCode ?? '0000');
    expect(data).toHaveLength(1);
  });
});
