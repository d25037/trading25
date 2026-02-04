import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockExistsSync = mock(() => true);
const mockGetMarketDbPath = mock(() => '/tmp/market.db');
const mockLogger = {
  debug: mock(),
  info: mock(),
  warn: mock(),
  error: mock(),
};

interface StockInfoData {
  code: string;
  companyName: string;
  companyNameEnglish: string;
  marketCode: string;
  marketName: string;
  sector17Code: string;
  sector17Name: string;
  sector33Code: string;
  sector33Name: string;
  scaleCategory: string;
  listedDate: Date;
}

class MockMarketDataReader {
  static stockDataByCode = new Map<
    string,
    Array<{ date: Date; open: number; high: number; low: number; close: number; volume: number }>
  >();
  static stockInfoByCode = new Map<string, StockInfoData>();
  static stockList: Array<{ code: string; companyName: string }> = [];
  static latestDate: Date | null = null;
  static tradingDateBefore: Date | null = null;
  static topixData: Array<{ date: Date; open: number; high: number; low: number; close: number }> = [];
  static shouldThrowError = false;

  constructor(public readonly path: string) {
    if (MockMarketDataReader.shouldThrowError) {
      throw new Error('Mock reader initialization error');
    }
  }

  getStockData(code: string) {
    return MockMarketDataReader.stockDataByCode.get(code) ?? [];
  }

  getStockByCode(code: string) {
    return MockMarketDataReader.stockInfoByCode.get(code) ?? null;
  }

  getStockList() {
    return MockMarketDataReader.stockList;
  }

  getLatestTradingDate() {
    return MockMarketDataReader.latestDate;
  }

  getTradingDateBefore() {
    return MockMarketDataReader.tradingDateBefore;
  }

  getTopixData() {
    return MockMarketDataReader.topixData;
  }
}

mock.module('node:fs', () => ({
  existsSync: mockExistsSync,
}));

mock.module('@trading25/shared/db', () => ({
  DrizzleMarketDataReader: MockMarketDataReader,
}));

mock.module('@trading25/shared/utils/dataset-paths', () => ({
  getMarketDbPath: mockGetMarketDbPath,
}));

mock.module('@trading25/shared/utils/logger', () => ({
  logger: mockLogger,
}));

let marketDataService: typeof import('./market-data-service').marketDataService;

describe('MarketDataService', () => {
  beforeEach(async () => {
    MockMarketDataReader.stockDataByCode.clear();
    MockMarketDataReader.stockInfoByCode.clear();
    MockMarketDataReader.stockList = [];
    MockMarketDataReader.latestDate = null;
    MockMarketDataReader.tradingDateBefore = null;
    MockMarketDataReader.topixData = [];
    MockMarketDataReader.shouldThrowError = false;

    mockExistsSync.mockClear();
    mockExistsSync.mockReturnValue(true);
    mockLogger.warn.mockClear();
    mockLogger.error.mockClear();
    mockLogger.debug.mockClear();

    const moduleUrl = new URL('./market-data-service.ts', import.meta.url);
    moduleUrl.searchParams.set('test', `market-service-${Date.now()}-${Math.random()}`);
    marketDataService = (await import(moduleUrl.href)).marketDataService;
  });

  it('returns stock OHLCV data', () => {
    // Note: market.db uses 5-digit codes with trailing "0" (e.g., "72030" for "7203")
    MockMarketDataReader.stockDataByCode.set('72030', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);

    const result = marketDataService.getStockOHLCV('7203', {});

    expect(result).toEqual([
      {
        date: '2025-01-02',
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);
  });

  it('returns all stocks with history range applied', () => {
    MockMarketDataReader.stockList = [
      { code: '7203', companyName: 'トヨタ自動車' },
      { code: '6758', companyName: 'ソニー' },
    ];
    MockMarketDataReader.latestDate = new Date('2025-01-10');
    MockMarketDataReader.tradingDateBefore = new Date('2024-12-01');
    MockMarketDataReader.stockDataByCode.set('7203', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);
    MockMarketDataReader.stockDataByCode.set('6758', []);

    const result = marketDataService.getAllStocks({ market: 'prime', history_days: 30 });

    expect(result).toEqual([
      {
        code: '7203',
        company_name: 'トヨタ自動車',
        data: [
          {
            date: '2025-01-02',
            open: 1000,
            high: 1100,
            low: 950,
            close: 1050,
            volume: 100000,
          },
        ],
      },
    ]);
  });

  it('returns TOPIX data', () => {
    MockMarketDataReader.topixData = [
      {
        date: new Date('2025-01-02'),
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
    ];

    const result = marketDataService.getTopix({});

    expect(result).toEqual([
      {
        date: '2025-01-02',
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
    ]);
  });

  it('normalizes 4-digit stock codes to 5-digit format', () => {
    expect(marketDataService.normalizeStockCode('7203')).toBe('72030');
    expect(marketDataService.normalizeStockCode('72030')).toBe('72030');
    expect(marketDataService.normalizeStockCode('123')).toBe('123');
  });

  it('returns stock info with normalized code', () => {
    MockMarketDataReader.stockInfoByCode.set('72030', {
      code: '72030',
      companyName: 'トヨタ自動車',
      companyNameEnglish: 'TOYOTA MOTOR CORPORATION',
      marketCode: 'prime',
      marketName: 'プライム',
      sector17Code: '7',
      sector17Name: '自動車・輸送機',
      sector33Code: '16',
      sector33Name: '輸送用機器',
      scaleCategory: 'TOPIX Large70',
      listedDate: new Date('1949-05-16'),
    });

    const result = marketDataService.getStockInfo('7203');

    expect(result).toEqual({
      code: '72030',
      companyName: 'トヨタ自動車',
      companyNameEnglish: 'TOYOTA MOTOR CORPORATION',
      marketCode: 'prime',
      marketName: 'プライム',
      sector17Code: '7',
      sector17Name: '自動車・輸送機',
      sector33Code: '16',
      sector33Name: '輸送用機器',
      scaleCategory: 'TOPIX Large70',
      listedDate: '1949-05-16',
    });
  });

  it('returns null when stock info not found', () => {
    const result = marketDataService.getStockInfo('9999');
    expect(result).toBeNull();
  });

  it('returns stock OHLCV with date range filter', () => {
    MockMarketDataReader.stockDataByCode.set('72030', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);

    const result = marketDataService.getStockOHLCV('7203', {
      start_date: '2025-01-01',
      end_date: '2025-01-10',
    });

    expect(result).toEqual([
      {
        date: '2025-01-02',
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);
  });

  it('logs warning when date range is invalid (start > end)', () => {
    MockMarketDataReader.stockDataByCode.set('72030', []);

    marketDataService.getStockOHLCV('7203', {
      start_date: '2025-12-31',
      end_date: '2025-01-01',
    });

    expect(mockLogger.warn).toHaveBeenCalledWith('Invalid date range: start_date is after end_date', {
      start_date: '2025-12-31',
      end_date: '2025-01-01',
    });
  });

  it('returns empty array when no latest trading date found', () => {
    MockMarketDataReader.stockList = [{ code: '7203', companyName: 'トヨタ自動車' }];
    MockMarketDataReader.latestDate = null;

    const result = marketDataService.getAllStocks({ market: 'prime', history_days: 30 });

    expect(result).toEqual([]);
    expect(mockLogger.warn).toHaveBeenCalledWith('No trading data found in market.db');
  });

  it('uses fallback date range when tradingDateBefore returns null', () => {
    MockMarketDataReader.stockList = [{ code: '7203', companyName: 'トヨタ自動車' }];
    MockMarketDataReader.latestDate = new Date('2025-01-10');
    MockMarketDataReader.tradingDateBefore = null;
    MockMarketDataReader.stockDataByCode.set('7203', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);

    const result = marketDataService.getAllStocks({ market: 'prime', history_days: 30 });

    expect(result).toEqual([
      {
        code: '7203',
        company_name: 'トヨタ自動車',
        data: [
          {
            date: '2025-01-02',
            open: 1000,
            high: 1100,
            low: 950,
            close: 1050,
            volume: 100000,
          },
        ],
      },
    ]);
  });

  it('returns TOPIX data with date range filter', () => {
    MockMarketDataReader.topixData = [
      {
        date: new Date('2025-01-02'),
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
    ];

    const result = marketDataService.getTopix({
      start_date: '2025-01-01',
      end_date: '2025-01-10',
    });

    expect(result).toEqual([
      {
        date: '2025-01-02',
        open: 2000,
        high: 2100,
        low: 1980,
        close: 2050,
      },
    ]);
  });
});
