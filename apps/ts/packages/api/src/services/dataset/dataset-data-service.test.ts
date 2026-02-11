import { beforeEach, describe, expect, it, mock } from 'bun:test';

const mockGetDatasetPath = mock(() => '/tmp/test-dataset.db');
const mockLogger = {
  debug: mock(),
  info: mock(),
  warn: mock(),
  error: mock(),
};

// apps/bt/ API client mock
const mockResampleOHLCV = mock(
  async (request: { stock_code: string; timeframe: string; start_date?: string; end_date?: string }) => ({
    stock_code: request.stock_code,
    timeframe: request.timeframe,
    meta: { source_bars: 5, resampled_bars: 1 },
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
  })
);

class MockBacktestClient {
  async resampleOHLCV(request: { stock_code: string; timeframe: string; start_date?: string; end_date?: string }) {
    return mockResampleOHLCV(request);
  }
}

mock.module('@trading25/shared/clients/backtest', () => ({
  BacktestClient: MockBacktestClient,
}));

class MockDatasetReader {
  static stockDataByCode = new Map<
    string,
    Array<{ date: Date; open: number; high: number; low: number; close: number; volume: number }>
  >();
  static stockList: Array<{ code: string; sector33Code?: string; sector33Name?: string }> = [];
  static stockDateRangeByCode = new Map<string, { from: Date; to: Date } | null>();
  static statementsByCode = new Map<
    string,
    Array<{
      disclosedDate: Date;
      typeOfCurrentPeriod: string | null;
      typeOfDocument: string | null;
      earningsPerShare: number | null;
      profit: number | null;
      equity: number | null;
      nextYearForecastEarningsPerShare: number | null;
      bps: number | null;
      sales: number | null;
      operatingProfit: number | null;
      ordinaryProfit: number | null;
      operatingCashFlow: number | null;
      dividendFY: number | null;
      forecastEps: number | null;
      investingCashFlow: number | null;
      financingCashFlow: number | null;
      cashAndEquivalents: number | null;
      totalAssets: number | null;
      sharesOutstanding: number | null;
      treasuryShares: number | null;
    }>
  >();
  static marginByCode = new Map<
    string,
    Array<{ date: Date; longMarginVolume: number | null; shortMarginVolume: number | null }>
  >();
  static sectorData: Array<{
    sectorCode: string;
    sectorName: string;
    date: Date;
    open: number;
    high: number;
    low: number;
    close: number;
  }> = [];
  static closeCount = 0;

  constructor(public readonly path: string) {}

  async getStockData(code: string) {
    return MockDatasetReader.stockDataByCode.get(code) ?? [];
  }

  async getStockList() {
    return MockDatasetReader.stockList;
  }

  async getStockDateRange(code: string) {
    return MockDatasetReader.stockDateRangeByCode.get(code) ?? null;
  }

  async getStatementsData(code: string) {
    return MockDatasetReader.statementsByCode.get(code) ?? [];
  }

  async getMarginData(code: string) {
    return MockDatasetReader.marginByCode.get(code) ?? [];
  }

  async getSectorData() {
    return MockDatasetReader.sectorData;
  }

  async getTopixData() {
    return [] as Array<{ date: Date; open: number; high: number; low: number; close: number }>;
  }

  async close() {
    MockDatasetReader.closeCount += 1;
  }
}

mock.module('@trading25/shared/dataset', () => ({
  DatasetReader: MockDatasetReader,
}));

mock.module('@trading25/shared/utils/dataset-paths', () => ({
  getDatasetPath: mockGetDatasetPath,
}));

mock.module('@trading25/shared/utils/logger', () => ({
  logger: mockLogger,
}));

let datasetDataService: typeof import('./dataset-data-service').datasetDataService;

describe('DatasetDataService', () => {
  beforeEach(async () => {
    MockDatasetReader.stockDataByCode.clear();
    MockDatasetReader.stockList = [];
    MockDatasetReader.stockDateRangeByCode.clear();
    MockDatasetReader.statementsByCode.clear();
    MockDatasetReader.marginByCode.clear();
    MockDatasetReader.sectorData = [];
    MockDatasetReader.closeCount = 0;

    mockResampleOHLCV.mockClear();

    const moduleUrl = new URL('./dataset-data-service.ts', import.meta.url);
    moduleUrl.searchParams.set('test', 'dataset-service');
    datasetDataService = (await import(moduleUrl.href)).datasetDataService;
  });

  it('returns OHLCV data with timeframe conversion via apps/bt/ API', async () => {
    const result = await datasetDataService.getStockOHLCV('sample', '7203', { timeframe: 'weekly' });

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
    expect(mockResampleOHLCV).toHaveBeenCalledTimes(1);
    expect(mockResampleOHLCV).toHaveBeenCalledWith({
      stock_code: '7203',
      source: 'market',
      timeframe: 'weekly',
      start_date: undefined,
      end_date: undefined,
    });
    // apps/bt/ API経由なのでreaderはcloseされない
    expect(MockDatasetReader.closeCount).toBe(0);
  });

  it('returns daily OHLCV data from DatasetReader', async () => {
    MockDatasetReader.stockDataByCode.set('7203', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);

    const result = await datasetDataService.getStockOHLCV('sample', '7203', { timeframe: 'daily' });

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
    expect(mockResampleOHLCV).not.toHaveBeenCalled();
    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('returns batch OHLCV data and skips empty results', async () => {
    MockDatasetReader.stockDataByCode.set('7203', [
      {
        date: new Date('2025-01-02'),
        open: 1000,
        high: 1100,
        low: 950,
        close: 1050,
        volume: 100000,
      },
    ]);
    MockDatasetReader.stockDataByCode.set('6758', []);

    const result = await datasetDataService.getStockOHLCVBatch('sample', { codes: '7203,6758', timeframe: 'daily' });

    expect(result).toEqual({
      '7203': [
        {
          date: '2025-01-02',
          open: 1000,
          high: 1100,
          low: 950,
          close: 1050,
          volume: 100000,
        },
      ],
    });
    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('returns batch OHLCV via apps/bt/ API for weekly timeframe', async () => {
    const result = await datasetDataService.getStockOHLCVBatch('sample', { codes: '7203,6758', timeframe: 'weekly' });

    expect(result).toEqual({
      '7203': [
        {
          date: '2025-01-02',
          open: 1000,
          high: 1100,
          low: 950,
          close: 1050,
          volume: 100000,
        },
      ],
      '6758': [
        {
          date: '2025-01-02',
          open: 1000,
          high: 1100,
          low: 950,
          close: 1050,
          volume: 100000,
        },
      ],
    });
    expect(mockResampleOHLCV).toHaveBeenCalledTimes(2);
  });

  it('returns null when apps/bt/ API fails for weekly timeframe', async () => {
    mockResampleOHLCV.mockRejectedValueOnce(new Error('apps/bt/ API connection failed'));

    const result = await datasetDataService.getStockOHLCV('sample', '7203', { timeframe: 'weekly' });

    expect(result).toBeNull();
    expect(mockResampleOHLCV).toHaveBeenCalledTimes(1);
    expect(mockLogger.error).toHaveBeenCalledWith('Failed to resample OHLCV via apps/bt/ API', expect.any(Object));
  });

  it('returns null for batch when apps/bt/ API fails for all stocks', async () => {
    mockResampleOHLCV.mockRejectedValue(new Error('apps/bt/ API connection failed'));

    const result = await datasetDataService.getStockOHLCVBatch('sample', { codes: '7203,6758', timeframe: 'monthly' });

    expect(result).toBeNull();
    expect(mockResampleOHLCV).toHaveBeenCalledTimes(2);
  });

  it('returns partial results when apps/bt/ API fails for some stocks in batch', async () => {
    mockResampleOHLCV
      .mockResolvedValueOnce({
        stock_code: '7203',
        timeframe: 'monthly',
        meta: { source_bars: 20, resampled_bars: 1 },
        data: [{ date: '2025-01-31', open: 1000, high: 1100, low: 950, close: 1050, volume: 100000 }],
      })
      .mockRejectedValueOnce(new Error('apps/bt/ API failed for 6758'));

    const result = await datasetDataService.getStockOHLCVBatch('sample', { codes: '7203,6758', timeframe: 'monthly' });

    expect(result).toEqual({
      '7203': [{ date: '2025-01-31', open: 1000, high: 1100, low: 950, close: 1050, volume: 100000 }],
    });
    expect(mockResampleOHLCV).toHaveBeenCalledTimes(2);
  });

  it('returns detailed stock list with min record filtering', async () => {
    MockDatasetReader.stockList = [{ code: '7203' }, { code: '6758' }];
    MockDatasetReader.stockDateRangeByCode.set('7203', {
      from: new Date('2024-01-01'),
      to: new Date('2024-01-10'),
    });
    MockDatasetReader.stockDateRangeByCode.set('6758', {
      from: new Date('2024-01-01'),
      to: new Date('2024-01-02'),
    });
    MockDatasetReader.stockDataByCode.set('7203', [
      {
        date: new Date('2024-01-01'),
        open: 1,
        high: 1,
        low: 1,
        close: 1,
        volume: 1,
      },
      {
        date: new Date('2024-01-02'),
        open: 1,
        high: 1,
        low: 1,
        close: 1,
        volume: 1,
      },
    ]);
    MockDatasetReader.stockDataByCode.set('6758', [
      {
        date: new Date('2024-01-01'),
        open: 1,
        high: 1,
        low: 1,
        close: 1,
        volume: 1,
      },
    ]);

    const result = await datasetDataService.getStockList('sample', { detail: 'true', min_records: 2 });

    expect(result).toEqual([
      {
        stockCode: '7203',
        record_count: 2,
        start_date: '2024-01-01',
        end_date: '2024-01-10',
      },
    ]);
    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('filters statements by period type and actual-only flag', async () => {
    MockDatasetReader.statementsByCode.set('7203', [
      {
        disclosedDate: new Date('2024-03-31'),
        typeOfCurrentPeriod: 'FY',
        typeOfDocument: 'Annual',
        earningsPerShare: 120,
        profit: 1000,
        equity: 2000,
        nextYearForecastEarningsPerShare: null,
        bps: null,
        sales: null,
        operatingProfit: null,
        ordinaryProfit: null,
        operatingCashFlow: null,
        dividendFY: null,
        forecastEps: null,
        investingCashFlow: null,
        financingCashFlow: null,
        cashAndEquivalents: null,
        totalAssets: null,
        sharesOutstanding: null,
        treasuryShares: null,
      },
      {
        disclosedDate: new Date('2024-06-30'),
        typeOfCurrentPeriod: '1Q',
        typeOfDocument: 'Quarter',
        earningsPerShare: 10,
        profit: 100,
        equity: null,
        nextYearForecastEarningsPerShare: null,
        bps: null,
        sales: null,
        operatingProfit: null,
        ordinaryProfit: null,
        operatingCashFlow: null,
        dividendFY: null,
        forecastEps: null,
        investingCashFlow: null,
        financingCashFlow: null,
        cashAndEquivalents: null,
        totalAssets: null,
        sharesOutstanding: null,
        treasuryShares: null,
      },
    ]);

    const result = await datasetDataService.getStatements('sample', '7203', {
      period_type: 'FY',
      actual_only: 'true',
    });

    expect(result).toHaveLength(1);
    expect(result?.[0]?.typeOfCurrentPeriod).toBe('FY');
    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('getSectorMapping returns correct index_code from INDEX_MASTER_DATA', async () => {
    MockDatasetReader.stockList = [
      { code: '3200', sector33Code: '3200', sector33Name: '化学' },
      { code: '3201', sector33Code: '3200', sector33Name: '化学' },
      { code: '0050', sector33Code: '0050', sector33Name: '水産･農林業' },
      { code: '3650', sector33Code: '3650', sector33Name: '電気機器' },
    ];
    MockDatasetReader.sectorData = [];

    const result = await datasetDataService.getSectorMapping('sample');

    expect(result).not.toBeNull();
    expect(result).toHaveLength(3);

    const chemicalSector = result?.find((r) => r.sector_name === '化学');
    expect(chemicalSector).toBeDefined();
    expect(chemicalSector?.index_code).toBe('0046'); // NOT "3200"
    expect(chemicalSector?.sector_code).toBe('3200');

    const fisherySector = result?.find((r) => r.sector_name === '水産･農林業');
    expect(fisherySector).toBeDefined();
    expect(fisherySector?.index_code).toBe('0040'); // NOT "0050"

    const electricSector = result?.find((r) => r.sector_name === '電気機器');
    expect(electricSector).toBeDefined();
    expect(electricSector?.index_code).toBe('004F'); // NOT "3650"

    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('getSectorsWithCount returns correct index_code', async () => {
    MockDatasetReader.stockList = [
      { code: '4001', sector33Code: '3200', sector33Name: '化学' },
      { code: '4002', sector33Code: '3200', sector33Name: '化学' },
      { code: '1301', sector33Code: '0050', sector33Name: '水産･農林業' },
    ];
    MockDatasetReader.sectorData = [];

    const result = await datasetDataService.getSectorsWithCount('sample');

    expect(result).not.toBeNull();
    expect(result).toHaveLength(2);

    const chemicalSector = result?.find((r) => r.sector_name === '化学');
    expect(chemicalSector).toBeDefined();
    expect(chemicalSector?.index_code).toBe('0046');
    expect(chemicalSector?.stock_count).toBe(2);

    const fisherySector = result?.find((r) => r.sector_name === '水産･農林業');
    expect(fisherySector).toBeDefined();
    expect(fisherySector?.index_code).toBe('0040');
    expect(fisherySector?.stock_count).toBe(1);

    expect(MockDatasetReader.closeCount).toBe(1);
  });

  it('getSectorMapping resolves index_code when sector name contains halfwidth middle dot', async () => {
    MockDatasetReader.stockList = [{ code: '9432', sector33Code: '5250', sector33Name: '情報･通信業' }];
    MockDatasetReader.sectorData = [];

    const result = await datasetDataService.getSectorMapping('sample');

    expect(result).not.toBeNull();
    expect(result).toHaveLength(1);

    const infoCommSector = result?.find((r) => r.sector_name === '情報･通信業');
    expect(infoCommSector).toBeDefined();
    expect(infoCommSector?.index_code).toBe('0058');
  });
});
