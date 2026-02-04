import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { getFirstElementOrFail } from '@trading25/shared/test-utils';
import type { JQuantsStatement } from '@trading25/shared/types/jquants';

// ---------------------------------------------------------------------------
// Mock setup (must be before any import of the module under test)
// ---------------------------------------------------------------------------

const mockGetStatements = mock<() => Promise<{ data: JQuantsStatement[] }>>(() => Promise.resolve({ data: [] }));

const mockGetMarketDbPath = mock(() => '/tmp/test-market.db');

class MockMarketDataReader {
  static priceMap = new Map<string, number>();
  static stockInfo: { companyName: string } | null = null;
  static stockData: Array<{ date: Date; close: number }> = [];
  static closeCalled = false;

  getPricesAtDates(_code: string, _dates: Date[]): Map<string, number> {
    return MockMarketDataReader.priceMap;
  }

  getStockByCode(_code: string) {
    return MockMarketDataReader.stockInfo;
  }

  getStockData(_code: string) {
    return MockMarketDataReader.stockData;
  }

  close() {
    MockMarketDataReader.closeCalled = true;
  }
}

// Passthrough re-exports for modules that other test files also import.
// mock.module is global in Bun, so we must preserve all original exports.
const origMarketSync = await import('@trading25/shared/market-sync');
mock.module('@trading25/shared/market-sync', () => ({
  ...origMarketSync,
  MarketDataReader: MockMarketDataReader,
}));

const origDatasetPaths = await import('@trading25/shared/utils/dataset-paths');
mock.module('@trading25/shared/utils/dataset-paths', () => ({
  ...origDatasetPaths,
  getMarketDbPath: mockGetMarketDbPath,
}));

mock.module('../utils/jquants-client-factory', () => ({
  createJQuantsClient: () => ({ getStatements: mockGetStatements }),
}));

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function assertDefined<T>(value: T | undefined | null, msg = 'Expected value to be defined'): asserts value is T {
  if (value == null) throw new Error(msg);
}

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

function createMockStatement(overrides: Partial<JQuantsStatement> = {}): JQuantsStatement {
  return {
    DiscDate: '2024-05-10',
    DiscTime: '15:00:00',
    Code: '7203',
    DiscNo: '1',
    DocType: '連結',
    CurPerType: 'FY',
    CurPerSt: '2023-04-01',
    CurPerEn: '2024-03-31',
    CurFYSt: '2023-04-01',
    CurFYEn: '2024-03-31',
    NxtFYSt: null,
    NxtFYEn: null,
    Sales: null,
    OP: null,
    OdP: null,
    NP: null,
    EPS: null,
    DEPS: null,
    TA: null,
    Eq: null,
    EqAR: null,
    BPS: null,
    CFO: null,
    CFI: null,
    CFF: null,
    CashEq: null,
    Div1Q: null,
    Div2Q: null,
    Div3Q: null,
    DivFY: null,
    DivAnn: null,
    DivUnit: null,
    DivTotalAnn: null,
    PayoutRatioAnn: null,
    FDiv1Q: null,
    FDiv2Q: null,
    FDiv3Q: null,
    FDivFY: null,
    FDivAnn: null,
    FDivUnit: null,
    FDivTotalAnn: null,
    FPayoutRatioAnn: null,
    NxFDiv1Q: null,
    NxFDiv2Q: null,
    NxFDiv3Q: null,
    NxFDivFY: null,
    NxFDivAnn: null,
    NxFDivUnit: null,
    NxFPayoutRatioAnn: null,
    FSales2Q: null,
    FOP2Q: null,
    FOdP2Q: null,
    FNP2Q: null,
    FEPS2Q: null,
    NxFSales2Q: null,
    NxFOP2Q: null,
    NxFOdP2Q: null,
    NxFNp2Q: null,
    NxFEPS2Q: null,
    FSales: null,
    FOP: null,
    FOdP: null,
    FNP: null,
    FEPS: null,
    NxFSales: null,
    NxFOP: null,
    NxFOdP: null,
    NxFNp: null,
    NxFEPS: null,
    MatChgSub: null,
    ChgByASRev: null,
    ChgNoASRev: null,
    ChgAcEst: null,
    RetroRst: null,
    ShOutFY: null,
    TrShFY: null,
    AvgSh: null,
    NCSales: null,
    NCOP: null,
    NCOdP: null,
    NCNP: null,
    NCEPS: null,
    NCTA: null,
    NCEq: null,
    NCEqAR: null,
    NCBPS: null,
    FNCSales2Q: null,
    FNCOP2Q: null,
    FNCOdP2Q: null,
    FNCNP2Q: null,
    FNCEPS2Q: null,
    NxFNCSales2Q: null,
    NxFNCOP2Q: null,
    NxFNCOdP2Q: null,
    NxFNCNP2Q: null,
    NxFNCEPS2Q: null,
    FNCSales: null,
    FNCOP: null,
    FNCOdP: null,
    FNCNP: null,
    FNCEPS: null,
    NxFNCSales: null,
    NxFNCOP: null,
    NxFNCOdP: null,
    NxFNCNP: null,
    NxFNCEPS: null,
    ...overrides,
  };
}

// Pre-defined fixtures
const fyStatement2024 = createMockStatement({
  DiscDate: '2024-05-10',
  Code: '7203',
  CurPerType: 'FY',
  CurPerSt: '2023-04-01',
  CurPerEn: '2024-03-31',
  CurFYSt: '2023-04-01',
  CurFYEn: '2024-03-31',
  DocType: '連結',
  Sales: 40_000_000_000_000,
  OP: 5_000_000_000_000,
  NP: 3_000_000_000_000,
  EPS: 250.5,
  DEPS: 248.0,
  BPS: 3500.0,
  Eq: 25_000_000_000_000,
  TA: 50_000_000_000_000,
  CFO: 4_000_000_000_000,
  CFI: -2_000_000_000_000,
  CFF: -1_000_000_000_000,
  CashEq: 5_000_000_000_000,
  ShOutFY: 15_000_000_000,
  TrShFY: 1_000_000_000,
  NxFEPS: 280.0,
  FEPS: 270.0,
});

const fyStatement2023 = createMockStatement({
  DiscDate: '2023-05-12',
  Code: '7203',
  CurPerType: 'FY',
  CurPerSt: '2022-04-01',
  CurPerEn: '2023-03-31',
  CurFYSt: '2022-04-01',
  CurFYEn: '2023-03-31',
  DocType: '連結',
  Sales: 35_000_000_000_000,
  OP: 4_000_000_000_000,
  NP: 2_500_000_000_000,
  EPS: 200.0,
  BPS: 3200.0,
  Eq: 22_000_000_000_000,
  TA: 45_000_000_000_000,
  CFO: 3_500_000_000_000,
  CFI: -1_800_000_000_000,
  CFF: -900_000_000_000,
  CashEq: 4_500_000_000_000,
  ShOutFY: 15_000_000_000,
  TrShFY: 1_000_000_000,
  NxFEPS: 250.0,
});

const q1Statement = createMockStatement({
  DiscDate: '2024-08-01',
  Code: '7203',
  CurPerType: 'Q1',
  CurPerSt: '2024-04-01',
  CurPerEn: '2024-06-30',
  CurFYSt: '2024-04-01',
  CurFYEn: '2025-03-31',
  DocType: '連結',
  Sales: 10_000_000_000_000,
  OP: 1_200_000_000_000,
  NP: 800_000_000_000,
  EPS: 60.0,
  BPS: 3600.0,
  Eq: 26_000_000_000_000,
  TA: 51_000_000_000_000,
  FEPS: 290.0,
  NxFEPS: 300.0,
});

const q3Statement = createMockStatement({
  DiscDate: '2025-02-05',
  Code: '7203',
  CurPerType: 'Q3',
  CurPerSt: '2024-04-01',
  CurPerEn: '2024-12-31',
  CurFYSt: '2024-04-01',
  CurFYEn: '2025-03-31',
  DocType: '連結',
  Sales: 30_000_000_000_000,
  OP: 3_800_000_000_000,
  NP: 2_400_000_000_000,
  EPS: 180.0,
  BPS: 3700.0,
  Eq: 27_000_000_000_000,
  TA: 52_000_000_000_000,
  FEPS: 310.0,
  NxFEPS: 320.0,
});

// ---------------------------------------------------------------------------
// Dynamic import with cache busting
// ---------------------------------------------------------------------------

type FundamentalsDataServiceType = InstanceType<typeof import('./fundamentals-data').FundamentalsDataService>;

let service: FundamentalsDataServiceType;

const defaultOptions = {
  symbol: '7203',
  periodType: 'all' as const,
  preferConsolidated: true,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('FundamentalsDataService', () => {
  beforeEach(async () => {
    MockMarketDataReader.priceMap = new Map();
    MockMarketDataReader.stockInfo = null;
    MockMarketDataReader.stockData = [];
    MockMarketDataReader.closeCalled = false;
    mockGetStatements.mockClear();
    mockGetStatements.mockImplementation(() => Promise.resolve({ data: [] }));

    const moduleUrl = new URL('./fundamentals-data.ts', import.meta.url);
    moduleUrl.searchParams.set('t', Date.now().toString());
    const mod = await import(moduleUrl.href);
    service = new mod.FundamentalsDataService();
  });

  // =========================================================================
  // getFundamentals - basic
  // =========================================================================
  describe('getFundamentals - basic', () => {
    it('returns empty data for empty statements', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.symbol).toBe('7203');
      expect(result.data).toEqual([]);
      expect(result.lastUpdated).toBeDefined();
    });

    it('calculates metrics for a single FY statement', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 2500);

      const result = await service.getFundamentals(defaultOptions);

      expect(result.data).toHaveLength(1);
      const dp = getFirstElementOrFail(result.data, 'Expected data point');
      expect(dp.date).toBe('2024-03-31');
      expect(dp.disclosedDate).toBe('2024-05-10');
      expect(dp.periodType).toBe('FY');
      expect(dp.eps).toBe(250.5);
      expect(dp.bps).toBe(3500.0);
      expect(dp.stockPrice).toBe(2500);
      expect(dp.per).toBeCloseTo(9.98, 1);
      expect(dp.pbr).toBeCloseTo(0.71, 1);
      expect(dp.roe).toBeCloseTo(12.0, 0);
    });

    it('sorts data by date descending', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2023, fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.data.length).toBeGreaterThanOrEqual(2);
      const dates = result.data.map((d) => d.date);
      for (let i = 1; i < dates.length; i++) {
        const prev = dates[i - 1];
        const curr = dates[i];
        assertDefined(prev);
        assertDefined(curr);
        expect(prev >= curr).toBe(true);
      }
    });

    it('gets companyName from MarketDataReader', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      MockMarketDataReader.stockInfo = { companyName: 'トヨタ自動車' };

      const result = await service.getFundamentals(defaultOptions);

      expect(result.companyName).toBe('トヨタ自動車');
    });
  });

  // =========================================================================
  // getFundamentals - filtering
  // =========================================================================
  describe('getFundamentals - filtering', () => {
    it('filters by periodType=FY excluding Q', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024, q1Statement] }));

      const result = await service.getFundamentals({ ...defaultOptions, periodType: 'FY' });

      expect(result.data.every((d) => d.periodType === 'FY')).toBe(true);
    });

    it('filters by from/to date range on CurPerEn', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2023, fyStatement2024] }));

      const result = await service.getFundamentals({
        ...defaultOptions,
        from: '2024-01-01',
        to: '2024-12-31',
      });

      expect(result.data).toHaveLength(1);
      const dp = getFirstElementOrFail(result.data, 'Expected filtered data point');
      expect(dp.date).toBe('2024-03-31');
    });

    it('from/to boundary is inclusive', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals({
        ...defaultOptions,
        from: '2024-03-31',
        to: '2024-03-31',
      });

      expect(result.data).toHaveLength(1);
    });

    it('returns dailyValuation even when filter produces 0 data', async () => {
      MockMarketDataReader.stockData = [{ date: new Date('2024-06-03'), close: 2600 }];
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals({
        ...defaultOptions,
        from: '2099-01-01',
        to: '2099-12-31',
      });

      expect(result.data).toEqual([]);
      expect(result.dailyValuation).toBeDefined();
    });
  });

  // =========================================================================
  // metrics calculation
  // =========================================================================
  describe('metrics calculation', () => {
    it('calculates EPS and BPS', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.eps).toBe(250.5);
      expect(dp.bps).toBe(3500.0);
    });

    it('calculates PER = stockPrice / EPS', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 3000);

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.per).toBeCloseTo(3000 / 250.5, 1);
    });

    it('calculates PBR = stockPrice / BPS', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 3500);

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.pbr).toBe(1.0);
    });

    it('converts financial amounts to millions of JPY', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.netProfit).toBe(3_000_000_000_000 / 1_000_000);
      expect(dp.equity).toBe(25_000_000_000_000 / 1_000_000);
      expect(dp.totalAssets).toBe(50_000_000_000_000 / 1_000_000);
      expect(dp.netSales).toBe(40_000_000_000_000 / 1_000_000);
      expect(dp.operatingProfit).toBe(5_000_000_000_000 / 1_000_000);
    });

    it('calculates FCF = CFO + CFI in millions', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.fcf).toBe((4_000_000_000_000 + -2_000_000_000_000) / 1_000_000);
    });

    it('calculates FCFYield', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 2500);

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.fcfYield).toBeCloseTo(5.71, 1);
    });

    it('calculates FCFMargin', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.fcfMargin).toBe(5.0);
    });

    it('returns null PER/PBR when stockPrice is unavailable', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.stockPrice).toBeNull();
      expect(dp.per).toBeNull();
      expect(dp.pbr).toBeNull();
    });
  });

  // =========================================================================
  // dailyValuation
  // =========================================================================
  describe('dailyValuation', () => {
    it('generates daily PER/PBR time series', async () => {
      MockMarketDataReader.stockData = [
        { date: new Date('2024-06-03'), close: 2600 },
        { date: new Date('2024-06-04'), close: 2650 },
      ];
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      assertDefined(result.dailyValuation, 'Expected dailyValuation');
      expect(result.dailyValuation.length).toBe(2);

      const first = getFirstElementOrFail(result.dailyValuation, 'Expected first valuation');
      expect(first.date).toBe('2024-06-03');
      expect(first.close).toBe(2600);
      expect(first.per).toBeCloseTo(2600 / 250.5, 1);
      expect(first.pbr).toBeCloseTo(2600 / 3500, 1);
    });

    it('applies the most recent disclosed FY to each trading day', async () => {
      MockMarketDataReader.stockData = [
        { date: new Date('2024-01-15'), close: 2400 },
        { date: new Date('2024-06-01'), close: 2600 },
      ];
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2023, fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      assertDefined(result.dailyValuation, 'Expected dailyValuation');
      const dv = result.dailyValuation;

      const jan = dv.find((d) => d.date === '2024-01-15');
      assertDefined(jan, 'Expected Jan valuation');
      expect(jan.per).toBeCloseTo(2400 / 200, 1);

      const jun = dv.find((d) => d.date === '2024-06-01');
      assertDefined(jun, 'Expected Jun valuation');
      expect(jun.per).toBeCloseTo(2600 / 250.5, 1);
    });

    it('returns empty dailyValuation when no stock prices', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.dailyValuation).toBeUndefined();
    });

    it('returns empty dailyValuation when no FY data', async () => {
      MockMarketDataReader.stockData = [{ date: new Date('2024-06-03'), close: 2600 }];
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [q1Statement] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.dailyValuation).toBeUndefined();
    });
  });

  // =========================================================================
  // forecast & enhancement
  // =========================================================================
  describe('forecast & enhancement', () => {
    it('FY: NxFEPS priority, FEPS fallback', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEps).toBe(280.0);
    });

    it('FY: falls back to FEPS when NxFEPS is null', async () => {
      const stmt = createMockStatement({
        ...fyStatement2024,
        NxFEPS: null,
        FEPS: 270.0,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [stmt] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEps).toBe(270.0);
    });

    it('Q: FEPS priority, NxFEPS fallback', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [q1Statement] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEps).toBe(290.0);
    });

    it('non-consolidated: uses NxFNCEPS/FNCEPS', async () => {
      const ncStmt = createMockStatement({
        ...fyStatement2024,
        DocType: '非連結',
        NxFNCEPS: 150.0,
        FNCEPS: 140.0,
        NCEPS: 120.0,
        NCNP: 1_000_000_000_000,
        NCEq: 10_000_000_000_000,
        NCTA: 20_000_000_000_000,
        NCSales: 15_000_000_000_000,
        NCOP: 2_000_000_000_000,
        NCBPS: 2000.0,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [ncStmt] }));

      const result = await service.getFundamentals({
        ...defaultOptions,
        preferConsolidated: false,
      });
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEps).toBe(150.0);
    });

    it('calculates forecastEpsChangeRate correctly', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEpsChangeRate).toBeCloseTo(((280 - 250.5) / 250.5) * 100, 1);
    });

    it('returns null forecastEpsChangeRate when actualEps is 0', async () => {
      const stmt = createMockStatement({
        ...fyStatement2024,
        EPS: 0,
        NxFEPS: 100.0,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [stmt] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEpsChangeRate).toBeNull();
    });

    it('returns null forecastEpsChangeRate when actualEps is null', async () => {
      const stmt = createMockStatement({
        ...fyStatement2024,
        EPS: null,
        NxFEPS: 100.0,
        NP: 1_000_000_000,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [stmt] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.forecastEpsChangeRate).toBeNull();
    });

    it('attaches previous period CF data (±45 day tolerance)', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024, fyStatement2023] }));

      const result = await service.getFundamentals(defaultOptions);

      assertDefined(result.latestMetrics, 'Expected latestMetrics');
      expect(result.latestMetrics.prevCashFlowOperating).toBe(3_500_000_000_000 / 1_000_000);
      expect(result.latestMetrics.prevCashFlowInvesting).toBe(-1_800_000_000_000 / 1_000_000);
      expect(result.latestMetrics.prevCashFlowFinancing).toBe(-900_000_000_000 / 1_000_000);
      expect(result.latestMetrics.prevCashAndEquivalents).toBe(4_500_000_000_000 / 1_000_000);
    });

    it('returns null prev CF fields when no previous period exists', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      assertDefined(result.latestMetrics, 'Expected latestMetrics');
      expect(result.latestMetrics.prevCashFlowOperating).toBeNull();
      expect(result.latestMetrics.prevCashFlowInvesting).toBeNull();
    });
  });

  // =========================================================================
  // revision annotation
  // =========================================================================
  describe('revision annotation', () => {
    it('sets revisedForecastEps when Q forecast differs from FY', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024, q3Statement] }));

      const result = await service.getFundamentals(defaultOptions);
      const fy = result.data.find((d) => d.periodType === 'FY');

      assertDefined(fy, 'Expected FY data point');
      expect(fy.revisedForecastEps).toBe(310.0);
      expect(fy.revisedForecastSource).toBe('Q3');
    });

    it('does not annotate when Q is disclosed before FY', async () => {
      const earlyQ = createMockStatement({
        ...q3Statement,
        DiscDate: '2024-01-15',
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024, earlyQ] }));

      const result = await service.getFundamentals(defaultOptions);
      const fy = result.data.find((d) => d.periodType === 'FY');

      assertDefined(fy, 'Expected FY data point');
      expect(fy.revisedForecastEps).toBeNull();
    });

    it('does not annotate when Q FEPS equals FY NxFEPS', async () => {
      const matchingQ = createMockStatement({
        ...q3Statement,
        FEPS: 280.0,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024, matchingQ] }));

      const result = await service.getFundamentals(defaultOptions);
      const fy = result.data.find((d) => d.periodType === 'FY');

      assertDefined(fy, 'Expected FY data point');
      expect(fy.revisedForecastEps).toBeNull();
    });

    it('does not annotate when no Q statements exist', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);
      const fy = result.data.find((d) => d.periodType === 'FY');

      assertDefined(fy, 'Expected FY data point');
      expect(fy.revisedForecastEps).toBeNull();
    });
  });

  // =========================================================================
  // edge cases
  // =========================================================================
  describe('edge cases', () => {
    it('handles all-null financial data returning null metrics', async () => {
      const nullStmt = createMockStatement();
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [nullStmt] }));

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.eps).toBeNull();
      expect(dp.bps).toBeNull();
      expect(dp.roe).toBeNull();
      expect(dp.roa).toBeNull();
      expect(dp.per).toBeNull();
      expect(dp.pbr).toBeNull();
      expect(dp.operatingMargin).toBeNull();
      expect(dp.netMargin).toBeNull();
      expect(dp.fcf).toBeNull();
    });

    it('handles single statement with no previous period CF', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.data).toHaveLength(1);
      assertDefined(result.latestMetrics, 'Expected latestMetrics');
      expect(result.latestMetrics.prevCashFlowOperating).toBeNull();
    });

    it('rounds numeric values to 2 decimal places', async () => {
      const stmt = createMockStatement({
        EPS: 123.456789,
        BPS: 987.654321,
        NP: 1_234_567_890_123,
        Eq: 10_000_000_000_000,
        TA: 20_000_000_000_000,
        Sales: 30_000_000_000_000,
        OP: 5_000_000_000_000,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [stmt] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 1500);

      const result = await service.getFundamentals(defaultOptions);
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.eps).toBe(Math.round(123.456789 * 100) / 100);
      expect(dp.bps).toBe(Math.round(987.654321 * 100) / 100);
    });

    it('uses NC* fields when preferConsolidated=false', async () => {
      const ncStmt = createMockStatement({
        DocType: '非連結',
        EPS: 999,
        BPS: 999,
        NP: 999,
        Eq: 999,
        NCEPS: 120.0,
        NCBPS: 2000.0,
        NCNP: 1_000_000_000_000,
        NCEq: 10_000_000_000_000,
        NCTA: 20_000_000_000_000,
        NCSales: 15_000_000_000_000,
        NCOP: 2_000_000_000_000,
      });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [ncStmt] }));
      MockMarketDataReader.priceMap.set('2024-05-10', 2400);

      const result = await service.getFundamentals({
        ...defaultOptions,
        preferConsolidated: false,
      });
      const dp = getFirstElementOrFail(result.data, 'Expected data');

      expect(dp.eps).toBe(120.0);
      expect(dp.bps).toBe(2000.0);
      expect(dp.per).toBe(20.0);
      expect(dp.pbr).toBe(1.2);
    });

    it('handles duplicate statements for same date', async () => {
      const dup1 = createMockStatement({ ...fyStatement2024, DiscNo: '1' });
      const dup2 = createMockStatement({ ...fyStatement2024, DiscNo: '2' });
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [dup1, dup2] }));

      const result = await service.getFundamentals(defaultOptions);

      expect(result.data).toHaveLength(2);
    });
  });

  // =========================================================================
  // cleanup
  // =========================================================================
  describe('cleanup', () => {
    it('close() calls MarketDataReader.close()', async () => {
      mockGetStatements.mockImplementation(() => Promise.resolve({ data: [fyStatement2024] }));
      await service.getFundamentals(defaultOptions);

      service.close();

      expect(MockMarketDataReader.closeCalled).toBe(true);
    });
  });
});
