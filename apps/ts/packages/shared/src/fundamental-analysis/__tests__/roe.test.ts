import { describe, expect, it } from 'bun:test';
import type { JQuantsStatement } from '../../types/jquants';
import { calculateROE, calculateROEBatch, canCalculateROE, formatROEResult } from '../roe';
import { ROECalculationError } from '../types';

describe('ROE Calculation', () => {
  const createMockStatement = (overrides: Partial<JQuantsStatement> = {}): JQuantsStatement => ({
    DiscDate: '2023-05-15',
    DiscTime: '15:30:00',
    Code: '7203',
    DiscNo: '20230515401234',
    DocType: 'FYFinancialStatements_Consolidated_JP',
    CurPerType: 'FY',
    CurPerSt: '2022-04-01',
    CurPerEn: '2023-03-31',
    CurFYSt: '2022-04-01',
    CurFYEn: '2023-03-31',
    NxtFYSt: '2023-04-01',
    NxtFYEn: '2024-03-31',
    Sales: 37500000, // 3.75 trillion yen
    OP: 2000000, // 200 billion yen
    OdP: 1900000,
    NP: 1500000, // 150 billion yen
    EPS: 500.0,
    DEPS: 495.0,
    TA: 50000000, // 5 trillion yen
    Eq: 15000000, // 1.5 trillion yen
    EqAR: 30.0,
    BPS: 5000.0,
    CFO: 2500000,
    CFI: -800000,
    CFF: -600000,
    CashEq: 3000000,
    Div1Q: 0,
    Div2Q: 80.0,
    Div3Q: 0,
    DivFY: 90.0,
    DivAnn: 170.0,
    DivUnit: null,
    DivTotalAnn: 510000000,
    PayoutRatioAnn: 34.0,
    FDiv1Q: 0,
    FDiv2Q: 85.0,
    FDiv3Q: 0,
    FDivFY: 95.0,
    FDivAnn: 180.0,
    FDivUnit: null,
    FDivTotalAnn: 540000000,
    FPayoutRatioAnn: 32.0,
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
    FSales: 39000000,
    FOP: 2100000,
    FOdP: 2000000,
    FNP: 1600000,
    FEPS: 533.3,
    NxFSales: null,
    NxFOP: null,
    NxFOdP: null,
    NxFNp: null,
    NxFEPS: null,
    MatChgSub: false,
    ChgByASRev: false,
    ChgNoASRev: false,
    ChgAcEst: false,
    RetroRst: false,
    ShOutFY: 3000000000,
    TrShFY: 0,
    AvgSh: 3000000000,
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
  });

  describe('calculateROE', () => {
    it('should calculate ROE correctly for valid data', () => {
      const statement = createMockStatement();
      const result = calculateROE(statement);

      // ROE = (1,500,000 / 15,000,000) * 100 = 10%
      expect(result.roe).toBe(10);
      expect(result.netProfit).toBe(1500000);
      expect(result.equity).toBe(15000000);
      expect(result.metadata.code).toBe('7203');
      expect(result.metadata.periodType).toBe('FY');
      expect(result.metadata.isConsolidated).toBe(true);
      expect(result.metadata.accountingStandard).toBe('JGAAP');
      expect(result.metadata.isAnnualized).toBe(false);
    });

    it('should handle quarterly data with annualization', () => {
      const statement = createMockStatement({
        CurPerType: '1Q',
        NP: 375000, // 1Q profit
      });

      const result = calculateROE(statement);

      // 1Q profit annualized: 375,000 * 4 = 1,500,000
      // ROE = (1,500,000 / 15,000,000) * 100 = 10%
      expect(result.roe).toBe(10);
      expect(result.netProfit).toBe(1500000); // Annualized
      expect(result.metadata.isAnnualized).toBe(true);
    });

    it('should handle 2Q (half-year) data correctly', () => {
      const statement = createMockStatement({
        CurPerType: '2Q',
        NP: 750000, // H1 profit
      });

      const result = calculateROE(statement);

      // H1 profit annualized: 750,000 * 2 = 1,500,000
      expect(result.roe).toBe(10);
      expect(result.netProfit).toBe(1500000); // Annualized
    });

    it('should handle 3Q data correctly', () => {
      const statement = createMockStatement({
        CurPerType: '3Q',
        NP: 1125000, // 9-month profit
      });

      const result = calculateROE(statement);

      // 3Q profit annualized: 1,125,000 * (4/3) = 1,500,000
      expect(result.roe).toBe(10);
      expect(result.netProfit).toBe(1500000); // Annualized
    });

    it('should not annualize quarterly data when annualize option is false', () => {
      const statement = createMockStatement({
        CurPerType: '1Q',
        NP: 375000,
      });

      const result = calculateROE(statement, { annualize: false });

      // No annualization: 375,000 / 15,000,000 * 100 = 2.5%
      expect(result.roe).toBe(2.5);
      expect(result.netProfit).toBe(375000); // Not annualized
      expect(result.metadata.isAnnualized).toBe(false);
    });

    it('should fallback to non-consolidated data when consolidated is not available', () => {
      const statement = createMockStatement({
        NP: null,
        Eq: null,
        NCNP: 800000,
        NCEq: 10000000,
      });

      const result = calculateROE(statement);

      // ROE = (800,000 / 10,000,000) * 100 = 8%
      expect(result.roe).toBe(8);
      expect(result.netProfit).toBe(800000);
      expect(result.equity).toBe(10000000);
    });

    it('should prefer non-consolidated data when preferConsolidated is false', () => {
      const statement = createMockStatement({
        NP: 1500000,
        Eq: 15000000,
        NCNP: 800000,
        NCEq: 10000000,
      });

      const result = calculateROE(statement, { preferConsolidated: false });

      // Should use non-consolidated data: 800,000 / 10,000,000 * 100 = 8%
      expect(result.roe).toBe(8);
      expect(result.netProfit).toBe(800000);
      expect(result.equity).toBe(10000000);
    });

    it('should throw error when profit is not available', () => {
      const statement = createMockStatement({
        NP: null,
        NCNP: null,
      });

      expect(() => calculateROE(statement)).toThrow(ROECalculationError);
      expect(() => calculateROE(statement)).toThrow('Net profit data is not available');
    });

    it('should throw error when equity is not available', () => {
      const statement = createMockStatement({
        Eq: null,
        NCEq: null,
      });

      expect(() => calculateROE(statement)).toThrow(ROECalculationError);
      expect(() => calculateROE(statement)).toThrow('Equity data is not available');
    });

    it('should throw error when equity is zero', () => {
      const statement = createMockStatement({
        Eq: 0,
      });

      expect(() => calculateROE(statement)).toThrow(ROECalculationError);
      expect(() => calculateROE(statement)).toThrow('below minimum threshold');
    });

    it('should throw error when equity is negative', () => {
      const statement = createMockStatement({
        Eq: -1000000,
      });

      expect(() => calculateROE(statement)).toThrow(ROECalculationError);
      expect(() => calculateROE(statement)).toThrow('Invalid equity value: -1000000');
    });

    it('should throw error when equity is below minimum threshold', () => {
      const statement = createMockStatement({
        Eq: 500, // Below default threshold of 1000
      });

      expect(() => calculateROE(statement)).toThrow(ROECalculationError);
      expect(() => calculateROE(statement)).toThrow('below minimum threshold');
    });

    it('should respect custom minimum equity threshold', () => {
      const statement = createMockStatement({
        Eq: 500,
      });

      // Should work with lower threshold
      const result = calculateROE(statement, { minEquityThreshold: 100 });
      expect(result.roe).toBe(300000); // 1,500,000 / 500 * 100
    });

    it('should detect IFRS accounting standard', () => {
      const statement = createMockStatement({
        DocType: 'FYFinancialStatements_Consolidated_IFRS',
      });

      const result = calculateROE(statement);
      expect(result.metadata.accountingStandard).toBe('IFRS');
    });

    it('should detect US GAAP accounting standard', () => {
      const statement = createMockStatement({
        DocType: 'FYFinancialStatements_Consolidated_US_GAAP',
      });

      const result = calculateROE(statement);
      expect(result.metadata.accountingStandard).toBe('US GAAP');
    });
  });

  describe('calculateROEBatch', () => {
    it('should calculate ROE for multiple statements', () => {
      const statements = [
        createMockStatement({ Code: '7203', NP: 1500000, Eq: 15000000 }), // 10%
        createMockStatement({ Code: '8411', NP: 800000, Eq: 10000000 }), // 8%
        createMockStatement({ Code: '9984', NP: 2000000, Eq: 10000000 }), // 20%
      ];

      const results = calculateROEBatch(statements);

      expect(results).toHaveLength(3);
      // Results should be sorted by ROE descending
      expect(results[0]?.metadata.code).toBe('9984'); // 20%
      expect(results[1]?.metadata.code).toBe('7203'); // 10%
      expect(results[2]?.metadata.code).toBe('8411'); // 8%
    });

    it('should prefer FY statements over quarterly', () => {
      const statements = [
        createMockStatement({
          Code: '7203',
          CurPerType: '1Q',
          CurPerEn: '2023-06-30',
          NP: 375000,
        }),
        createMockStatement({
          Code: '7203',
          CurPerType: 'FY',
          CurPerEn: '2023-03-31',
          NP: 1500000,
        }),
      ];

      const results = calculateROEBatch(statements);

      expect(results).toHaveLength(1);
      expect(results[0]?.metadata.periodType).toBe('FY');
    });

    it('should prefer more recent statements of same type', () => {
      const statements = [
        createMockStatement({
          Code: '7203',
          CurPerEn: '2022-03-31',
          NP: 1000000,
        }),
        createMockStatement({
          Code: '7203',
          CurPerEn: '2023-03-31',
          NP: 1500000,
        }),
      ];

      const results = calculateROEBatch(statements);

      expect(results).toHaveLength(1);
      expect(results[0]?.metadata.periodEnd).toBe('2023-03-31');
    });

    it('should skip statements with invalid data', () => {
      const statements = [
        createMockStatement({ Code: '7203' }), // Valid
        createMockStatement({ Code: '8411', NP: null }), // Invalid - no profit
        createMockStatement({ Code: '9984', Eq: 0 }), // Invalid - zero equity
      ];

      const results = calculateROEBatch(statements);

      expect(results).toHaveLength(1);
      expect(results[0]?.metadata.code).toBe('7203');
    });

    it('should return empty array for empty input', () => {
      const results = calculateROEBatch([]);
      expect(results).toEqual([]);
    });
  });

  describe('canCalculateROE', () => {
    it('should return true for valid statement', () => {
      const statement = createMockStatement();
      expect(canCalculateROE(statement)).toBe(true);
    });

    it('should return false when profit is missing', () => {
      const statement = createMockStatement({
        NP: null,
        NCNP: null,
      });
      expect(canCalculateROE(statement)).toBe(false);
    });

    it('should return false when equity is missing', () => {
      const statement = createMockStatement({
        Eq: null,
        NCEq: null,
      });
      expect(canCalculateROE(statement)).toBe(false);
    });

    it('should return false when equity is zero or negative', () => {
      const statement1 = createMockStatement({ Eq: 0 });
      const statement2 = createMockStatement({ Eq: -1000000 });

      expect(canCalculateROE(statement1)).toBe(false);
      expect(canCalculateROE(statement2)).toBe(false);
    });
  });

  describe('formatROEResult', () => {
    it('should format ROE result correctly', () => {
      const statement = createMockStatement();
      const result = calculateROE(statement);

      const formatted = formatROEResult(result);
      expect(formatted).toBe('7203: ROE 10.00% (FY 2023-03-31)');
    });

    it('should include annualized note for quarterly data', () => {
      const statement = createMockStatement({
        CurPerType: '1Q',
        NP: 375000,
      });
      const result = calculateROE(statement);

      const formatted = formatROEResult(result);
      expect(formatted).toBe('7203: ROE 10.00% (annualized) (1Q 2023-03-31)');
    });
  });
});
