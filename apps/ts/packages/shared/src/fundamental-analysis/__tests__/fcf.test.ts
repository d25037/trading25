/**
 * FCF (Free Cash Flow) Calculation Tests
 */

import { describe, expect, test } from 'bun:test';
import type { JQuantsStatement } from '../../types/jquants';
import {
  calculateFCFMargin,
  calculateFCFYield,
  calculateSimpleFCF,
  getCashAndEquivalents,
  getCashFlowFinancing,
  getCashFlowInvesting,
  getCashFlowOperating,
  getSharesOutstanding,
  getTreasuryShares,
} from '../fcf';

// Helper to create minimal statement for testing
function createStatement(overrides: Partial<JQuantsStatement> = {}): JQuantsStatement {
  return {
    DiscDate: '2024-05-10',
    DiscTime: '15:00:00',
    Code: '7203',
    DiscNo: '1',
    DocType: 'Annual',
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

describe('calculateSimpleFCF', () => {
  test('calculates FCF correctly (positive CFO, negative CFI)', () => {
    const result = calculateSimpleFCF(500000, -200000);
    expect(result).toBe(300000);
  });

  test('calculates FCF correctly (positive CFO, positive CFI)', () => {
    const result = calculateSimpleFCF(500000, 100000);
    expect(result).toBe(600000);
  });

  test('calculates negative FCF when investments exceed operating cash flow', () => {
    const result = calculateSimpleFCF(200000, -500000);
    expect(result).toBe(-300000);
  });

  test('returns null when CFO is null', () => {
    const result = calculateSimpleFCF(null, -200000);
    expect(result).toBeNull();
  });

  test('returns null when CFI is null', () => {
    const result = calculateSimpleFCF(500000, null);
    expect(result).toBeNull();
  });

  test('returns null when both are null', () => {
    const result = calculateSimpleFCF(null, null);
    expect(result).toBeNull();
  });

  test('handles zero values', () => {
    expect(calculateSimpleFCF(0, 0)).toBe(0);
    expect(calculateSimpleFCF(0, -100000)).toBe(-100000);
    expect(calculateSimpleFCF(100000, 0)).toBe(100000);
  });
});

describe('calculateFCFYield', () => {
  test('calculates FCF Yield correctly', () => {
    // FCF = 300000, stockPrice = 2500, shares = 100000000, treasury = 5000000
    // Market Cap = 2500 * (100000000 - 5000000) = 2500 * 95000000 = 237500000000
    // FCF Yield = (300000 / 237500000000) * 100 = 0.0001263...%
    const result = calculateFCFYield(300000, 2500, 100000000, 5000000);
    expect(result).not.toBeNull();
    expect(result).toBeCloseTo(0.0001263, 6);
  });

  test('calculates FCF Yield without treasury shares', () => {
    // FCF = 300000, stockPrice = 2500, shares = 100000
    // Market Cap = 2500 * 100000 = 250000000
    // FCF Yield = (300000 / 250000000) * 100 = 0.12%
    const result = calculateFCFYield(300000, 2500, 100000);
    expect(result).not.toBeNull();
    expect(result).toBeCloseTo(0.12, 2);
  });

  test('handles negative FCF (results in negative yield)', () => {
    const result = calculateFCFYield(-300000, 2500, 100000);
    expect(result).not.toBeNull();
    expect(result).toBeLessThan(0);
  });

  test('returns null when FCF is null', () => {
    const result = calculateFCFYield(null, 2500, 100000);
    expect(result).toBeNull();
  });

  test('returns null when stockPrice is null', () => {
    const result = calculateFCFYield(300000, null, 100000);
    expect(result).toBeNull();
  });

  test('returns null when stockPrice is zero or negative', () => {
    expect(calculateFCFYield(300000, 0, 100000)).toBeNull();
    expect(calculateFCFYield(300000, -100, 100000)).toBeNull();
  });

  test('returns null when sharesOutstanding is null', () => {
    const result = calculateFCFYield(300000, 2500, null);
    expect(result).toBeNull();
  });

  test('returns null when actualShares is zero or negative', () => {
    // shares = treasury shares -> actualShares = 0
    expect(calculateFCFYield(300000, 2500, 100000, 100000)).toBeNull();
    // treasury > outstanding -> actualShares < 0
    expect(calculateFCFYield(300000, 2500, 100000, 150000)).toBeNull();
  });

  test('handles zero FCF', () => {
    const result = calculateFCFYield(0, 2500, 100000);
    expect(result).toBe(0);
  });
});

describe('calculateFCFMargin', () => {
  test('calculates FCF Margin correctly', () => {
    // FCF = 300000, Sales = 5000000
    // FCF Margin = (300000 / 5000000) * 100 = 6%
    const result = calculateFCFMargin(300000, 5000000);
    expect(result).toBe(6);
  });

  test('handles negative FCF (results in negative margin)', () => {
    const result = calculateFCFMargin(-300000, 5000000);
    expect(result).toBe(-6);
  });

  test('returns null when FCF is null', () => {
    const result = calculateFCFMargin(null, 5000000);
    expect(result).toBeNull();
  });

  test('returns null when sales is null', () => {
    const result = calculateFCFMargin(300000, null);
    expect(result).toBeNull();
  });

  test('returns null when sales is zero or negative', () => {
    expect(calculateFCFMargin(300000, 0)).toBeNull();
    expect(calculateFCFMargin(300000, -1000000)).toBeNull();
  });

  test('handles zero FCF', () => {
    const result = calculateFCFMargin(0, 5000000);
    expect(result).toBe(0);
  });

  test('handles high margin scenario', () => {
    // FCF = 500000, Sales = 500000 -> 100% margin
    const result = calculateFCFMargin(500000, 500000);
    expect(result).toBe(100);
  });
});

describe('getCashFlowOperating', () => {
  test('returns CFO from statement', () => {
    const statement = createStatement({ CFO: 2000000000 });
    expect(getCashFlowOperating(statement)).toBe(2000000000);
  });

  test('returns null when CFO is null', () => {
    const statement = createStatement({ CFO: null });
    expect(getCashFlowOperating(statement)).toBeNull();
  });

  test('returns null when CFO is undefined', () => {
    const statement = createStatement();
    expect(getCashFlowOperating(statement)).toBeNull();
  });
});

describe('getCashFlowInvesting', () => {
  test('returns CFI from statement', () => {
    const statement = createStatement({ CFI: -1000000000 });
    expect(getCashFlowInvesting(statement)).toBe(-1000000000);
  });

  test('returns null when CFI is null', () => {
    const statement = createStatement({ CFI: null });
    expect(getCashFlowInvesting(statement)).toBeNull();
  });
});

describe('getCashFlowFinancing', () => {
  test('returns CFF from statement', () => {
    const statement = createStatement({ CFF: -500000000 });
    expect(getCashFlowFinancing(statement)).toBe(-500000000);
  });

  test('returns null when CFF is null', () => {
    const statement = createStatement({ CFF: null });
    expect(getCashFlowFinancing(statement)).toBeNull();
  });
});

describe('getCashAndEquivalents', () => {
  test('returns CashEq from statement', () => {
    const statement = createStatement({ CashEq: 8000000000 });
    expect(getCashAndEquivalents(statement)).toBe(8000000000);
  });

  test('returns null when CashEq is null', () => {
    const statement = createStatement({ CashEq: null });
    expect(getCashAndEquivalents(statement)).toBeNull();
  });
});

describe('getSharesOutstanding', () => {
  test('returns ShOutFY from statement', () => {
    const statement = createStatement({ ShOutFY: 100000000 });
    expect(getSharesOutstanding(statement)).toBe(100000000);
  });

  test('returns null when ShOutFY is null', () => {
    const statement = createStatement({ ShOutFY: null });
    expect(getSharesOutstanding(statement)).toBeNull();
  });
});

describe('getTreasuryShares', () => {
  test('returns TrShFY from statement', () => {
    const statement = createStatement({ TrShFY: 5000000 });
    expect(getTreasuryShares(statement)).toBe(5000000);
  });

  test('returns null when TrShFY is null', () => {
    const statement = createStatement({ TrShFY: null });
    expect(getTreasuryShares(statement)).toBeNull();
  });
});
