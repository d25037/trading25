/**
 * Tests for fundamental-analysis/utils.ts
 */
import { describe, expect, it } from 'bun:test';
import {
  calculateDailyValuation,
  type FinancialDataInput,
  type FYDataPoint,
  filterValidFYData,
  findApplicableFY,
  hasActualFinancialData,
  hasValidValuationMetrics,
  isFiscalYear,
  isQuarterlyPeriod,
  isValidBps,
  isValidEps,
  normalizePeriodType,
  toNumberOrNull,
} from '../utils';

describe('toNumberOrNull', () => {
  it('returns null for null/undefined/empty string', () => {
    expect(toNumberOrNull(null)).toBeNull();
    expect(toNumberOrNull(undefined)).toBeNull();
    expect(toNumberOrNull('')).toBeNull();
  });

  it('returns number for valid number', () => {
    expect(toNumberOrNull(123)).toBe(123);
    expect(toNumberOrNull(0)).toBe(0);
    expect(toNumberOrNull(-456)).toBe(-456);
    expect(toNumberOrNull(3.14)).toBe(3.14);
  });

  it('returns null for non-finite numbers', () => {
    expect(toNumberOrNull(Number.POSITIVE_INFINITY)).toBeNull();
    expect(toNumberOrNull(Number.NEGATIVE_INFINITY)).toBeNull();
    expect(toNumberOrNull(Number.NaN)).toBeNull();
  });

  it('parses string numbers', () => {
    expect(toNumberOrNull('123')).toBe(123);
    expect(toNumberOrNull('3.14')).toBe(3.14);
    expect(toNumberOrNull('-456')).toBe(-456);
    expect(toNumberOrNull('0')).toBe(0);
  });

  it('returns null for invalid strings', () => {
    expect(toNumberOrNull('abc')).toBeNull();
    expect(toNumberOrNull('12abc')).toBeNull();
  });

  it('returns null for other types', () => {
    expect(toNumberOrNull({})).toBeNull();
    expect(toNumberOrNull([])).toBeNull();
    expect(toNumberOrNull(true)).toBeNull();
  });
});

describe('isFiscalYear', () => {
  it('returns true for FY', () => {
    expect(isFiscalYear('FY')).toBe(true);
  });

  it('returns false for quarters', () => {
    expect(isFiscalYear('1Q')).toBe(false);
    expect(isFiscalYear('2Q')).toBe(false);
    expect(isFiscalYear('3Q')).toBe(false);
  });

  it('returns false for null/undefined', () => {
    expect(isFiscalYear(null)).toBe(false);
    expect(isFiscalYear(undefined)).toBe(false);
  });
});

describe('isQuarterlyPeriod', () => {
  it('returns true for 1Q, 2Q, 3Q', () => {
    expect(isQuarterlyPeriod('1Q')).toBe(true);
    expect(isQuarterlyPeriod('2Q')).toBe(true);
    expect(isQuarterlyPeriod('3Q')).toBe(true);
  });

  it('returns false for FY (Q4 is reported as FY)', () => {
    expect(isQuarterlyPeriod('FY')).toBe(false);
  });

  it('returns false for null/undefined', () => {
    expect(isQuarterlyPeriod(null)).toBe(false);
    expect(isQuarterlyPeriod(undefined)).toBe(false);
  });
});

describe('normalizePeriodType', () => {
  it('normalizes legacy Q1/Q2/Q3 to 1Q/2Q/3Q', () => {
    expect(normalizePeriodType('Q1')).toBe('1Q');
    expect(normalizePeriodType('Q2')).toBe('2Q');
    expect(normalizePeriodType('Q3')).toBe('3Q');
  });

  it('returns FY/all/1Q-3Q as-is', () => {
    expect(normalizePeriodType('FY')).toBe('FY');
    expect(normalizePeriodType('all')).toBe('all');
    expect(normalizePeriodType('1Q')).toBe('1Q');
  });

  it('returns null for null/undefined', () => {
    expect(normalizePeriodType(null)).toBeNull();
    expect(normalizePeriodType(undefined)).toBeNull();
  });
});

describe('isValidEps', () => {
  it('returns true for valid positive EPS', () => {
    expect(isValidEps(100)).toBe(true);
    expect(isValidEps(359.56)).toBe(true);
  });

  it('returns true for negative EPS (loss)', () => {
    expect(isValidEps(-50)).toBe(true);
  });

  it('returns false for zero (forecast indicator)', () => {
    expect(isValidEps(0)).toBe(false);
  });

  it('returns false for null/undefined', () => {
    expect(isValidEps(null)).toBe(false);
    expect(isValidEps(undefined)).toBe(false);
  });
});

describe('isValidBps', () => {
  it('returns true for positive BPS', () => {
    expect(isValidBps(2753.09)).toBe(true);
    expect(isValidBps(100)).toBe(true);
  });

  it('returns false for zero or negative BPS', () => {
    expect(isValidBps(0)).toBe(false);
    expect(isValidBps(-100)).toBe(false);
  });

  it('returns false for null/undefined', () => {
    expect(isValidBps(null)).toBe(false);
    expect(isValidBps(undefined)).toBe(false);
  });
});

describe('hasValidValuationMetrics', () => {
  it('returns true if EPS is valid', () => {
    expect(hasValidValuationMetrics(100, null)).toBe(true);
  });

  it('returns true if BPS is valid', () => {
    expect(hasValidValuationMetrics(null, 2000)).toBe(true);
  });

  it('returns true if both are valid', () => {
    expect(hasValidValuationMetrics(100, 2000)).toBe(true);
  });

  it('returns false if neither is valid', () => {
    expect(hasValidValuationMetrics(null, null)).toBe(false);
    expect(hasValidValuationMetrics(0, 0)).toBe(false);
    expect(hasValidValuationMetrics(0, -100)).toBe(false);
  });
});

describe('hasActualFinancialData', () => {
  it('returns true when ROE is present', () => {
    const data: FinancialDataInput = { roe: 12.5 };
    expect(hasActualFinancialData(data)).toBe(true);
  });

  it('returns true when valid EPS is present', () => {
    const data: FinancialDataInput = { eps: 100 };
    expect(hasActualFinancialData(data)).toBe(true);
  });

  it('returns true when netProfit is present (including zero and negative)', () => {
    expect(hasActualFinancialData({ netProfit: 1000000 })).toBe(true);
    expect(hasActualFinancialData({ netProfit: 0 })).toBe(true);
    expect(hasActualFinancialData({ netProfit: -500000 })).toBe(true); // Loss
  });

  it('returns true when equity is present (including zero and negative)', () => {
    expect(hasActualFinancialData({ equity: 5000000 })).toBe(true);
    expect(hasActualFinancialData({ equity: 0 })).toBe(true);
    expect(hasActualFinancialData({ equity: -1000000 })).toBe(true); // 債務超過
  });

  it('returns false when all fields are null/undefined', () => {
    const data: FinancialDataInput = {
      roe: null,
      eps: null,
      netProfit: null,
      equity: null,
    };
    expect(hasActualFinancialData(data)).toBe(false);
  });

  it('returns false when EPS is zero (forecast indicator) and no other data', () => {
    const data: FinancialDataInput = { eps: 0 };
    expect(hasActualFinancialData(data)).toBe(false);
  });
});

describe('calculateDailyValuation', () => {
  it('calculates PER and PBR correctly', () => {
    const fy: FYDataPoint = { disclosedDate: '2025-05-08', eps: 359.56, bps: 2753.09 };
    const result = calculateDailyValuation(3584, fy);
    expect(result.per).toBe(9.97);
    expect(result.pbr).toBe(1.3);
  });

  it('returns null for null FY data', () => {
    const result = calculateDailyValuation(3584, null);
    expect(result.per).toBeNull();
    expect(result.pbr).toBeNull();
  });

  it('returns null PER for zero EPS', () => {
    const fy: FYDataPoint = { disclosedDate: '2025-05-08', eps: 0, bps: 2753.09 };
    const result = calculateDailyValuation(3584, fy);
    expect(result.per).toBeNull();
    expect(result.pbr).toBe(1.3);
  });

  it('handles negative EPS (calculates negative PER)', () => {
    const fy: FYDataPoint = { disclosedDate: '2025-05-08', eps: -100, bps: 2000 };
    const result = calculateDailyValuation(2000, fy);
    expect(result.per).toBe(-20);
    expect(result.pbr).toBe(1);
  });
});

describe('findApplicableFY', () => {
  const fyDataPoints: FYDataPoint[] = [
    { disclosedDate: '2023-05-10', eps: 200, bps: 2000 },
    { disclosedDate: '2024-05-08', eps: 300, bps: 2500 },
    { disclosedDate: '2025-05-08', eps: 359.56, bps: 2753.09 },
  ];

  it('finds the most recent FY before the date', () => {
    const result = findApplicableFY(fyDataPoints, '2024-12-01');
    expect(result?.disclosedDate).toBe('2024-05-08');
    expect(result?.eps).toBe(300);
  });

  it('finds FY on exact disclosure date', () => {
    const result = findApplicableFY(fyDataPoints, '2025-05-08');
    expect(result?.disclosedDate).toBe('2025-05-08');
    expect(result?.eps).toBe(359.56);
  });

  it('returns null if date is before all FY disclosures', () => {
    const result = findApplicableFY(fyDataPoints, '2023-01-01');
    expect(result).toBeNull();
  });

  it('returns latest FY for date after all disclosures', () => {
    const result = findApplicableFY(fyDataPoints, '2026-01-22');
    expect(result?.disclosedDate).toBe('2025-05-08');
  });
});

describe('filterValidFYData', () => {
  const statements = [
    { periodType: 'FY', disclosedDate: '2024-05-08', eps: 300, bps: 2500 },
    { periodType: '1Q', disclosedDate: '2024-08-01', eps: 100, bps: 0 }, // 1Q excluded
    { periodType: 'FY', disclosedDate: '2025-05-08', eps: 359.56, bps: 2753.09 },
    { periodType: 'FY', disclosedDate: '2023-05-10', eps: 0, bps: 0 }, // Forecast excluded
  ];

  it('filters to FY with valid metrics, sorted by date', () => {
    const result = filterValidFYData(statements, (s) => s);
    expect(result).toHaveLength(2);
    expect(result[0]?.disclosedDate).toBe('2024-05-08');
    expect(result[1]?.disclosedDate).toBe('2025-05-08');
  });

  it('excludes FY with zero EPS and zero BPS (forecasts)', () => {
    const result = filterValidFYData(statements, (s) => s);
    const forecastIncluded = result.some((r) => r.disclosedDate === '2023-05-10');
    expect(forecastIncluded).toBe(false);
  });
});
