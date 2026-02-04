import { describe, expect, it } from 'bun:test';
import { calculateResiduals, olsRegression } from '../regression';
import { FactorRegressionError } from '../types';

describe('olsRegression', () => {
  it('fits a perfect linear relationship y = 2x + 1', () => {
    const x = [1, 2, 3, 4, 5];
    const y = x.map((xi) => 2 * xi + 1); // [3, 5, 7, 9, 11]
    const result = olsRegression(y, x);
    expect(result.beta).toBeCloseTo(2, 10);
    expect(result.alpha).toBeCloseTo(1, 10);
    expect(result.rSquared).toBeCloseTo(1, 10);
    for (const r of result.residuals) {
      expect(r).toBeCloseTo(0, 10);
    }
  });

  it('returns zero beta and rSquared for uncorrelated data', () => {
    // x increases, y stays constant
    const x = [1, 2, 3, 4, 5];
    const y = [5, 5, 5, 5, 5];
    const result = olsRegression(y, x);
    expect(result.beta).toBeCloseTo(0, 10);
    expect(result.rSquared).toBe(0); // ssTot is 0
    expect(result.alpha).toBeCloseTo(5, 10);
  });

  it('handles constant x (zero variance)', () => {
    const x = [3, 3, 3, 3, 3];
    const y = [1, 2, 3, 4, 5];
    const result = olsRegression(y, x);
    expect(result.beta).toBe(0);
    expect(result.alpha).toBeCloseTo(3, 10); // mean of y
    expect(result.rSquared).toBe(0);
  });

  it('throws on mismatched array lengths', () => {
    expect(() => olsRegression([1, 2, 3], [1, 2])).toThrow(FactorRegressionError);
  });

  it('throws on insufficient data (less than 2 points)', () => {
    expect(() => olsRegression([1], [1])).toThrow(FactorRegressionError);
    expect(() => olsRegression([], [])).toThrow(FactorRegressionError);
  });

  it('clamps R² to [0, 1]', () => {
    // With noisy data, R² should be between 0 and 1
    const x = [1, 2, 3, 4, 5];
    const y = [2, 4, 5, 4, 10];
    const result = olsRegression(y, x);
    expect(result.rSquared).toBeGreaterThanOrEqual(0);
    expect(result.rSquared).toBeLessThanOrEqual(1);
  });
});

describe('calculateResiduals', () => {
  it('calculates residuals for known alpha and beta', () => {
    const x = [1, 2, 3];
    const y = [3, 5, 7]; // y = 2x + 1
    const residuals = calculateResiduals(y, x, 1, 2);
    for (const r of residuals) {
      expect(r).toBeCloseTo(0, 10);
    }
  });

  it('calculates non-zero residuals', () => {
    const x = [1, 2, 3];
    const y = [4, 6, 10]; // not exactly 2x + 1
    const residuals = calculateResiduals(y, x, 1, 2);
    expect(residuals[0]).toBeCloseTo(1, 10); // 4 - (1 + 2*1) = 1
    expect(residuals[1]).toBeCloseTo(1, 10); // 6 - (1 + 2*2) = 1
    expect(residuals[2]).toBeCloseTo(3, 10); // 10 - (1 + 2*3) = 3
  });

  it('throws on mismatched array lengths', () => {
    expect(() => calculateResiduals([1, 2], [1], 0, 1)).toThrow(FactorRegressionError);
  });
});
