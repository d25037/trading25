/**
 * Factor Analysis - OLS Regression
 *
 * Ordinary Least Squares regression implementation
 */

import type { OLSResult } from './types';
import { FactorRegressionError } from './types';

/**
 * Calculate mean of an array
 */
function mean(arr: number[]): number {
  if (arr.length === 0) return 0;
  let sum = 0;
  for (const val of arr) {
    sum += val;
  }
  return sum / arr.length;
}

/**
 * Calculate covariance between two arrays
 * Cov(x,y) = E[(x - μx)(y - μy)]
 */
function covariance(x: number[], y: number[], meanX: number, meanY: number): number {
  if (x.length !== y.length || x.length === 0) return 0;

  let sum = 0;
  for (let i = 0; i < x.length; i++) {
    const xi = x[i];
    const yi = y[i];
    if (xi !== undefined && yi !== undefined) {
      sum += (xi - meanX) * (yi - meanY);
    }
  }
  return sum / x.length;
}

/**
 * Calculate variance of an array
 * Var(x) = E[(x - μ)²]
 */
function variance(arr: number[], meanVal: number): number {
  if (arr.length === 0) return 0;

  let sum = 0;
  for (const val of arr) {
    sum += (val - meanVal) ** 2;
  }
  return sum / arr.length;
}

/**
 * Perform Ordinary Least Squares regression
 * y = alpha + beta * x + residual
 *
 * Uses standard OLS formulas:
 * beta = Cov(x,y) / Var(x)
 * alpha = mean(y) - beta * mean(x)
 * R² = 1 - SS_res / SS_tot
 *
 * @param y - dependent variable (e.g., stock returns)
 * @param x - independent variable (e.g., index returns)
 * @returns OLS regression result
 */
export function olsRegression(y: number[], x: number[]): OLSResult {
  if (y.length !== x.length) {
    throw new FactorRegressionError('Arrays must have same length', 'ARRAY_LENGTH_MISMATCH', {
      yLength: y.length,
      xLength: x.length,
    });
  }

  if (y.length < 2) {
    throw new FactorRegressionError('At least 2 data points required', 'INSUFFICIENT_DATA', {
      dataPoints: y.length,
    });
  }

  const meanY = mean(y);
  const meanX = mean(x);
  const varX = variance(x, meanX);

  // Handle zero variance case (constant x values)
  if (varX === 0) {
    return {
      alpha: meanY,
      beta: 0,
      rSquared: 0,
      residuals: y.map((yi) => yi - meanY),
    };
  }

  // Calculate beta: Cov(x,y) / Var(x)
  const covXY = covariance(x, y, meanX, meanY);
  const beta = covXY / varX;

  // Calculate alpha: mean(y) - beta * mean(x)
  const alpha = meanY - beta * meanX;

  // Calculate residuals and R²
  const residuals: number[] = [];
  let ssRes = 0; // Sum of squared residuals
  let ssTot = 0; // Total sum of squares

  for (let i = 0; i < y.length; i++) {
    const yi = y[i];
    const xi = x[i];
    if (yi !== undefined && xi !== undefined) {
      const predicted = alpha + beta * xi;
      const residual = yi - predicted;
      residuals.push(residual);
      ssRes += residual ** 2;
      ssTot += (yi - meanY) ** 2;
    }
  }

  // R² = 1 - SS_res / SS_tot
  // Handle edge case where ssTot is 0 (all y values are the same)
  const rSquared = ssTot === 0 ? 0 : 1 - ssRes / ssTot;

  return {
    alpha,
    beta,
    rSquared: Math.max(0, Math.min(1, rSquared)), // Clamp to [0, 1]
    residuals,
  };
}

/**
 * Calculate residuals from regression
 * residual_i = y_i - (alpha + beta * x_i)
 *
 * @param y - dependent variable values
 * @param x - independent variable values
 * @param alpha - intercept
 * @param beta - slope
 * @returns Array of residuals
 */
export function calculateResiduals(y: number[], x: number[], alpha: number, beta: number): number[] {
  if (y.length !== x.length) {
    throw new FactorRegressionError('Arrays must have same length', 'ARRAY_LENGTH_MISMATCH', {
      yLength: y.length,
      xLength: x.length,
    });
  }

  const residuals: number[] = [];
  for (let i = 0; i < y.length; i++) {
    const yi = y[i];
    const xi = x[i];
    if (yi !== undefined && xi !== undefined) {
      residuals.push(yi - (alpha + beta * xi));
    }
  }

  return residuals;
}
