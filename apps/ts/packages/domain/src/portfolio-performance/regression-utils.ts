/**
 * Internal regression helpers for portfolio benchmark calculations.
 *
 * Kept local to portfolio-performance so shared no longer depends on
 * legacy factor-regression domain modules.
 */

export interface PriceData {
  date: string;
  close: number;
}

interface DailyReturn {
  date: string;
  return: number;
}

interface AlignedReturns {
  dates: string[];
  stockReturns: number[];
  indexReturns: number[];
}

interface OLSResult {
  alpha: number;
  beta: number;
  rSquared: number;
  residuals: number[];
}

/**
 * Calculate daily log returns from price data.
 */
export function calculateDailyReturns(priceData: PriceData[]): DailyReturn[] {
  if (priceData.length < 2) {
    return [];
  }

  const returns: DailyReturn[] = [];

  for (let i = 1; i < priceData.length; i++) {
    const current = priceData[i];
    const previous = priceData[i - 1];

    if (!current || !previous || previous.close <= 0 || current.close <= 0) {
      continue;
    }

    returns.push({
      date: current.date,
      return: Math.log(current.close / previous.close),
    });
  }

  return returns;
}

/**
 * Align two return series by date.
 */
export function alignReturns(
  stockReturns: { date: string; return: number }[],
  indexReturns: { date: string; return: number }[]
): AlignedReturns {
  const indexMap = new Map<string, number>();
  for (const item of indexReturns) {
    indexMap.set(item.date, item.return);
  }

  const dates: string[] = [];
  const alignedStock: number[] = [];
  const alignedIndex: number[] = [];

  for (const stock of stockReturns) {
    const indexReturn = indexMap.get(stock.date);
    if (indexReturn !== undefined) {
      dates.push(stock.date);
      alignedStock.push(stock.return);
      alignedIndex.push(indexReturn);
    }
  }

  return {
    dates,
    stockReturns: alignedStock,
    indexReturns: alignedIndex,
  };
}

function mean(arr: number[]): number {
  if (arr.length === 0) return 0;
  let sum = 0;
  for (const val of arr) {
    sum += val;
  }
  return sum / arr.length;
}

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

function variance(arr: number[], meanVal: number): number {
  if (arr.length === 0) return 0;

  let sum = 0;
  for (const val of arr) {
    sum += (val - meanVal) ** 2;
  }
  return sum / arr.length;
}

/**
 * Simple OLS regression (y = alpha + beta * x).
 */
export function olsRegression(y: number[], x: number[]): OLSResult {
  if (y.length !== x.length) {
    throw new Error(`Arrays must have same length (y=${y.length}, x=${x.length})`);
  }

  if (y.length < 2) {
    throw new Error('At least 2 data points required');
  }

  const meanY = mean(y);
  const meanX = mean(x);
  const varX = variance(x, meanX);

  if (varX === 0) {
    return {
      alpha: meanY,
      beta: 0,
      rSquared: 0,
      residuals: y.map((yi) => yi - meanY),
    };
  }

  const covXY = covariance(x, y, meanX, meanY);
  const beta = covXY / varX;
  const alpha = meanY - beta * meanX;

  const residuals: number[] = [];
  let ssRes = 0;
  let ssTot = 0;

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

  const rSquared = ssTot === 0 ? 0 : 1 - ssRes / ssTot;

  return {
    alpha,
    beta,
    rSquared: Math.max(0, Math.min(1, rSquared)),
    residuals,
  };
}
