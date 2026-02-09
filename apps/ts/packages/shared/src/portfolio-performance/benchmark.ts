/**
 * Portfolio Benchmark Comparison
 *
 * Calculate benchmark comparison metrics (alpha, beta, correlation)
 */

import type { BenchmarkDataPoint, BenchmarkMetrics, PerformanceDataPoint } from './types';

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
}

export interface PriceData {
  date: string;
  close: number;
}

/** Number of trading days per year (used for annualization) */
const TRADING_DAYS_PER_YEAR = 252;

function calculateDailyReturns(priceData: PriceData[]): DailyReturn[] {
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

function alignReturns(stockReturns: DailyReturn[], indexReturns: DailyReturn[]): AlignedReturns {
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

function variance(arr: number[], meanVal: number): number {
  if (arr.length === 0) return 0;
  let sum = 0;
  for (const val of arr) {
    sum += (val - meanVal) ** 2;
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

function olsRegression(y: number[], x: number[]): OLSResult {
  if (y.length !== x.length || y.length < 2) {
    return { alpha: 0, beta: 0, rSquared: 0 };
  }

  const meanY = mean(y);
  const meanX = mean(x);
  const varX = variance(x, meanX);
  if (varX === 0) {
    return { alpha: meanY, beta: 0, rSquared: 0 };
  }

  const beta = covariance(x, y, meanX, meanY) / varX;
  const alpha = meanY - beta * meanX;

  let ssRes = 0;
  let ssTot = 0;
  for (let i = 0; i < y.length; i++) {
    const yi = y[i];
    const xi = x[i];
    if (yi !== undefined && xi !== undefined) {
      const residual = yi - (alpha + beta * xi);
      ssRes += residual ** 2;
      ssTot += (yi - meanY) ** 2;
    }
  }

  const rSquared = ssTot === 0 ? 0 : 1 - ssRes / ssTot;
  return { alpha, beta, rSquared: Math.max(0, Math.min(1, rSquared)) };
}

/**
 * Calculate Pearson correlation coefficient
 */
function calculateCorrelation(x: number[], y: number[]): number {
  if (x.length !== y.length || x.length === 0) return 0;

  const n = x.length;
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumX2 = 0;
  let sumY2 = 0;

  for (let i = 0; i < n; i++) {
    const xi = x[i];
    const yi = y[i];
    if (xi !== undefined && yi !== undefined) {
      sumX += xi;
      sumY += yi;
      sumXY += xi * yi;
      sumX2 += xi * xi;
      sumY2 += yi * yi;
    }
  }

  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

  if (denominator === 0) return 0;
  return numerator / denominator;
}

/**
 * Calculate cumulative return from daily returns
 */
function calculateCumulativeReturn(dailyReturns: number[]): number {
  let cumulative = 0;
  for (const r of dailyReturns) {
    cumulative += r;
  }
  return cumulative;
}

/**
 * Calculate benchmark comparison metrics
 *
 * @param portfolioReturns Portfolio daily returns (from calculatePortfolioTimeSeries)
 * @param benchmarkPrices Benchmark index price data
 * @param benchmarkCode Benchmark index code (e.g., "0000")
 * @param benchmarkName Benchmark name (e.g., "TOPIX")
 * @returns Benchmark metrics or null if insufficient data
 */
export function calculateBenchmarkMetrics(
  portfolioReturns: PerformanceDataPoint[],
  benchmarkPrices: PriceData[],
  benchmarkCode: string,
  benchmarkName: string,
  minDataPoints = 30
): BenchmarkMetrics | null {
  if (portfolioReturns.length < minDataPoints) {
    return null;
  }

  // Calculate benchmark daily returns
  const benchmarkReturns = calculateDailyReturns(benchmarkPrices);

  if (benchmarkReturns.length < minDataPoints) {
    return null;
  }

  // Convert portfolio returns to DailyReturn format for alignment
  const portfolioDailyReturns = portfolioReturns.map((p) => ({
    date: p.date,
    return: p.dailyReturn,
  }));

  // Align returns to matching dates
  const aligned = alignReturns(portfolioDailyReturns, benchmarkReturns);

  if (aligned.dates.length < minDataPoints) {
    return null;
  }

  // Perform OLS regression: portfolio = alpha + beta * benchmark
  const regression = olsRegression(aligned.stockReturns, aligned.indexReturns);

  // Calculate correlation
  const correlation = calculateCorrelation(aligned.stockReturns, aligned.indexReturns);

  // Calculate cumulative returns
  const portfolioCumulativeReturn = calculateCumulativeReturn(aligned.stockReturns);
  const benchmarkCumulativeReturn = calculateCumulativeReturn(aligned.indexReturns);

  // Calculate relative return (outperformance)
  const relativeReturn = portfolioCumulativeReturn - benchmarkCumulativeReturn;

  // Annualize alpha
  const annualizedAlpha = regression.alpha * TRADING_DAYS_PER_YEAR;

  return {
    code: benchmarkCode,
    name: benchmarkName,
    beta: regression.beta,
    alpha: annualizedAlpha,
    correlation,
    rSquared: regression.rSquared,
    benchmarkReturn: benchmarkCumulativeReturn,
    relativeReturn,
  };
}

/**
 * Generate benchmark comparison time series
 *
 * @param portfolioReturns Portfolio performance data points
 * @param benchmarkPrices Benchmark price data
 * @returns Array of benchmark data points with aligned cumulative returns
 */
export function generateBenchmarkTimeSeries(
  portfolioReturns: PerformanceDataPoint[],
  benchmarkPrices: PriceData[]
): BenchmarkDataPoint[] {
  // Calculate benchmark daily returns
  const benchmarkReturns = calculateDailyReturns(benchmarkPrices);

  // Create lookup map for benchmark returns
  const benchmarkReturnMap = new Map<string, number>();
  for (const r of benchmarkReturns) {
    benchmarkReturnMap.set(r.date, r.return);
  }

  // Generate aligned time series
  const results: BenchmarkDataPoint[] = [];
  let benchmarkCumulative = 0;

  for (const p of portfolioReturns) {
    const benchmarkDailyReturn = benchmarkReturnMap.get(p.date);

    if (benchmarkDailyReturn !== undefined) {
      benchmarkCumulative += benchmarkDailyReturn;

      results.push({
        date: p.date,
        portfolioReturn: p.cumulativeReturn,
        benchmarkReturn: benchmarkCumulative,
      });
    }
  }

  return results;
}

/**
 * Convert log returns to simple percentage returns for display
 */
export function logReturnToPercent(logReturn: number): number {
  return (Math.exp(logReturn) - 1) * 100;
}

/**
 * Format benchmark metrics for display
 */
export function formatBenchmarkMetrics(metrics: BenchmarkMetrics): Record<string, string> {
  return {
    beta: metrics.beta.toFixed(2),
    alpha: `${metrics.alpha >= 0 ? '+' : ''}${(metrics.alpha * 100).toFixed(2)}%`,
    correlation: metrics.correlation.toFixed(2),
    rSquared: `${(metrics.rSquared * 100).toFixed(1)}%`,
    benchmarkReturn: `${metrics.benchmarkReturn >= 0 ? '+' : ''}${logReturnToPercent(metrics.benchmarkReturn).toFixed(2)}%`,
    relativeReturn: `${metrics.relativeReturn >= 0 ? '+' : ''}${logReturnToPercent(metrics.relativeReturn).toFixed(2)}%`,
  };
}
