/**
 * Portfolio Benchmark Comparison
 *
 * Calculate benchmark comparison metrics (alpha, beta, correlation)
 */

import type { BenchmarkDataPoint, BenchmarkMetrics, PerformanceDataPoint } from './types';
import {
  alignReturns,
  calculateDailyReturns,
  olsRegression,
  type PriceData,
} from './regression-utils';

/** Number of trading days per year (used for annualization) */
const TRADING_DAYS_PER_YEAR = 252;

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
