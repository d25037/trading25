/**
 * Portfolio Performance Module
 *
 * P&L calculations and benchmark comparison for portfolio analysis
 */

// Benchmark
export {
  calculateBenchmarkMetrics,
  formatBenchmarkMetrics,
  generateBenchmarkTimeSeries,
  logReturnToPercent,
} from './benchmark';
export type { PortfolioItemWithPrice, PriceTimeSeries } from './calculations';

// Calculations
export {
  calculateAllHoldingsPerformance,
  calculateHoldingPerformance,
  calculatePortfolioSummary,
  calculatePortfolioTimeSeries,
  formatCurrency,
  formatReturnRate,
  logToSimpleReturn,
} from './calculations';
// Types
export type {
  BenchmarkDataPoint,
  BenchmarkMetrics,
  HoldingPerformance,
  PerformanceDataPoint,
  PortfolioPerformanceOptions,
  PortfolioPerformanceResult,
  PortfolioSummary,
} from './types';
export { PortfolioPerformanceError } from './types';
