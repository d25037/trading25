/**
 * Factor Regression Module
 *
 * Two-stage factor regression for stock/portfolio risk decomposition
 */

// Factor regression
export { performFactorRegression } from './factor-regression.js';
// Portfolio factor regression
export {
  calculatePortfolioWeights,
  calculateWeightedPortfolioReturns,
  performPortfolioFactorRegression,
} from './portfolio-regression.js';
// OLS regression
export { calculateResiduals, olsRegression } from './regression.js';
// Return calculations
export type { AlignedReturns, PriceData } from './returns.js';
export { alignReturns, calculateDailyReturns, calculateSimpleReturns } from './returns.js';
// Types
export type {
  DailyReturn,
  ExcludedStock,
  FactorRegressionOptions,
  FactorRegressionResult,
  IndexMatch,
  OLSResult,
  PortfolioFactorRegressionResult,
  PortfolioWeight,
} from './types.js';
export { FactorRegressionError } from './types.js';
