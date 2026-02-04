/**
 * Portfolio Factor Regression
 *
 * Performs two-stage factor regression on a portfolio by calculating
 * weighted average returns based on current market values.
 */

import type { PortfolioItem } from '../portfolio/types';
import { performFactorRegression } from './factor-regression';
import type {
  DailyReturn,
  ExcludedStock,
  FactorRegressionOptions,
  PortfolioFactorRegressionResult,
  PortfolioWeight,
} from './types';
import { FactorRegressionError } from './types';

/**
 * Minimum data points required for a stock to be included in portfolio analysis
 */
const MIN_STOCK_DATA_POINTS = 30;

/**
 * Minimum weight coverage required to include a date in portfolio returns
 * (e.g., 0.5 means we need data for stocks representing at least 50% of portfolio value)
 */
const MIN_WEIGHT_COVERAGE = 0.5;

/**
 * Portfolio item with weight and market value information
 */
interface WeightedPortfolioItem extends PortfolioItem {
  weight: number;
  latestPrice: number;
  marketValue: number;
}

/**
 * Calculate portfolio weights based on current market values
 *
 * @param items - Portfolio items with holdings
 * @param latestPrices - Map of stock code (4-digit) -> latest close price
 * @returns Array of weighted portfolio items
 */
export function calculatePortfolioWeights(
  items: PortfolioItem[],
  latestPrices: Map<string, number>
): WeightedPortfolioItem[] {
  // Calculate market value for each item
  const itemsWithValue = items.map((item) => {
    // Use latest price, fallback to purchase price if not available
    const latestPrice = latestPrices.get(item.code) ?? item.purchasePrice;
    const marketValue = item.quantity * latestPrice;
    return {
      ...item,
      latestPrice,
      marketValue,
    };
  });

  // Calculate total portfolio value
  const totalValue = itemsWithValue.reduce((sum, item) => sum + item.marketValue, 0);

  if (totalValue === 0) {
    throw new FactorRegressionError('Portfolio has zero total value', 'ZERO_PORTFOLIO_VALUE');
  }

  // Calculate weights
  return itemsWithValue.map((item) => ({
    ...item,
    weight: item.marketValue / totalValue,
  }));
}

/**
 * Result of weighted portfolio returns calculation
 */
interface WeightedReturnsResult {
  portfolioReturns: DailyReturn[];
  excludedStocks: ExcludedStock[];
  includedWeights: WeightedPortfolioItem[];
}

/**
 * Partition items into included and excluded based on data availability
 */
function partitionItemsByDataAvailability(
  stockReturnsMap: Map<string, DailyReturn[]>,
  weightedItems: WeightedPortfolioItem[]
): { includedItems: WeightedPortfolioItem[]; excludedStocks: ExcludedStock[] } {
  const excludedStocks: ExcludedStock[] = [];
  const includedItems: WeightedPortfolioItem[] = [];

  for (const item of weightedItems) {
    const returns = stockReturnsMap.get(item.code);
    if (!returns) {
      excludedStocks.push({
        code: item.code,
        companyName: item.companyName,
        reason: 'No price data available in market database',
      });
    } else if (returns.length < MIN_STOCK_DATA_POINTS) {
      excludedStocks.push({
        code: item.code,
        companyName: item.companyName,
        reason: `Insufficient data points: ${returns.length} < ${MIN_STOCK_DATA_POINTS}`,
      });
    } else {
      includedItems.push(item);
    }
  }

  return { includedItems, excludedStocks };
}

/**
 * Normalize weights after excluding some items
 */
function normalizeWeights(items: WeightedPortfolioItem[]): WeightedPortfolioItem[] {
  const totalValue = items.reduce((sum, item) => sum + item.marketValue, 0);
  return items.map((item) => ({
    ...item,
    weight: item.marketValue / totalValue,
  }));
}

/**
 * Build date -> return lookup maps for each stock
 */
function buildStockReturnMaps(
  stockReturnsMap: Map<string, DailyReturn[]>,
  items: WeightedPortfolioItem[]
): { allDates: string[]; stockReturnMaps: Map<string, Map<string, number>> } {
  const allDatesSet = new Set<string>();
  const stockReturnMaps = new Map<string, Map<string, number>>();

  for (const item of items) {
    const returns = stockReturnsMap.get(item.code);
    if (returns) {
      const dateMap = new Map<string, number>();
      for (const r of returns) {
        allDatesSet.add(r.date);
        dateMap.set(r.date, r.return);
      }
      stockReturnMaps.set(item.code, dateMap);
    }
  }

  return { allDates: [...allDatesSet].sort(), stockReturnMaps };
}

/**
 * Calculate portfolio return for a single date
 */
function calculateDailyPortfolioReturn(
  date: string,
  normalizedItems: WeightedPortfolioItem[],
  stockReturnMaps: Map<string, Map<string, number>>
): DailyReturn | null {
  let weightedReturn = 0;
  let totalWeight = 0;

  for (const item of normalizedItems) {
    const returnMap = stockReturnMaps.get(item.code);
    const stockReturn = returnMap?.get(date);
    if (stockReturn !== undefined) {
      weightedReturn += item.weight * stockReturn;
      totalWeight += item.weight;
    }
  }

  if (totalWeight < MIN_WEIGHT_COVERAGE) {
    return null;
  }

  return { date, return: weightedReturn / totalWeight };
}

/**
 * Calculate weighted portfolio returns from individual stock returns
 *
 * @param stockReturnsMap - Map of stock code (4-digit) -> daily returns
 * @param weightedItems - Portfolio items with weights
 * @returns Portfolio daily returns (weighted average) and excluded stocks
 */
export function calculateWeightedPortfolioReturns(
  stockReturnsMap: Map<string, DailyReturn[]>,
  weightedItems: WeightedPortfolioItem[]
): WeightedReturnsResult {
  // Partition items by data availability
  const { includedItems, excludedStocks } = partitionItemsByDataAvailability(stockReturnsMap, weightedItems);

  if (includedItems.length === 0) {
    throw new FactorRegressionError('No stocks with sufficient data for analysis', 'NO_VALID_STOCKS', {
      excludedCount: excludedStocks.length,
    });
  }

  // Normalize weights after exclusions
  const normalizedItems = normalizeWeights(includedItems);

  // Build lookup maps
  const { allDates, stockReturnMaps } = buildStockReturnMaps(stockReturnsMap, normalizedItems);

  // Calculate weighted returns for each date
  const portfolioReturns: DailyReturn[] = [];
  for (const date of allDates) {
    const dailyReturn = calculateDailyPortfolioReturn(date, normalizedItems, stockReturnMaps);
    if (dailyReturn) {
      portfolioReturns.push(dailyReturn);
    }
  }

  return {
    portfolioReturns,
    excludedStocks,
    includedWeights: normalizedItems,
  };
}

/**
 * Perform factor regression on a portfolio
 *
 * @param portfolioId - Portfolio ID
 * @param portfolioName - Portfolio name
 * @param items - Portfolio items with holdings
 * @param latestPrices - Map of stock code -> latest price (for weight calculation)
 * @param stockReturnsMap - Map of stock code -> daily returns
 * @param topixReturns - TOPIX daily returns
 * @param indicesReturns - Map of index code -> daily returns
 * @param options - Regression options
 * @returns Portfolio factor regression result
 */
export function performPortfolioFactorRegression(
  portfolioId: number,
  portfolioName: string,
  items: PortfolioItem[],
  latestPrices: Map<string, number>,
  stockReturnsMap: Map<string, DailyReturn[]>,
  topixReturns: DailyReturn[],
  indicesReturns: Map<string, DailyReturn[]>,
  options?: FactorRegressionOptions
): PortfolioFactorRegressionResult {
  // Calculate weights based on current market values
  const weightedItems = calculatePortfolioWeights(items, latestPrices);

  // Calculate weighted portfolio returns
  const { portfolioReturns, excludedStocks, includedWeights } = calculateWeightedPortfolioReturns(
    stockReturnsMap,
    weightedItems
  );

  // Validate minimum data points
  if (portfolioReturns.length < (options?.minDataPoints ?? 60)) {
    throw new FactorRegressionError(
      `Insufficient portfolio data points: ${portfolioReturns.length}`,
      'INSUFFICIENT_PORTFOLIO_DATA',
      { dataPoints: portfolioReturns.length }
    );
  }

  // Use existing factor regression with portfolio returns
  const regressionResult = performFactorRegression(
    `portfolio-${portfolioId}`,
    portfolioReturns,
    topixReturns,
    indicesReturns,
    options,
    portfolioName
  );

  // Build weights info for response (all original items, not just included)
  const totalValue = weightedItems.reduce((sum, w) => sum + w.marketValue, 0);

  const weights: PortfolioWeight[] = weightedItems.map((w) => ({
    code: w.code,
    companyName: w.companyName,
    weight: w.weight,
    latestPrice: w.latestPrice,
    marketValue: w.marketValue,
    quantity: w.quantity,
  }));

  return {
    portfolioId,
    portfolioName,
    weights,
    totalValue,
    stockCount: items.length,
    includedStockCount: includedWeights.length,
    marketBeta: regressionResult.marketBeta,
    marketRSquared: regressionResult.marketRSquared,
    sector17Matches: regressionResult.sector17Matches,
    sector33Matches: regressionResult.sector33Matches,
    topixStyleMatches: regressionResult.topixStyleMatches,
    analysisDate: regressionResult.analysisDate,
    dataPoints: regressionResult.dataPoints,
    dateRange: regressionResult.dateRange,
    excludedStocks,
  };
}
