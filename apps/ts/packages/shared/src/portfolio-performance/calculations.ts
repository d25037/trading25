/**
 * Portfolio Performance Calculations
 *
 * Core P&L and return calculations for portfolio holdings
 */

import type { HoldingPerformance, PerformanceDataPoint, PortfolioSummary } from './types';

/**
 * Portfolio item with current price for calculations
 */
export interface PortfolioItemWithPrice {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  /** Current price from market DB */
  currentPrice: number;
}

/**
 * Calculate performance metrics for a single holding
 */
export function calculateHoldingPerformance(
  item: PortfolioItemWithPrice,
  totalPortfolioValue: number
): HoldingPerformance {
  const cost = item.purchasePrice * item.quantity;
  const marketValue = item.currentPrice * item.quantity;
  const pnl = marketValue - cost;
  const returnRate = cost > 0 ? pnl / cost : 0;
  const weight = totalPortfolioValue > 0 ? marketValue / totalPortfolioValue : 0;

  return {
    code: item.code,
    companyName: item.companyName,
    quantity: item.quantity,
    purchasePrice: item.purchasePrice,
    currentPrice: item.currentPrice,
    cost,
    marketValue,
    pnl,
    returnRate,
    weight,
    purchaseDate: item.purchaseDate,
    account: item.account,
  };
}

/**
 * Calculate portfolio summary metrics
 */
export function calculatePortfolioSummary(holdings: HoldingPerformance[]): PortfolioSummary {
  let totalCost = 0;
  let currentValue = 0;

  for (const holding of holdings) {
    totalCost += holding.cost;
    currentValue += holding.marketValue;
  }

  const totalPnL = currentValue - totalCost;
  const returnRate = totalCost > 0 ? totalPnL / totalCost : 0;

  return {
    totalCost,
    currentValue,
    totalPnL,
    returnRate,
  };
}

/**
 * Calculate all holdings performance with summary
 */
export function calculateAllHoldingsPerformance(items: PortfolioItemWithPrice[]): {
  holdings: HoldingPerformance[];
  summary: PortfolioSummary;
} {
  // First pass: calculate total portfolio value
  let totalValue = 0;
  for (const item of items) {
    totalValue += item.currentPrice * item.quantity;
  }

  // Second pass: calculate each holding's performance with weight
  const holdings: HoldingPerformance[] = [];
  for (const item of items) {
    holdings.push(calculateHoldingPerformance(item, totalValue));
  }

  // Calculate summary
  const summary = calculatePortfolioSummary(holdings);

  return { holdings, summary };
}

/**
 * Price data for time series calculation
 */
export interface PriceTimeSeries {
  date: string;
  close: number;
}

/**
 * Collect all unique dates from stock prices and sort them
 */
function collectSortedDates(stockPrices: Map<string, PriceTimeSeries[]>): string[] {
  const allDates = new Set<string>();
  for (const prices of stockPrices.values()) {
    for (const p of prices) {
      allDates.add(p.date);
    }
  }
  return Array.from(allDates).sort();
}

/**
 * Build price lookup maps for each stock
 */
function buildPriceMaps(stockPrices: Map<string, PriceTimeSeries[]>): Map<string, Map<string, number>> {
  const priceMaps = new Map<string, Map<string, number>>();
  for (const [code, prices] of stockPrices) {
    const priceMap = new Map<string, number>();
    for (const p of prices) {
      priceMap.set(p.date, p.close);
    }
    priceMaps.set(code, priceMap);
  }
  return priceMaps;
}

/**
 * Calculate weighted portfolio return for a single day
 */
function calculateDailyPortfolioReturn(
  prevDate: string,
  currDate: string,
  weights: Map<string, number>,
  priceMaps: Map<string, Map<string, number>>
): number {
  let portfolioReturn = 0;
  let totalWeight = 0;

  for (const [code, weight] of weights) {
    const priceMap = priceMaps.get(code);
    if (!priceMap) continue;

    const prevPrice = priceMap.get(prevDate);
    const currPrice = priceMap.get(currDate);

    if (prevPrice && currPrice && prevPrice > 0) {
      const stockReturn = Math.log(currPrice / prevPrice);
      portfolioReturn += weight * stockReturn;
      totalWeight += weight;
    }
  }

  // Normalize if not all stocks have data for this date
  if (totalWeight > 0 && totalWeight < 0.99) {
    return portfolioReturn / totalWeight;
  }
  return portfolioReturn;
}

/**
 * Calculate portfolio daily returns from weighted stock returns
 *
 * Portfolio return = sum(weight_i * return_i)
 *
 * @param stockPrices Map of stock code to price time series
 * @param weights Map of stock code to weight (should sum to 1)
 * @returns Array of performance data points
 */
export function calculatePortfolioTimeSeries(
  stockPrices: Map<string, PriceTimeSeries[]>,
  weights: Map<string, number>
): PerformanceDataPoint[] {
  const sortedDates = collectSortedDates(stockPrices);

  if (sortedDates.length < 2) {
    return [];
  }

  const priceMaps = buildPriceMaps(stockPrices);
  const results: PerformanceDataPoint[] = [];
  let cumulativeReturn = 0;

  for (let i = 1; i < sortedDates.length; i++) {
    const prevDate = sortedDates[i - 1];
    const currDate = sortedDates[i];

    if (!prevDate || !currDate) continue;

    const dailyReturn = calculateDailyPortfolioReturn(prevDate, currDate, weights, priceMaps);
    cumulativeReturn += dailyReturn;

    results.push({
      date: currDate,
      dailyReturn,
      cumulativeReturn,
    });
  }

  return results;
}

/**
 * Calculate simple cumulative returns from log returns
 * Converts log cumulative return to simple return: exp(r) - 1
 */
export function logToSimpleReturn(logReturn: number): number {
  return Math.exp(logReturn) - 1;
}

/**
 * Format return rate as percentage string
 */
export function formatReturnRate(rate: number, decimals = 2): string {
  const percentage = rate * 100;
  const sign = percentage >= 0 ? '+' : '';
  return `${sign}${percentage.toFixed(decimals)}%`;
}

/**
 * Format currency value with locale
 */
export function formatCurrency(value: number, locale = 'ja-JP'): string {
  return value.toLocaleString(locale);
}
