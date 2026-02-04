/**
 * Factor Analysis - Return Calculations
 *
 * Functions for calculating daily returns from price data
 */

import type { DailyReturn } from './types';

/**
 * Price data point interface
 */
export interface PriceData {
  date: string; // YYYY-MM-DD
  close: number;
}

/**
 * Calculate daily log returns from price data
 * r_t = ln(P_t / P_{t-1})
 *
 * @param priceData - Array of price data sorted by date ascending
 * @returns Array of daily returns (one less than input)
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

    // Log return: ln(P_t / P_{t-1})
    const logReturn = Math.log(current.close / previous.close);

    returns.push({
      date: current.date,
      return: logReturn,
    });
  }

  return returns;
}

/**
 * Aligned returns for regression
 */
export interface AlignedReturns {
  dates: string[];
  stockReturns: number[];
  indexReturns: number[];
}

/**
 * Align returns for two series (stock and index) to same dates
 * Returns only dates where both series have data
 *
 * @param stockReturns - Stock daily returns
 * @param indexReturns - Index daily returns
 * @returns Aligned returns arrays with matching dates
 */
export function alignReturns(stockReturns: DailyReturn[], indexReturns: DailyReturn[]): AlignedReturns {
  // Build index map for quick lookup
  const indexMap = new Map<string, number>();
  for (const item of indexReturns) {
    indexMap.set(item.date, item.return);
  }

  // Align based on stock dates
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

/**
 * Calculate simple (arithmetic) returns instead of log returns
 * r_t = (P_t - P_{t-1}) / P_{t-1}
 *
 * @param priceData - Array of price data sorted by date ascending
 * @returns Array of daily returns
 */
export function calculateSimpleReturns(priceData: PriceData[]): DailyReturn[] {
  if (priceData.length < 2) {
    return [];
  }

  const returns: DailyReturn[] = [];

  for (let i = 1; i < priceData.length; i++) {
    const current = priceData[i];
    const previous = priceData[i - 1];

    if (!current || !previous || previous.close <= 0) {
      continue;
    }

    // Simple return: (P_t - P_{t-1}) / P_{t-1}
    const simpleReturn = (current.close - previous.close) / previous.close;

    returns.push({
      date: current.date,
      return: simpleReturn,
    });
  }

  return returns;
}
