/**
 * Factor Regression Service
 *
 * Service for two-stage factor regression analysis (risk decomposition)
 */

import { getAllIndexCodes } from '@trading25/shared/db/constants/index-master-data';
import { DrizzleMarketDatabase } from '@trading25/shared/db/drizzle-market-database';
import { DrizzleMarketDataReader } from '@trading25/shared/db/drizzle-market-reader';
import {
  calculateDailyReturns,
  type DailyReturn,
  type FactorRegressionResult,
  performFactorRegression,
} from '@trading25/shared/factor-regression';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { FactorRegressionResponse } from '../schemas/factor-regression';

/**
 * TOPIX index code
 */
const TOPIX_CODE = '0000';

export class FactorRegressionService {
  private marketDb: DrizzleMarketDatabase | null = null;
  private marketReader: DrizzleMarketDataReader | null = null;
  private dbPath: string;

  constructor() {
    this.dbPath = getMarketDbPath();
  }

  /**
   * Get or create MarketDatabase instance
   */
  private getMarketDb(): DrizzleMarketDatabase {
    if (!this.marketDb) {
      this.marketDb = new DrizzleMarketDatabase(this.dbPath);
    }
    return this.marketDb;
  }

  /**
   * Get or create MarketDataReader instance
   */
  private getMarketReader(): DrizzleMarketDataReader {
    if (!this.marketReader) {
      this.marketReader = new DrizzleMarketDataReader(this.dbPath);
    }
    return this.marketReader;
  }

  /**
   * Perform factor regression analysis for a stock
   */
  async analyzeStock(params: { symbol: string; lookbackDays: number }): Promise<FactorRegressionResponse> {
    const { symbol, lookbackDays } = params;
    logger.debug('Starting factor regression analysis', { symbol, lookbackDays });

    const reader = this.getMarketReader();
    const marketDb = this.getMarketDb();

    // 1. Get stock info
    const dbSymbol = `${symbol}0`; // Convert 4-digit to 5-digit format
    const stockInfo = reader.getStockByCode(dbSymbol);
    if (!stockInfo) {
      throw new Error(`Stock ${symbol} not found in market database`);
    }

    // 2. Get stock price data
    const stockData = reader.getStockData(dbSymbol);
    if (stockData.length < 60) {
      throw new Error(`Insufficient data for stock ${symbol}: ${stockData.length} records (minimum 60 required)`);
    }

    logger.debug('Stock data loaded', { symbol, records: stockData.length });

    // 3. Calculate stock returns
    const stockPriceData = stockData.map((d) => ({
      date: d.date.toISOString().split('T')[0] ?? '',
      close: d.close,
    }));
    const stockReturns = calculateDailyReturns(stockPriceData);

    // 4. Get TOPIX data and calculate returns
    const topixData = marketDb.getIndicesDataByCode(TOPIX_CODE);
    if (topixData.length === 0) {
      throw new Error('TOPIX data not found in market database');
    }

    const topixPriceData = topixData
      .filter((d) => d.close !== null)
      .map((d) => ({
        date: d.date,
        close: d.close as number,
      }));
    const topixReturns = calculateDailyReturns(topixPriceData);

    logger.debug('TOPIX data loaded', { records: topixData.length });

    // 5. Get all indices data for factor matching
    const allIndexCodes = getAllIndexCodes();
    const indicesReturns = new Map<string, DailyReturn[]>();

    for (const code of allIndexCodes) {
      const indexData = marketDb.getIndicesDataByCode(code);
      if (indexData.length === 0) continue;

      const indexPriceData = indexData
        .filter((d) => d.close !== null)
        .map((d) => ({
          date: d.date,
          close: d.close as number,
        }));
      const returns = calculateDailyReturns(indexPriceData);
      indicesReturns.set(code, returns);
    }

    logger.debug('Indices data loaded', { indexCount: indicesReturns.size });

    // 6. Perform factor regression
    const result = performFactorRegression(
      symbol,
      stockReturns,
      topixReturns,
      indicesReturns,
      { lookbackDays, minDataPoints: 60 },
      stockInfo.companyName
    );

    logger.debug('Factor regression complete', {
      symbol,
      marketBeta: result.marketBeta.toFixed(3),
      marketRSquared: `${(result.marketRSquared * 100).toFixed(1)}%`,
      dataPoints: result.dataPoints,
    });

    return this.transformToResponse(result);
  }

  /**
   * Transform internal result to API response
   */
  private transformToResponse(result: FactorRegressionResult): FactorRegressionResponse {
    return {
      stockCode: result.stockCode,
      companyName: result.companyName,
      marketBeta: Math.round(result.marketBeta * 1000) / 1000,
      marketRSquared: Math.round(result.marketRSquared * 1000) / 1000,
      sector17Matches: result.sector17Matches.map((m) => ({
        indexCode: m.indexCode,
        indexName: m.indexName,
        category: m.category,
        rSquared: Math.round(m.rSquared * 1000) / 1000,
        beta: Math.round(m.beta * 1000) / 1000,
      })),
      sector33Matches: result.sector33Matches.map((m) => ({
        indexCode: m.indexCode,
        indexName: m.indexName,
        category: m.category,
        rSquared: Math.round(m.rSquared * 1000) / 1000,
        beta: Math.round(m.beta * 1000) / 1000,
      })),
      topixStyleMatches: result.topixStyleMatches.map((m) => ({
        indexCode: m.indexCode,
        indexName: m.indexName,
        category: m.category,
        rSquared: Math.round(m.rSquared * 1000) / 1000,
        beta: Math.round(m.beta * 1000) / 1000,
      })),
      analysisDate: result.analysisDate,
      dataPoints: result.dataPoints,
      dateRange: result.dateRange,
    };
  }

  /**
   * Close database connections
   */
  close(): void {
    if (this.marketDb) {
      this.marketDb.close();
      this.marketDb = null;
    }
    if (this.marketReader) {
      this.marketReader.close();
      this.marketReader = null;
    }
  }
}
