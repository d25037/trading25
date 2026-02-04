/**
 * Portfolio Factor Regression Service
 *
 * Service for two-stage factor regression analysis on portfolios
 */

import { getMarketDbPath, getPortfolioDbPath } from '@trading25/shared';
import { getAllIndexCodes } from '@trading25/shared/db/constants/index-master-data';
import { DrizzleMarketDatabase } from '@trading25/shared/db/drizzle-market-database';
import { DrizzleMarketDataReader } from '@trading25/shared/db/drizzle-market-reader';
import {
  calculateDailyReturns,
  type DailyReturn,
  type PortfolioFactorRegressionResult,
  performPortfolioFactorRegression,
} from '@trading25/shared/factor-regression';
import { PortfolioDatabase, PortfolioNotFoundError } from '@trading25/shared/portfolio';
import { logger } from '@trading25/shared/utils/logger';
import type { PortfolioFactorRegressionResponse } from '../schemas/portfolio-factor-regression';

/**
 * TOPIX index code
 */
const TOPIX_CODE = '0000';

export class PortfolioFactorRegressionService {
  private marketDb: DrizzleMarketDatabase | null = null;
  private marketReader: DrizzleMarketDataReader | null = null;
  private portfolioDb: PortfolioDatabase | null = null;
  private marketDbPath: string;
  private portfolioDbPath: string;

  constructor() {
    this.marketDbPath = getMarketDbPath();
    this.portfolioDbPath = getPortfolioDbPath();
  }

  /**
   * Get or create MarketDatabase instance
   */
  private getMarketDb(): DrizzleMarketDatabase {
    if (!this.marketDb) {
      this.marketDb = new DrizzleMarketDatabase(this.marketDbPath);
    }
    return this.marketDb;
  }

  /**
   * Get or create MarketDataReader instance
   */
  private getMarketReader(): DrizzleMarketDataReader {
    if (!this.marketReader) {
      this.marketReader = new DrizzleMarketDataReader(this.marketDbPath);
    }
    return this.marketReader;
  }

  /**
   * Get or create PortfolioDatabase instance
   */
  private getPortfolioDb(): PortfolioDatabase {
    if (!this.portfolioDb) {
      this.portfolioDb = new PortfolioDatabase(this.portfolioDbPath, false);
    }
    return this.portfolioDb;
  }

  /**
   * Perform factor regression analysis for a portfolio
   */
  async analyzePortfolio(params: {
    portfolioId: number;
    lookbackDays: number;
  }): Promise<PortfolioFactorRegressionResponse> {
    const { portfolioId, lookbackDays } = params;
    logger.debug('Starting portfolio factor regression analysis', { portfolioId, lookbackDays });

    // Use try-finally to ensure database connections are properly managed
    // Note: We don't close here because the service is long-lived and reuses connections
    // The close() method is called on SIGINT/SIGTERM for graceful shutdown
    const portfolioDb = this.getPortfolioDb();
    const reader = this.getMarketReader();
    const marketDb = this.getMarketDb();

    // 1. Get portfolio with items
    const portfolio = portfolioDb.getPortfolioWithItems(portfolioId);
    if (!portfolio) {
      throw new PortfolioNotFoundError(portfolioId);
    }

    if (portfolio.items.length === 0) {
      throw new Error('Portfolio has no stocks');
    }

    logger.debug('Portfolio loaded', {
      portfolioId,
      name: portfolio.name,
      stockCount: portfolio.items.length,
    });

    // 2. Get latest prices and stock returns for each stock
    const latestPrices = new Map<string, number>();
    const stockReturnsMap = new Map<string, DailyReturn[]>();

    for (const item of portfolio.items) {
      const dbSymbol = `${item.code}0`; // Convert 4-digit to 5-digit format
      const stockData = reader.getStockData(dbSymbol);

      if (stockData.length > 0) {
        // Get latest price
        const latestData = stockData[stockData.length - 1];
        if (latestData) {
          latestPrices.set(item.code, latestData.close);
        }

        // Calculate returns
        const priceData = stockData.map((d) => ({
          date: d.date.toISOString().split('T')[0] as string,
          close: d.close,
        }));
        const returns = calculateDailyReturns(priceData);
        stockReturnsMap.set(item.code, returns);

        logger.debug('Stock data loaded', {
          code: item.code,
          records: stockData.length,
          latestPrice: latestPrices.get(item.code),
        });
      }
    }

    // 3. Get TOPIX data and calculate returns
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

    // 4. Get all indices data for factor matching
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

    // 5. Perform portfolio factor regression
    const result = performPortfolioFactorRegression(
      portfolioId,
      portfolio.name,
      portfolio.items,
      latestPrices,
      stockReturnsMap,
      topixReturns,
      indicesReturns,
      { lookbackDays, minDataPoints: 60 }
    );

    logger.debug('Portfolio factor regression complete', {
      portfolioId,
      portfolioName: portfolio.name,
      marketBeta: result.marketBeta.toFixed(3),
      marketRSquared: `${(result.marketRSquared * 100).toFixed(1)}%`,
      stockCount: result.stockCount,
      includedStockCount: result.includedStockCount,
      dataPoints: result.dataPoints,
    });

    return this.transformToResponse(result);
  }

  /**
   * Transform internal result to API response
   */
  private transformToResponse(result: PortfolioFactorRegressionResult): PortfolioFactorRegressionResponse {
    return {
      portfolioId: result.portfolioId,
      portfolioName: result.portfolioName,
      weights: result.weights.map((w) => ({
        code: w.code,
        companyName: w.companyName,
        weight: Math.round(w.weight * 1000) / 1000,
        latestPrice: Math.round(w.latestPrice * 100) / 100,
        marketValue: Math.round(w.marketValue),
        quantity: w.quantity,
      })),
      totalValue: Math.round(result.totalValue),
      stockCount: result.stockCount,
      includedStockCount: result.includedStockCount,
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
      excludedStocks: result.excludedStocks.map((s) => ({
        code: s.code,
        companyName: s.companyName,
        reason: s.reason,
      })),
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
    if (this.portfolioDb) {
      this.portfolioDb.close();
      this.portfolioDb = null;
    }
  }
}
