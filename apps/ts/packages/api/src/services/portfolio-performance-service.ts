/**
 * Portfolio Performance Service
 *
 * Business logic for portfolio P&L calculations and benchmark comparison
 */

import * as fs from 'node:fs';
import { DrizzleMarketDataReader } from '@trading25/shared/db';
import type { PriceData } from '@trading25/shared/factor-regression/returns';
import { PortfolioNotFoundError } from '@trading25/shared/portfolio';
import {
  type BenchmarkDataPoint,
  type BenchmarkMetrics,
  calculateAllHoldingsPerformance,
  calculateBenchmarkMetrics,
  calculatePortfolioTimeSeries,
  generateBenchmarkTimeSeries,
  type PerformanceDataPoint,
  type PortfolioItemWithPrice,
  type PortfolioPerformanceResult,
  type PriceTimeSeries,
} from '@trading25/shared/portfolio-performance';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { PortfolioService } from './portfolio-service';

/** Default benchmark index code (TOPIX) */
const DEFAULT_BENCHMARK_CODE = '0000';

/** Benchmark name mapping */
const BENCHMARK_NAMES: Record<string, string> = {
  '0000': 'TOPIX',
  '0001': 'TOPIX Core30',
  '0002': 'TOPIX Large70',
  '0003': 'TOPIX 100',
};

/**
 * Portfolio Performance Service
 */
export class PortfolioPerformanceService {
  private static marketDbReader: DrizzleMarketDataReader | null = null;
  private portfolioService: PortfolioService;

  constructor(portfolioService: PortfolioService) {
    this.portfolioService = portfolioService;
  }

  /**
   * Get market database reader (lazy initialization)
   */
  private getMarketDbReader(): DrizzleMarketDataReader | null {
    if (PortfolioPerformanceService.marketDbReader !== null) {
      return PortfolioPerformanceService.marketDbReader;
    }

    const dbPath = getMarketDbPath();
    if (!fs.existsSync(dbPath)) {
      logger.debug('Market database not found', { dbPath });
      return null;
    }

    try {
      PortfolioPerformanceService.marketDbReader = new DrizzleMarketDataReader(dbPath);
      logger.info('Market database reader initialized for performance service', { dbPath });
      return PortfolioPerformanceService.marketDbReader;
    } catch (error) {
      logger.warn('Failed to initialize market database reader', {
        dbPath,
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  /**
   * Convert 4-digit code to 5-digit JQuants format
   */
  private toJQuantsCode(code: string): string {
    return code.length === 4 ? `${code}0` : code;
  }

  /**
   * Get current price for a stock from market database
   */
  private getCurrentPrice(code: string): number | null {
    const reader = this.getMarketDbReader();
    if (!reader) return null;

    try {
      const jquantsCode = this.toJQuantsCode(code);
      const stockData = reader.getStockData(jquantsCode);

      if (stockData.length === 0) {
        logger.debug('No stock data found for current price', { code });
        return null;
      }

      // Get the latest close price
      const latest = stockData[stockData.length - 1];
      return latest ? latest.close : null;
    } catch (error) {
      logger.warn('Failed to get current price', {
        code,
        error: error instanceof Error ? error.message : error,
      });
      return null;
    }
  }

  /**
   * Get historical prices for a stock
   */
  private getHistoricalPrices(code: string, lookbackDays: number): PriceTimeSeries[] {
    const reader = this.getMarketDbReader();
    if (!reader) return [];

    try {
      const jquantsCode = this.toJQuantsCode(code);
      const stockData = reader.getStockData(jquantsCode);

      if (stockData.length === 0) return [];

      // Get last N days of data
      const startIndex = Math.max(0, stockData.length - lookbackDays);
      const relevantData = stockData.slice(startIndex);

      return relevantData.map((row) => ({
        date: row.date.toISOString().split('T')[0] as string,
        close: row.close,
      }));
    } catch (error) {
      logger.warn('Failed to get historical prices', {
        code,
        error: error instanceof Error ? error.message : error,
      });
      return [];
    }
  }

  /**
   * Get index prices for benchmark comparison
   * Currently only supports TOPIX (code 0000)
   */
  private getIndexPrices(indexCode: string, lookbackDays: number): PriceData[] {
    const reader = this.getMarketDbReader();
    if (!reader) return [];

    try {
      // Currently only TOPIX is supported
      if (indexCode !== '0000') {
        logger.debug('Only TOPIX (0000) is supported for benchmark', { indexCode });
        return [];
      }

      const indexData = reader.getTopixData();

      if (indexData.length === 0) {
        logger.debug('No TOPIX data found');
        return [];
      }

      // Get last N days of data
      const startIndex = Math.max(0, indexData.length - lookbackDays);
      const relevantData = indexData.slice(startIndex);

      return relevantData.map((row) => ({
        date: row.date.toISOString().split('T')[0] as string,
        close: row.close,
      }));
    } catch (error) {
      logger.warn('Failed to get index prices', {
        indexCode,
        error: error instanceof Error ? error.message : error,
      });
      return [];
    }
  }

  /**
   * Convert portfolio item to PortfolioItemWithPrice
   */
  private convertToItemWithPrice(
    item: {
      code: string;
      companyName: string;
      quantity: number;
      purchasePrice: number;
      purchaseDate: Date;
      account?: string | null;
    },
    warnings: string[]
  ): PortfolioItemWithPrice {
    const currentPrice = this.getCurrentPrice(item.code);
    const purchaseDate = item.purchaseDate.toISOString().split('T')[0] as string;

    if (currentPrice === null) {
      warnings.push(`Current price not available for ${item.code} (${item.companyName}), using purchase price`);
    }

    return {
      code: item.code,
      companyName: item.companyName,
      quantity: item.quantity,
      purchasePrice: item.purchasePrice,
      purchaseDate,
      account: item.account || undefined,
      currentPrice: currentPrice ?? item.purchasePrice,
    };
  }

  /**
   * Calculate benchmark metrics and time series
   */
  private calculateBenchmarkData(
    timeSeries: PerformanceDataPoint[],
    benchmarkCode: string,
    lookbackDays: number,
    warnings: string[]
  ): { benchmark: BenchmarkMetrics | null; benchmarkTimeSeries: BenchmarkDataPoint[] | null } {
    const benchmarkPrices = this.getIndexPrices(benchmarkCode, lookbackDays);
    const benchmarkName = BENCHMARK_NAMES[benchmarkCode] || `Index ${benchmarkCode}`;

    const benchmark = calculateBenchmarkMetrics(timeSeries, benchmarkPrices, benchmarkCode, benchmarkName, 30);

    if (!benchmark) {
      warnings.push('Insufficient data for benchmark comparison');
      return { benchmark: null, benchmarkTimeSeries: null };
    }

    const benchmarkTimeSeries = generateBenchmarkTimeSeries(timeSeries, benchmarkPrices);
    return { benchmark, benchmarkTimeSeries };
  }

  /**
   * Calculate portfolio performance with P&L and benchmark comparison
   */
  async getPortfolioPerformance(
    portfolioId: number,
    benchmarkCode: string = DEFAULT_BENCHMARK_CODE,
    lookbackDays = 252
  ): Promise<PortfolioPerformanceResult> {
    logger.debug('Calculating portfolio performance', { portfolioId, benchmarkCode, lookbackDays });

    const portfolio = await this.portfolioService.getPortfolioWithItems(portfolioId);
    if (!portfolio) {
      throw new PortfolioNotFoundError(portfolioId);
    }

    const warnings: string[] = [];
    const analysisDate = new Date().toISOString().split('T')[0] as string;

    if (portfolio.items.length === 0) {
      return this.buildEmptyPerformanceResult(portfolio, analysisDate);
    }

    // Collect items with prices and historical data
    const { itemsWithPrices, stockPricesMap } = this.collectItemData(portfolio.items, lookbackDays, warnings);

    // Calculate holdings performance
    const { holdings, summary } = calculateAllHoldingsPerformance(itemsWithPrices);

    // Build weights map
    const weightsMap = new Map<string, number>();
    for (const holding of holdings) {
      weightsMap.set(holding.code, holding.weight);
    }

    // Calculate time series
    const timeSeries = calculatePortfolioTimeSeries(stockPricesMap, weightsMap);

    // Calculate benchmark data
    const benchmarkResult = this.processBenchmarkData(
      timeSeries,
      stockPricesMap,
      benchmarkCode,
      lookbackDays,
      warnings
    );

    logger.debug('Portfolio performance calculated', {
      portfolioId,
      holdingsCount: holdings.length,
      timeSeriesLength: timeSeries.length,
      hasBenchmark: benchmarkResult.benchmark !== null,
      warningsCount: warnings.length,
    });

    return {
      portfolioId: portfolio.id,
      portfolioName: portfolio.name,
      portfolioDescription: portfolio.description || undefined,
      summary,
      holdings,
      timeSeries,
      ...benchmarkResult,
      analysisDate,
      warnings,
    };
  }

  /**
   * Build empty performance result for portfolios with no holdings
   */
  private buildEmptyPerformanceResult(
    portfolio: { id: number; name: string; description?: string | null },
    analysisDate: string
  ): PortfolioPerformanceResult {
    return {
      portfolioId: portfolio.id,
      portfolioName: portfolio.name,
      portfolioDescription: portfolio.description || undefined,
      summary: { totalCost: 0, currentValue: 0, totalPnL: 0, returnRate: 0 },
      holdings: [],
      timeSeries: [],
      benchmark: null,
      benchmarkTimeSeries: null,
      analysisDate,
      dateRange: null,
      dataPoints: 0,
      warnings: ['Portfolio has no holdings'],
    };
  }

  /**
   * Collect item data with prices and historical prices
   */
  private collectItemData(
    items: Array<{
      code: string;
      companyName: string;
      quantity: number;
      purchasePrice: number;
      purchaseDate: Date;
      account?: string | null;
    }>,
    lookbackDays: number,
    warnings: string[]
  ): { itemsWithPrices: PortfolioItemWithPrice[]; stockPricesMap: Map<string, PriceTimeSeries[]> } {
    const itemsWithPrices: PortfolioItemWithPrice[] = [];
    const stockPricesMap = new Map<string, PriceTimeSeries[]>();

    for (const item of items) {
      itemsWithPrices.push(this.convertToItemWithPrice(item, warnings));

      const historicalPrices = this.getHistoricalPrices(item.code, lookbackDays);
      if (historicalPrices.length > 0) {
        stockPricesMap.set(item.code, historicalPrices);
      }
    }

    return { itemsWithPrices, stockPricesMap };
  }

  /**
   * Process benchmark data and return result object
   */
  private processBenchmarkData(
    timeSeries: PerformanceDataPoint[],
    stockPricesMap: Map<string, PriceTimeSeries[]>,
    benchmarkCode: string,
    lookbackDays: number,
    warnings: string[]
  ): {
    benchmark: BenchmarkMetrics | null;
    benchmarkTimeSeries: BenchmarkDataPoint[] | null;
    dateRange: { from: string; to: string } | null;
    dataPoints: number;
  } {
    if (timeSeries.length < 30) {
      if (stockPricesMap.size > 0) {
        warnings.push(`Insufficient time series data (${timeSeries.length} days, minimum 30 required)`);
      }
      return { benchmark: null, benchmarkTimeSeries: null, dateRange: null, dataPoints: 0 };
    }

    const { benchmark, benchmarkTimeSeries } = this.calculateBenchmarkData(
      timeSeries,
      benchmarkCode,
      lookbackDays,
      warnings
    );

    const firstDate = timeSeries[0]?.date;
    const lastDate = timeSeries[timeSeries.length - 1]?.date;
    const dateRange = firstDate && lastDate ? { from: firstDate, to: lastDate } : null;

    return { benchmark, benchmarkTimeSeries, dateRange, dataPoints: timeSeries.length };
  }

  /**
   * Close resources
   */
  close(): void {
    if (PortfolioPerformanceService.marketDbReader) {
      PortfolioPerformanceService.marketDbReader.close();
      PortfolioPerformanceService.marketDbReader = null;
    }
  }
}
