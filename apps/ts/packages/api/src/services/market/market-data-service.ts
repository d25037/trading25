/**
 * Market Data Service
 * Provides data access for market.db
 * Used by Python API clients (trading25-bt)
 */
import * as fs from 'node:fs';
import { DrizzleMarketDataReader } from '@trading25/shared/db';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type {
  MarketDateRangeQuery,
  MarketOHLCRecord,
  MarketOHLCVRecord,
  MarketStockData,
  MarketStocksQuery,
  StockInfo,
} from '../../schemas/market-data';

/**
 * Format date to YYYY-MM-DD string
 */
function formatDate(date: Date): string {
  return date.toISOString().split('T')[0] ?? '';
}

/**
 * Parse date string to Date object with validation
 */
function parseDate(dateStr: string | undefined): Date | undefined {
  if (!dateStr) return undefined;
  const date = new Date(dateStr);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date string: ${dateStr}`);
  }
  return date;
}

/**
 * Build and validate date range from query parameters
 */
function buildDateRange(query: {
  start_date?: string;
  end_date?: string;
}): { from: Date; to: Date } | undefined {
  if (!query.start_date && !query.end_date) {
    return undefined;
  }

  const from = parseDate(query.start_date) ?? new Date('1900-01-01');
  const to = parseDate(query.end_date) ?? new Date();

  if (from > to) {
    logger.warn('Invalid date range: start_date is after end_date', {
      start_date: query.start_date,
      end_date: query.end_date,
    });
  }

  return { from, to };
}

/**
 * Get market database reader (lazy singleton)
 */
let marketDbReaderInstance: DrizzleMarketDataReader | null = null;

function getReader(): DrizzleMarketDataReader | null {
  if (marketDbReaderInstance !== null) {
    return marketDbReaderInstance;
  }

  const dbPath = getMarketDbPath();
  if (!fs.existsSync(dbPath)) {
    logger.error('Market database not found', { dbPath });
    return null;
  }

  try {
    marketDbReaderInstance = new DrizzleMarketDataReader(dbPath);
    logger.info('Market database reader initialized', { dbPath });
    return marketDbReaderInstance;
  } catch (error) {
    logger.error('Failed to initialize market database reader', {
      dbPath,
      error: error instanceof Error ? error.message : error,
    });
    return null;
  }
}

/**
 * Get market code list from query parameter
 */
function getMarketCodes(market: 'prime' | 'standard'): string[] {
  return [market];
}

/**
 * Market Data Service
 */
export const marketDataService = {
  /**
   * Normalize stock code to 5-digit format for market.db lookup
   * market.db uses 5-digit codes with trailing "0" (e.g., "72030" for Toyota "7203")
   */
  normalizeStockCode(stockCode: string): string {
    return stockCode.length === 4 ? `${stockCode}0` : stockCode;
  },

  /**
   * Get single stock info by code
   */
  getStockInfo(stockCode: string): StockInfo | null {
    const reader = getReader();
    if (!reader) return null;

    try {
      const dbSymbol = this.normalizeStockCode(stockCode);
      const stockInfo = reader.getStockByCode(dbSymbol);
      if (!stockInfo) return null;

      return {
        code: stockInfo.code,
        companyName: stockInfo.companyName,
        companyNameEnglish: stockInfo.companyNameEnglish,
        marketCode: stockInfo.marketCode,
        marketName: stockInfo.marketName,
        sector17Code: stockInfo.sector17Code,
        sector17Name: stockInfo.sector17Name,
        sector33Code: stockInfo.sector33Code,
        sector33Name: stockInfo.sector33Name,
        scaleCategory: stockInfo.scaleCategory,
        listedDate: formatDate(stockInfo.listedDate),
      };
    } catch (error) {
      logger.error('Failed to get stock info from market.db', { stockCode, error });
      return null;
    }
  },

  /**
   * Get stock OHLCV data
   */
  getStockOHLCV(stockCode: string, query: MarketDateRangeQuery): MarketOHLCVRecord[] | null {
    const reader = getReader();
    if (!reader) return null;

    try {
      const dbSymbol = this.normalizeStockCode(stockCode);
      const dateRange = buildDateRange(query);

      const data = reader.getStockData(dbSymbol, dateRange);

      return data.map((d) => ({
        date: formatDate(d.date),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
        volume: d.volume,
      }));
    } catch (error) {
      logger.error('Failed to get stock OHLCV from market.db', { stockCode, error });
      return null;
    }
  },

  /**
   * Get all stocks data for screening
   * Returns stock list with OHLCV history for specified period
   */
  getAllStocks(query: MarketStocksQuery): MarketStockData[] | null {
    const reader = getReader();
    if (!reader) return null;

    try {
      const marketCodes = getMarketCodes(query.market);
      const stocks = reader.getStockList(marketCodes);

      // Calculate date range based on history_days
      const latestDate = reader.getLatestTradingDate();
      if (!latestDate) {
        logger.warn('No trading data found in market.db');
        return [];
      }

      const startDate = reader.getTradingDateBefore(latestDate, query.history_days);
      const dateRange = startDate
        ? { from: startDate, to: latestDate }
        : { from: new Date('1900-01-01'), to: latestDate };

      const result: MarketStockData[] = [];

      for (const stock of stocks) {
        const stockData = reader.getStockData(stock.code, dateRange);
        if (stockData.length > 0) {
          result.push({
            code: stock.code,
            company_name: stock.companyName,
            data: stockData.map((d) => ({
              date: formatDate(d.date),
              open: d.open,
              high: d.high,
              low: d.low,
              close: d.close,
              volume: d.volume,
            })),
          });
        }
      }

      logger.debug('Retrieved all stocks data from market.db', {
        market: query.market,
        stockCount: result.length,
        historyDays: query.history_days,
      });

      return result;
    } catch (error) {
      logger.error('Failed to get all stocks from market.db', { error });
      return null;
    }
  },

  /**
   * Get TOPIX data
   */
  getTopix(query: MarketDateRangeQuery): MarketOHLCRecord[] | null {
    const reader = getReader();
    if (!reader) return null;

    try {
      const dateRange = buildDateRange(query);

      const data = reader.getTopixData(dateRange);

      return data.map((d) => ({
        date: formatDate(d.date),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }));
    } catch (error) {
      logger.error('Failed to get TOPIX data from market.db', { error });
      return null;
    }
  },
};
