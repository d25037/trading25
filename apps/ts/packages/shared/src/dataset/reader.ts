/**
 * Dataset Reader
 * Unified data access interface for reading dataset information
 */

import { DATASET_METADATA_KEYS, DrizzleDatasetDatabase } from '../db';
import { getErrorMessage } from '../errors';
import { logger } from '../utils/logger';
import type {
  DatasetStats,
  DateRange,
  MarginData,
  SectorData,
  StatementsData,
  StatementsFieldCoverage,
  StockData,
  StockInfo,
  TopixData,
} from './types';

/**
 * Unified dataset reader for accessing all data types
 */
export class DatasetReader {
  private database: DrizzleDatasetDatabase | null = null;

  constructor(private databasePath: string) {}

  /**
   * Initialize the database connection
   */
  private async ensureDatabase(): Promise<DrizzleDatasetDatabase> {
    if (!this.database) {
      this.database = new DrizzleDatasetDatabase(this.databasePath);
    }
    return this.database;
  }

  // ===== STOCK INFORMATION =====

  /**
   * Get list of all stocks in the dataset
   */
  async getStockList(marketCodes?: string[]): Promise<StockInfo[]> {
    const db = await this.ensureDatabase();
    return db.getStockList(marketCodes);
  }

  /**
   * Get stock information for specific codes
   */
  async getStockInfo(stockCodes: string[]): Promise<StockInfo[]> {
    const allStocks = await this.getStockList();
    return allStocks.filter((stock) => stockCodes.includes(stock.code));
  }

  /**
   * Search stocks by company name
   */
  async searchStocksByName(searchTerm: string): Promise<StockInfo[]> {
    const allStocks = await this.getStockList();
    const term = searchTerm.toLowerCase();

    return allStocks.filter(
      (stock) => stock.companyName.toLowerCase().includes(term) || stock.companyNameEnglish.toLowerCase().includes(term)
    );
  }

  /**
   * Get stocks by market type
   */
  async getStocksByMarket(marketCodes: string[]): Promise<StockInfo[]> {
    return this.getStockList(marketCodes);
  }

  /**
   * Get stocks by sector
   */
  async getStocksBySector(sectorCode: string): Promise<StockInfo[]> {
    const allStocks = await this.getStockList();
    return allStocks.filter((stock) => stock.sector33Code === sectorCode);
  }

  // ===== STOCK QUOTES =====

  /**
   * Get stock price data
   */
  async getStockData(stockCode: string, dateRange?: DateRange): Promise<StockData[]> {
    const db = await this.ensureDatabase();
    return db.getStockData(stockCode, dateRange);
  }

  /**
   * Get latest stock prices for multiple stocks
   */
  async getLatestPrices(stockCodes: string[]): Promise<Map<string, StockData | null>> {
    const results = new Map<string, StockData | null>();

    for (const code of stockCodes) {
      try {
        const data = await this.getStockData(code);
        const latest = data.length > 0 ? data[data.length - 1] : null;
        results.set(code, latest || null);
      } catch (error) {
        logger.debug(`Failed to get latest price for ${code}`, { error: getErrorMessage(error) });
        results.set(code, null);
      }
    }

    return results;
  }

  /**
   * Get price data for multiple stocks
   */
  async getMultipleStockData(stockCodes: string[], dateRange?: DateRange): Promise<Map<string, StockData[]>> {
    const results = new Map<string, StockData[]>();

    for (const code of stockCodes) {
      try {
        const data = await this.getStockData(code, dateRange);
        results.set(code, data);
      } catch (error) {
        logger.debug(`Failed to get stock data for ${code}`, { error: getErrorMessage(error) });
        results.set(code, []);
      }
    }

    return results;
  }

  /**
   * Get price data within specific price range
   */
  async getStocksByPriceRange(
    minPrice: number,
    maxPrice: number,
    date?: Date
  ): Promise<Array<{ stock: StockInfo; price: StockData }>> {
    const stocks = await this.getStockList();
    const results: Array<{ stock: StockInfo; price: StockData }> = [];

    for (const stock of stocks) {
      try {
        const priceData = await this.getStockData(stock.code, date ? { from: date, to: date } : undefined);

        const relevantPrice = date
          ? priceData.find((p) => p.date.toDateString() === date.toDateString())
          : priceData[priceData.length - 1]; // Latest price

        if (relevantPrice && relevantPrice.close >= minPrice && relevantPrice.close <= maxPrice) {
          results.push({ stock, price: relevantPrice });
        }
      } catch (error) {
        logger.debug(`Failed to get price data for ${stock.code}`, { error: getErrorMessage(error) });
      }
    }

    return results;
  }

  // ===== MARGIN DATA =====

  /**
   * Get margin trading data
   */
  async getMarginData(stockCode: string, dateRange?: DateRange): Promise<MarginData[]> {
    const db = await this.ensureDatabase();
    return db.getMarginData(stockCode, dateRange);
  }

  // ===== STATEMENTS DATA =====

  /**
   * Get financial statements data for a specific stock
   */
  async getStatementsData(stockCode: string, dateRange?: DateRange): Promise<StatementsData[]> {
    const db = await this.ensureDatabase();
    return db.getStatementsData(stockCode, dateRange);
  }

  /**
   * Get stocks with high margin trading activity
   */
  async getHighMarginActivityStocks(
    minVolume: number = 1000000,
    dateRange?: DateRange
  ): Promise<Array<{ stock: StockInfo; marginData: MarginData[] }>> {
    const stocks = await this.getStockList();
    const results: Array<{ stock: StockInfo; marginData: MarginData[] }> = [];

    for (const stock of stocks) {
      try {
        const marginData = await this.getMarginData(stock.code, dateRange);
        const highVolumeData = marginData.filter(
          (data) => (data.longMarginVolume || 0) + (data.shortMarginVolume || 0) >= minVolume
        );

        if (highVolumeData.length > 0) {
          results.push({ stock, marginData: highVolumeData });
        }
      } catch (error) {
        logger.debug(`Failed to get margin data for ${stock.code}`, { error: getErrorMessage(error) });
      }
    }

    return results;
  }

  // ===== TOPIX DATA =====

  /**
   * Get TOPIX index data
   */
  async getTopixData(dateRange?: DateRange): Promise<TopixData[]> {
    const db = await this.ensureDatabase();
    return db.getTopixData(dateRange);
  }

  /**
   * Get latest TOPIX value
   */
  async getLatestTopix(): Promise<TopixData | null> {
    const data = await this.getTopixData();
    return data.length > 0 ? data[data.length - 1] || null : null;
  }

  // ===== SECTOR INDICES =====

  /**
   * Get sector index data
   */
  async getSectorData(sectorCode?: string, dateRange?: DateRange): Promise<SectorData[]> {
    const db = await this.ensureDatabase();
    return db.getSectorData(sectorCode, dateRange);
  }

  /**
   * Get all sector indices for a specific date
   */
  async getSectorSnapshot(date: Date): Promise<SectorData[]> {
    return this.getSectorData(undefined, { from: date, to: date });
  }

  /**
   * Get sector performance comparison
   */
  async getSectorPerformance(
    dateRange: DateRange
  ): Promise<Array<{ sector: string; startPrice: number; endPrice: number; performance: number }>> {
    const sectorData = await this.getSectorData(undefined, dateRange);

    // Group by sector
    const sectorGroups = new Map<string, SectorData[]>();
    for (const data of sectorData) {
      if (!sectorGroups.has(data.sectorCode)) {
        sectorGroups.set(data.sectorCode, []);
      }
      sectorGroups.get(data.sectorCode)?.push(data);
    }

    const performance: Array<{
      sector: string;
      startPrice: number;
      endPrice: number;
      performance: number;
    }> = [];

    for (const [, data] of sectorGroups.entries()) {
      if (data.length < 2) continue;

      // Sort by date
      data.sort((a, b) => a.date.getTime() - b.date.getTime());

      const firstData = data[0];
      const lastData = data[data.length - 1];

      if (!firstData || !lastData) continue;

      const startPrice = firstData.close;
      const endPrice = lastData.close;
      const performancePercent = ((endPrice - startPrice) / startPrice) * 100;

      performance.push({
        sector: firstData.sectorName,
        startPrice,
        endPrice,
        performance: performancePercent,
      });
    }

    // Sort by performance descending
    return performance.sort((a, b) => b.performance - a.performance);
  }

  // ===== STATISTICS AND METADATA =====

  /**
   * Get comprehensive dataset statistics
   */
  async getDatasetStats(): Promise<DatasetStats> {
    const db = await this.ensureDatabase();
    return db.getDatasetStats();
  }

  /**
   * Get date range of available data
   */
  async getDataDateRange(): Promise<DateRange | null> {
    const stats = await this.getDatasetStats();
    return stats.dateRange;
  }

  /**
   * Check if specific data types are available
   */
  async getDataAvailability(): Promise<{
    hasMarginData: boolean;
    hasTOPIXData: boolean;
    hasSectorData: boolean;
    hasStatementsData: boolean;
  }> {
    const stats = await this.getDatasetStats();
    return {
      hasMarginData: stats.hasMarginData,
      hasTOPIXData: stats.hasTOPIXData,
      hasSectorData: stats.hasSectorData,
      hasStatementsData: stats.hasStatementsData,
    };
  }

  /**
   * Get statements field coverage statistics
   * Returns the count of non-null values for each extended financial metric
   */
  async getStatementsFieldCoverage(): Promise<StatementsFieldCoverage> {
    const db = await this.ensureDatabase();
    return db.getStatementsFieldCoverage();
  }

  /**
   * Get basic dataset info
   */
  async getDatasetInfo(): Promise<{
    totalStocks: number;
    totalQuotes: number;
    markets: string[];
    dateRange: DateRange;
    databaseSize: string;
  }> {
    const stats = await this.getDatasetStats();
    const sizeInMB = (stats.databaseSize / (1024 * 1024)).toFixed(1);

    return {
      totalStocks: stats.totalStocks,
      totalQuotes: stats.totalQuotes,
      markets: stats.markets,
      dateRange: stats.dateRange,
      databaseSize: `${sizeInMB} MB`,
    };
  }

  // ===== VALIDATION METHODS =====

  /**
   * Get date gaps between TOPIX trading calendar and stock data
   * @param snapshotDate - Optional snapshot date (YYYY-MM-DD format) to exclude dates after this date
   */
  async getDateGaps(snapshotDate?: string): Promise<{ totalTradingDays: number; missingDatesCount: number }> {
    const db = await this.ensureDatabase();
    return db.getDateGaps(snapshotDate);
  }

  /**
   * Get foreign key integrity issues (orphan records referencing non-existent stocks)
   */
  async getFKIntegrityIssues(): Promise<{
    stockDataOrphans: number;
    marginDataOrphans: number;
    statementsOrphans: number;
  }> {
    const db = await this.ensureDatabase();
    return db.getFKIntegrityIssues();
  }

  /**
   * Get count of stocks without any quote data
   */
  async getOrphanStocksCount(): Promise<number> {
    const db = await this.ensureDatabase();
    return db.getOrphanStocksCount();
  }

  /**
   * Get preset name from metadata (for stock count validation)
   */
  async getPreset(): Promise<string | null> {
    const db = await this.ensureDatabase();
    return db.getPreset();
  }

  /**
   * Get creation date from metadata (for snapshot information)
   */
  async getCreatedAt(): Promise<string | null> {
    const db = await this.ensureDatabase();
    return db.getMetadata(DATASET_METADATA_KEYS.CREATED_AT);
  }

  /**
   * Get resume status showing missing data coverage
   */
  async getResumeStatus(): Promise<{
    totalStocks: number;
    missingQuotes: number;
    missingStatements: number;
    missingMargin: number;
  }> {
    const db = await this.ensureDatabase();
    return db.getResumeStatus();
  }

  // ===== UTILITY METHODS =====

  /**
   * Validate stock code exists in dataset
   */
  async validateStockCode(stockCode: string): Promise<boolean> {
    try {
      const stocks = await this.getStockList();
      return stocks.some((stock) => stock.code === stockCode);
    } catch {
      return false;
    }
  }

  /**
   * Get available date range for a specific stock
   */
  async getStockDateRange(stockCode: string): Promise<DateRange | null> {
    try {
      const data = await this.getStockData(stockCode);
      if (data.length === 0) return null;

      const dates = data.map((d) => d.date).sort((a, b) => a.getTime() - b.getTime());
      const firstDate = dates[0];
      const lastDate = dates[dates.length - 1];

      if (!firstDate || !lastDate) return null;

      return {
        from: firstDate,
        to: lastDate,
      };
    } catch {
      return null;
    }
  }

  /**
   * Test database connection
   */
  async testConnection(): Promise<boolean> {
    try {
      await this.getDatasetStats();
      return true;
    } catch {
      return false;
    }
  }

  // ===== CLEANUP =====

  /**
   * Close database connection
   */
  async close(): Promise<void> {
    if (this.database) {
      await this.database.close();
      this.database = null;
    }
  }

  /**
   * Get database file path
   */
  getDatabasePath(): string {
    return this.databasePath;
  }
}
