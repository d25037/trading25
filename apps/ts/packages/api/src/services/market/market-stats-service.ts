import * as fs from 'node:fs';
import { MarketDatabase, METADATA_KEYS } from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { MarketStatsResponse } from '../../schemas/market-stats';

/**
 * Format date to YYYY-MM-DD string
 */
function formatDate(date: Date | null): string | null {
  if (!date) return null;
  return date.toISOString().split('T')[0] || null;
}

/**
 * Format date range for response
 */
function formatDateRange(min: Date | null, max: Date | null): { min: string; max: string } | null {
  if (!min || !max) return null;
  return {
    min: formatDate(min) || '',
    max: formatDate(max) || '',
  };
}

/**
 * Get file size in bytes
 */
function getFileSize(path: string): number {
  try {
    const stats = fs.statSync(path);
    return stats.size;
  } catch {
    return 0;
  }
}

export class MarketStatsService {
  private db: MarketDatabase | null = null;
  private dbPath: string;

  constructor() {
    this.dbPath = getMarketDbPath();
  }

  private getDatabase(): MarketDatabase {
    if (!this.db) {
      this.db = new MarketDatabase(this.dbPath);
    }
    return this.db;
  }

  async getStats(): Promise<MarketStatsResponse> {
    logger.debug('Getting market database stats', { dbPath: this.dbPath });

    const db = this.getDatabase();

    // Get basic info
    const isInitialized = db.isInitialized();
    const lastSync = db.getMetadata(METADATA_KEYS.LAST_SYNC_DATE);
    const databaseSize = getFileSize(this.dbPath);

    // Get TOPIX stats
    const topixRange = db.getTopixDateRange();

    // Get stocks stats
    const stockCount = db.getStockCount();

    // Get stock data stats
    const stockDataRange = db.getStockDataDateRange();
    const averageStocksPerDay =
      stockDataRange.count > 0 && stockDataRange.dateCount > 0
        ? Math.round(stockDataRange.count / stockDataRange.dateCount)
        : 0;

    // Get indices stats
    const indexMasterCount = db.getIndexMasterCount();
    const indicesDataCount = db.getIndicesDataCount();
    const indicesRange = db.getIndicesDataRange();
    const indexCountByCategory = db.getIndexMasterCountByCategory();

    logger.debug('Stats retrieved', {
      initialized: isInitialized,
      topixCount: topixRange.count,
      stockCount: stockCount.total,
      stockDataCount: stockDataRange.count,
      indicesDataCount,
    });

    return {
      initialized: isInitialized,
      lastSync: lastSync || null,
      databaseSize,
      topix: {
        count: topixRange.count,
        dateRange: formatDateRange(topixRange.min, topixRange.max),
      },
      stocks: {
        total: stockCount.total,
        byMarket: stockCount.byMarket,
      },
      stockData: {
        count: stockDataRange.count,
        dateCount: stockDataRange.dateCount,
        dateRange: formatDateRange(stockDataRange.min, stockDataRange.max),
        averageStocksPerDay,
      },
      indices: {
        masterCount: indexMasterCount,
        dataCount: indicesDataCount,
        dateCount: indicesRange.count,
        dateRange: formatDateRange(indicesRange.min, indicesRange.max),
        byCategory: indexCountByCategory,
      },
      lastUpdated: new Date().toISOString(),
    };
  }

  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
