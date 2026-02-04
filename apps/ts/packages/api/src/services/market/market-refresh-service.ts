import { createBatchExecutor } from '@trading25/shared/clients/base/BatchExecutor';
import { ApiClient } from '@trading25/shared/dataset/api-client';
import { MarketDatabase, StockHistoryRefetcher } from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { MarketRefreshResponse } from '../../schemas/market-refresh';
import { createJQuantsClient } from '../../utils/jquants-client-factory';

/**
 * Service for refreshing stock historical data
 */
export class MarketRefreshService {
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

  /**
   * Refresh historical data for specified stocks
   */
  async refreshStocks(codes: string[]): Promise<MarketRefreshResponse> {
    logger.debug('Starting stock refresh', { codes, count: codes.length });

    const db = this.getDatabase();

    // Initialize API client and refetcher
    const jquantsClient = createJQuantsClient();
    const apiClient = new ApiClient(jquantsClient);
    const rateLimiter = createBatchExecutor();
    const refetcher = new StockHistoryRefetcher(db, apiClient, rateLimiter);

    try {
      const result = await refetcher.refetchMultipleStocks(codes);

      logger.info('Stock refresh completed', {
        totalStocks: result.totalStocks,
        successCount: result.successCount,
        failedCount: result.failedCount,
        totalRecordsStored: result.totalRecordsStored,
      });

      return {
        totalStocks: result.totalStocks,
        successCount: result.successCount,
        failedCount: result.failedCount,
        totalApiCalls: result.totalApiCalls,
        totalRecordsStored: result.totalRecordsStored,
        results: result.results.map((r) => ({
          code: r.code,
          success: r.success,
          recordsFetched: r.recordsFetched,
          recordsStored: r.recordsStored,
          error: r.error,
        })),
        errors: result.errors,
        lastUpdated: new Date().toISOString(),
      };
    } catch (error) {
      logger.error('Stock refresh failed', {
        codes,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  /**
   * Close database connection
   */
  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
