/**
 * Stock History Refetcher
 * Refetches historical data for specific stocks to update adjusted prices
 */

import { type BatchExecutor, createBatchExecutor } from '../clients/base/BatchExecutor';
import type { ApiClient } from '../dataset/api-client';
import type { StockData } from '../dataset/types';
import type { DrizzleMarketDatabase as MarketDatabase } from '../db/drizzle-market-database';

/**
 * Refetch result for a single stock
 */
export interface StockRefetchResult {
  code: string;
  success: boolean;
  recordsFetched: number;
  recordsStored: number;
  error?: string;
}

/**
 * Refetch result for multiple stocks
 */
export interface RefetchResult {
  totalStocks: number;
  successCount: number;
  failedCount: number;
  totalApiCalls: number;
  totalRecordsStored: number;
  results: StockRefetchResult[];
  errors: string[];
}

/**
 * Stock History Refetcher
 * Fetches complete historical data for specific stocks using code parameter
 * Filters data to TOPIX date range to maintain data integrity
 */
export class StockHistoryRefetcher {
  private rateLimiter: BatchExecutor;

  constructor(
    private db: MarketDatabase,
    private apiClient: ApiClient,
    rateLimiter?: BatchExecutor,
    private debug: boolean = false
  ) {
    this.rateLimiter = rateLimiter || createBatchExecutor();
  }

  /**
   * Refetch historical data for a single stock
   * Data is filtered to TOPIX date range for consistency
   */
  async refetchStockHistory(code: string): Promise<StockRefetchResult> {
    if (this.debug) console.error(`[REFETCH DEBUG] Starting refetch for stock ${code}...`);

    try {
      // Get TOPIX date range for filtering
      const topixRange = this.db.getTopixDateRange();
      if (!topixRange.min || !topixRange.max) {
        throw new Error('TOPIX date range not available. Run initial sync first.');
      }

      if (this.debug) {
        console.error(
          `[REFETCH DEBUG] TOPIX range: ${topixRange.min.toISOString().split('T')[0]} to ${topixRange.max.toISOString().split('T')[0]}`
        );
      }

      // Fetch all historical data using code parameter
      const allData = await this.fetchStockByCode(code);

      if (this.debug) {
        console.error(`[REFETCH DEBUG] Fetched ${allData.length} records for ${code} from API`);
      }

      // Filter to TOPIX date range
      const filteredData = allData.filter(
        (item) => topixRange.min && topixRange.max && item.date >= topixRange.min && item.date <= topixRange.max
      );

      if (this.debug) {
        console.error(
          `[REFETCH DEBUG] Filtered to ${filteredData.length} records within TOPIX range (${allData.length - filteredData.length} excluded)`
        );
      }

      // Store filtered data (will update existing records with ON CONFLICT DO UPDATE)
      if (filteredData.length > 0) {
        this.db.insertStockDataBulk(code, filteredData);
        this.db.markStockRefreshed(code);
      }

      if (this.debug)
        console.error(`[REFETCH DEBUG] ✓ Successfully refetched ${code}: ${filteredData.length} records stored`);

      return {
        code,
        success: true,
        recordsFetched: allData.length,
        recordsStored: filteredData.length,
      };
    } catch (error) {
      const errorMsg = `Failed to refetch ${code}: ${error instanceof Error ? error.message : String(error)}`;
      console.error(`[REFETCH ERROR] ${errorMsg}`);
      if (this.debug && error instanceof Error && error.stack) {
        console.error(`[REFETCH ERROR] Stack:`, error.stack);
      }

      return {
        code,
        success: false,
        recordsFetched: 0,
        recordsStored: 0,
        error: errorMsg,
      };
    }
  }

  /**
   * Refetch historical data for multiple stocks
   */
  async refetchMultipleStocks(
    codes: string[],
    onProgress?: (completed: number, total: number, currentCode: string) => void
  ): Promise<RefetchResult> {
    const result: RefetchResult = {
      totalStocks: codes.length,
      successCount: 0,
      failedCount: 0,
      totalApiCalls: 0,
      totalRecordsStored: 0,
      results: [],
      errors: [],
    };

    if (this.debug) console.error(`[REFETCH DEBUG] Starting batch refetch for ${codes.length} stocks...`);

    for (let i = 0; i < codes.length; i++) {
      const code = codes[i];
      if (!code) continue;

      if (this.debug) console.error(`[REFETCH DEBUG] Processing ${i + 1}/${codes.length}: ${code}`);

      const stockResult = await this.refetchStockHistory(code);
      result.results.push(stockResult);
      result.totalApiCalls++;

      if (stockResult.success) {
        result.successCount++;
        result.totalRecordsStored += stockResult.recordsStored;
      } else {
        result.failedCount++;
        if (stockResult.error) {
          result.errors.push(stockResult.error);
        }
      }

      if (onProgress) {
        onProgress(i + 1, codes.length, code);
      }
    }

    if (this.debug) {
      console.error(
        `[REFETCH DEBUG] Batch refetch completed: ${result.successCount}/${codes.length} successful, ${result.totalRecordsStored} total records stored`
      );
    }

    return result;
  }

  /**
   * Fetch all historical data for a stock using code parameter
   */
  private async fetchStockByCode(code: string): Promise<StockData[]> {
    return this.rateLimiter.execute(async () => {
      if (this.debug) console.error(`[REFETCH DEBUG] API call: getDailyQuotes({ code: '${code}' })`);

      const response = await this.apiClient.client.getDailyQuotes({ code });

      if (!response.data || response.data.length === 0) {
        if (this.debug) console.error(`[REFETCH WARN] No quotes returned for ${code}`);
        return [];
      }

      // Transform to StockData format (using v2 field names)
      const stockData = response.data.map((quote) => ({
        code: quote.Code,
        date: new Date(quote.Date),
        open: quote.AdjO ?? quote.O ?? 0,
        high: quote.AdjH ?? quote.H ?? 0,
        low: quote.AdjL ?? quote.L ?? 0,
        close: quote.AdjC ?? quote.C ?? 0,
        volume: quote.AdjVo ?? quote.Vo ?? 0,
        adjustmentFactor: quote.AdjFactor,
      }));

      if (this.debug) console.error(`[REFETCH DEBUG] ✓ Fetched ${stockData.length} quotes for ${code}`);

      return stockData;
    });
  }
}
