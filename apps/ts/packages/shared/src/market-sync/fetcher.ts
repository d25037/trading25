/**
 * Market Sync - Data Fetcher
 * Specialized fetcher for date-based market-wide data retrieval.
 * Note: Pagination is now handled automatically by JQuantsClient.
 */

import { type BatchExecutor, createBatchExecutor } from '../clients/base/BatchExecutor';
import type { ApiClient } from '../dataset/api-client';
import type { StockData, StockInfo, TopixData } from '../dataset/types';

/**
 * Index data for a single date
 */
export interface IndexData {
  code: string;
  date: Date;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
}

/**
 * Date range for data fetching
 */
export interface DateRange {
  from: Date;
  to: Date;
}

/**
 * Market Data Fetcher - Optimized for date-based retrieval.
 * Pagination is handled automatically by the underlying JQuantsClient.
 */
export class MarketDataFetcher {
  private rateLimiter: BatchExecutor;

  constructor(
    private apiClient: ApiClient,
    rateLimiter?: BatchExecutor,
    private debug: boolean = false
  ) {
    this.rateLimiter = rateLimiter || createBatchExecutor();
  }

  /**
   * Transform API v2 quote response to StockData format
   */
  private transformQuotesResponse(
    quotes: Array<{
      Code: string;
      Date: string;
      O?: number | null;
      H?: number | null;
      L?: number | null;
      C?: number | null;
      Vo?: number | null;
      AdjO?: number | null;
      AdjH?: number | null;
      AdjL?: number | null;
      AdjC?: number | null;
      AdjVo?: number | null;
      AdjFactor?: number;
    }>
  ): StockData[] {
    return quotes.map((quote) => ({
      code: quote.Code,
      date: new Date(quote.Date),
      open: quote.AdjO ?? quote.O ?? 0,
      high: quote.AdjH ?? quote.H ?? 0,
      low: quote.AdjL ?? quote.L ?? 0,
      close: quote.AdjC ?? quote.C ?? 0,
      volume: quote.AdjVo ?? quote.Vo ?? 0,
      adjustmentFactor: quote.AdjFactor,
    }));
  }

  /**
   * Fetch all stock quotes for a specific date.
   * Pagination is handled automatically by JQuantsClient.
   */
  async fetchStocksByDate(date: Date): Promise<StockData[]> {
    const parts = date.toISOString().split('T');
    const dateStr = parts[0] ?? '';
    if (this.debug) console.error(`[FETCHER DEBUG] Fetching quotes for date: ${dateStr}`);

    return this.rateLimiter.execute(async () => {
      try {
        const response = await this.apiClient.client.getDailyQuotes({ date: dateStr });

        if (!response.data || response.data.length === 0) {
          if (this.debug) console.error(`[FETCHER WARN] No quotes returned for ${dateStr}`);
          return [];
        }

        const quotes = this.transformQuotesResponse(response.data);
        if (this.debug) console.error(`[FETCHER DEBUG] ✓ Fetched ${quotes.length} quotes for ${dateStr}`);
        return quotes;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch quotes for ${dateStr}:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Fetch TOPIX data for a date range
   */
  async fetchTopixRange(from: Date, to: Date): Promise<TopixData[]> {
    return this.rateLimiter.execute(async () => {
      const fromStr = from.toISOString().split('T')[0];
      const toStr = to.toISOString().split('T')[0];
      if (this.debug) console.error(`[FETCHER DEBUG] Fetching TOPIX data from ${fromStr} to ${toStr}`);

      try {
        const topixData = await this.apiClient.getTopixData({
          from,
          to,
        });

        if (this.debug) console.error(`[FETCHER DEBUG] ✓ Fetched ${topixData.length} TOPIX records`);
        return topixData;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch TOPIX data:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Fetch complete stocks list (listed_info)
   * Filters for Prime and Standard markets only
   */
  async fetchStocksList(): Promise<StockInfo[]> {
    return this.rateLimiter.execute(async () => {
      if (this.debug) console.error(`[FETCHER DEBUG] Fetching stocks list...`);

      try {
        const allStocks = await this.apiClient.getStockList();
        if (this.debug) console.error(`[FETCHER DEBUG] Received ${allStocks.length} stocks from API`);

        // Filter for Prime and Standard markets only (Growth will be skipped during insert)
        const filteredStocks = allStocks.filter(
          (stock) => stock.marketCode === 'prime' || stock.marketCode === 'standard'
        );
        if (this.debug)
          console.error(
            `[FETCHER DEBUG] ✓ Filtered to ${filteredStocks.length} Prime/Standard stocks (${allStocks.length - filteredStocks.length} Growth stocks excluded)`
          );

        return filteredStocks;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch stocks list:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Fetch data for multiple dates sequentially
   * Returns a map of date -> stock data
   */
  async fetchMultipleDatesByDate(
    dates: Date[],
    onProgress?: (completed: number, total: number, currentDate: Date) => void
  ): Promise<Map<Date, StockData[]>> {
    const results = new Map<Date, StockData[]>();

    for (let i = 0; i < dates.length; i++) {
      const date = dates[i];
      if (!date) continue;

      const data = await this.fetchStocksByDate(date);
      results.set(date, data);

      if (onProgress) {
        onProgress(i + 1, dates.length, date);
      }
    }

    return results;
  }

  /**
   * Fetch all indices data for a specific date
   * Uses the date parameter to get all indices in one call
   */
  async fetchIndicesByDate(date: Date): Promise<IndexData[]> {
    const parts = date.toISOString().split('T');
    const dateStr = parts[0] ?? '';
    if (this.debug) console.error(`[FETCHER DEBUG] Fetching indices for date: ${dateStr}`);

    return this.rateLimiter.execute(async () => {
      try {
        const response = await this.apiClient.client.getIndices({
          date: dateStr,
        });

        if (!response.data || response.data.length === 0) {
          if (this.debug) console.error(`[FETCHER WARN] No indices returned for ${dateStr}`);
          return [];
        }

        const result: IndexData[] = response.data.map((idx) => ({
          code: idx.Code,
          date: new Date(idx.Date),
          open: idx.O,
          high: idx.H,
          low: idx.L,
          close: idx.C,
        }));

        if (this.debug) console.error(`[FETCHER DEBUG] ✓ Fetched ${result.length} indices for ${dateStr}`);
        return result;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch indices for ${dateStr}:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Fetch indices data for a date range.
   * Pagination is handled automatically by JQuantsClient.
   */
  async fetchIndicesRange(from: Date, to: Date): Promise<IndexData[]> {
    const fromStr = from.toISOString().split('T')[0] ?? '';
    const toStr = to.toISOString().split('T')[0] ?? '';
    if (this.debug) console.error(`[FETCHER DEBUG] Fetching indices from ${fromStr} to ${toStr}`);

    return this.rateLimiter.execute(async () => {
      try {
        const response = await this.apiClient.client.getIndices({ from: fromStr, to: toStr });

        if (!response.data || response.data.length === 0) {
          if (this.debug) console.error(`[FETCHER WARN] No indices returned for range ${fromStr} to ${toStr}`);
          return [];
        }

        const result: IndexData[] = response.data.map((idx) => ({
          code: idx.Code,
          date: new Date(idx.Date),
          open: idx.O,
          high: idx.H,
          low: idx.L,
          close: idx.C,
        }));

        if (this.debug) console.error(`[FETCHER DEBUG] ✓ Fetched ${result.length} index records`);
        return result;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch indices range:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Fetch all historical data for a specific index code.
   * Pagination is handled automatically by JQuantsClient.
   */
  async fetchIndicesByCode(code: string, from?: Date, to?: Date): Promise<IndexData[]> {
    if (this.debug) console.error(`[FETCHER DEBUG] Fetching index ${code}...`);

    return this.rateLimiter.execute(async () => {
      try {
        const response = await this.apiClient.client.getIndices({
          code,
          from: from?.toISOString().split('T')[0],
          to: to?.toISOString().split('T')[0],
        });

        if (!response.data || response.data.length === 0) {
          if (this.debug) console.error(`[FETCHER WARN] No data returned for index ${code}`);
          return [];
        }

        const result: IndexData[] = response.data.map((idx) => ({
          code: idx.Code,
          date: new Date(idx.Date),
          open: idx.O,
          high: idx.H,
          low: idx.L,
          close: idx.C,
        }));

        if (this.debug) console.error(`[FETCHER DEBUG] ✓ Fetched ${result.length} records for index ${code}`);
        return result;
      } catch (error) {
        console.error(
          `[FETCHER ERROR] Failed to fetch index ${code}:`,
          error instanceof Error ? error.message : String(error)
        );
        throw error;
      }
    });
  }

  /**
   * Get rate limiter statistics
   */
  getBatchExecutorStats() {
    return this.rateLimiter.getStats();
  }
}
