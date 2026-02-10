/**
 * Dataset - Unified Data Fetcher
 * Single class replacing 12 specialized fetchers
 */

import { calculatePlanConcurrency, validateJQuantsPlan } from '@trading25/clients-ts/base/BaseJQuantsClient';
import { type BatchExecutor, categorizeErrorType, createBatchExecutor } from '@trading25/clients-ts/base/BatchExecutor';
import { getAllIndexCodesExcludingTOPIX } from '../db/constants/index-master-data';
import type { ApiClient } from './api-client';
import { type StreamConfig, StreamingFetchers, StreamingUtils } from './streaming/memory-efficient-fetchers';
import type {
  DateRange,
  DebugConfig,
  MarginData,
  ProgressCallback,
  SectorData,
  StatementsData,
  StockData,
  StockInfo,
  TopixData,
} from './types';
import { ApiError, DEFAULT_DEBUG_CONFIG } from './types';

/**
 * Processing statistics interface for debug logging
 */
interface ProcessingStats {
  startTime: number;
  totalStocks: number;
  successCount: number;
  errorCount: number;
  emptyResponseCount: number;
  totalStatementsFound: number;
  apiCallTimes: number[];
  rateLimiterDelays: number[];
  errorsByType: Map<string, number>;
}

/**
 * Unified data fetcher with integrated rate limiting and progress reporting
 * Now supports both traditional batch processing and memory-efficient streaming
 */
export class DataFetcher {
  private rateLimiter: BatchExecutor;
  private streamingFetcher: StreamingFetchers;
  private debugConfig: DebugConfig;

  constructor(
    private apiClient: ApiClient,
    rateLimiter?: BatchExecutor,
    streamConfig?: Partial<StreamConfig>,
    debugConfig: DebugConfig = DEFAULT_DEBUG_CONFIG
  ) {
    this.debugConfig = debugConfig;
    this.rateLimiter = rateLimiter || createBatchExecutor();
    this.streamingFetcher = new StreamingFetchers(apiClient, this.rateLimiter, streamConfig);
  }

  // ===== STOCK LIST =====

  /**
   * Fetch complete stock list
   */
  async fetchStockList(onProgress?: ProgressCallback): Promise<StockInfo[]> {
    try {
      onProgress?.({
        stage: 'stocks',
        processed: 0,
        total: 1,
        currentItem: 'Fetching stock list',
        errors: [],
      });

      const stocks = await this.rateLimiter.execute(() => this.apiClient.getStockList());

      onProgress?.({
        stage: 'stocks',
        processed: 1,
        total: 1,
        currentItem: `Fetched ${stocks.length} stocks`,
        errors: [],
      });

      return stocks;
    } catch (error) {
      throw new ApiError(
        'Failed to fetch stock list',
        'FETCH_STOCKS_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== STOCK QUOTES =====

  /**
   * Fetch quotes for a single stock
   */
  async fetchStockQuotes(
    stockCode: string,
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): Promise<StockData[]> {
    try {
      onProgress?.({
        stage: 'quotes',
        processed: 0,
        total: 1,
        currentItem: `Fetching quotes for ${stockCode}`,
        errors: [],
      });

      const quotes = await this.rateLimiter.execute(() => this.apiClient.getStockQuotes(stockCode, dateRange));

      onProgress?.({
        stage: 'quotes',
        processed: 1,
        total: 1,
        currentItem: `Fetched ${quotes.length} quotes for ${stockCode}`,
        errors: [],
      });

      return quotes;
    } catch (error) {
      throw new ApiError(
        `Failed to fetch quotes for ${stockCode}`,
        'FETCH_QUOTES_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Fetch quotes for multiple stocks with rate limiting
   * Traditional batch processing - loads all data into memory
   * Uses concurrent processing based on JQuants plan for optimal performance
   */
  async fetchAllStockQuotes(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ): Promise<Map<string, StockData[]>> {
    const results = new Map<string, StockData[]>();
    const errors: string[] = [];

    const operations = stockCodes.map((stockCode) => async () => {
      try {
        const quotes = await this.apiClient.getStockQuotes(stockCode, dateRange);
        results.set(stockCode, quotes);
        return quotes;
      } catch (error) {
        const errorMsg = `Failed to fetch quotes for ${stockCode}: ${error}`;
        errors.push(errorMsg);
        results.set(stockCode, []); // Set empty array for failed stocks
        throw new Error(errorMsg);
      }
    });

    const plan = validateJQuantsPlan(process.env.JQUANTS_PLAN);
    const concurrency = calculatePlanConcurrency(plan);

    console.log(`[QUOTES] Starting fetch for ${stockCodes.length} stocks (concurrency: ${concurrency})`);

    try {
      await this.rateLimiter.executeAll(operations, {
        concurrency,
        signal,
        onProgress: (completed, total) => {
          const currentStockCode = completed < total ? stockCodes[completed] : stockCodes[completed - 1];
          onProgress?.({
            stage: 'quotes',
            processed: completed,
            total,
            currentItem: currentStockCode || '',
            errors,
          });
        },
      });
    } catch (error) {
      // If cancelled, re-throw to allow proper cleanup
      if (error instanceof Error && error.message === 'Operation cancelled') {
        throw error;
      }
      // Some operations failed, but continue with successful ones
    }

    return results;
  }

  /**
   * Memory-efficient streaming version of fetchAllStockQuotes
   * Recommended for large datasets (>100 stocks)
   */
  async fetchAllStockQuotesStreaming(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): Promise<Map<string, StockData[]>> {
    // Use streaming fetcher to minimize memory usage
    const stream = this.streamingFetcher.streamStockQuotes(stockCodes, dateRange, onProgress);
    return StreamingUtils.streamToMap(stream);
  }

  /**
   * Generator-based streaming quotes (most memory efficient)
   * Use for very large datasets or when processing one stock at a time
   */
  streamStockQuotes(stockCodes: string[], dateRange?: DateRange, onProgress?: ProgressCallback) {
    return this.streamingFetcher.streamStockQuotes(stockCodes, dateRange, onProgress);
  }

  /**
   * Concurrent streaming with controlled memory usage
   * Supports cancellation via AbortSignal
   */
  streamConcurrentStockQuotes(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ) {
    return this.streamingFetcher.streamConcurrentQuotes(stockCodes, dateRange, onProgress, signal);
  }

  // ===== MARGIN DATA =====

  /**
   * Fetch margin data for a single stock
   */
  async fetchMarginData(
    stockCode: string,
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): Promise<MarginData[]> {
    try {
      onProgress?.({
        stage: 'margin',
        processed: 0,
        total: 1,
        currentItem: `Fetching margin data for ${stockCode}`,
        errors: [],
      });

      const marginData = await this.rateLimiter.execute(() => this.apiClient.getMarginData(stockCode, dateRange));

      onProgress?.({
        stage: 'margin',
        processed: 1,
        total: 1,
        currentItem: `Fetched ${marginData.length} margin records for ${stockCode}`,
        errors: [],
      });

      return marginData;
    } catch (error) {
      throw new ApiError(
        `Failed to fetch margin data for ${stockCode}`,
        'FETCH_MARGIN_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Fetch margin data for multiple stocks
   * Uses concurrent processing based on JQuants plan for optimal performance
   */
  async fetchAllMarginData(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ): Promise<Map<string, MarginData[]>> {
    const results = new Map<string, MarginData[]>();
    const errors: string[] = [];

    const operations = stockCodes.map((stockCode) => async () => {
      try {
        const marginData = await this.apiClient.getMarginData(stockCode, dateRange);
        results.set(stockCode, marginData);
        return marginData;
      } catch (error) {
        const errorMsg = `Failed to fetch margin data for ${stockCode}: ${error}`;
        errors.push(errorMsg);
        results.set(stockCode, []);
        throw new Error(errorMsg);
      }
    });

    const plan = validateJQuantsPlan(process.env.JQUANTS_PLAN);
    const concurrency = calculatePlanConcurrency(plan);

    console.log(`[MARGIN] Starting fetch for ${stockCodes.length} stocks (concurrency: ${concurrency})`);

    try {
      await this.rateLimiter.executeAll(operations, {
        concurrency,
        signal,
        onProgress: (completed, total) => {
          const currentStockCode = completed < total ? stockCodes[completed] : stockCodes[completed - 1];
          onProgress?.({
            stage: 'margin',
            processed: completed,
            total,
            currentItem: currentStockCode || '',
            errors,
          });
        },
      });
    } catch (error) {
      // If cancelled, re-throw to allow proper cleanup
      if (error instanceof Error && error.message === 'Operation cancelled') {
        throw error;
      }
      // Some operations failed, but continue with successful ones
    }

    return results;
  }

  // ===== TOPIX DATA =====

  /**
   * Fetch TOPIX index data
   */
  async fetchTopixData(dateRange?: DateRange, onProgress?: ProgressCallback): Promise<TopixData[]> {
    try {
      onProgress?.({
        stage: 'topix',
        processed: 0,
        total: 1,
        currentItem: 'Fetching TOPIX data',
        errors: [],
      });

      const topixData = await this.rateLimiter.execute(() => this.apiClient.getTopixData(dateRange));

      onProgress?.({
        stage: 'topix',
        processed: 1,
        total: 1,
        currentItem: `Fetched ${topixData.length} TOPIX records`,
        errors: [],
      });

      return topixData;
    } catch (error) {
      throw new ApiError(
        'Failed to fetch TOPIX data',
        'FETCH_TOPIX_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== SECTOR INDICES =====

  /**
   * Fetch sector indices data.
   * If no sector codes provided, uses all index codes excluding TOPIX.
   */
  async fetchSectorIndices(
    sectorCodes?: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ): Promise<SectorData[]> {
    try {
      // Use default sector codes if none provided (from index-master-data.ts)
      // Excludes TOPIX (0000) which requires a separate endpoint
      const codesToFetch = sectorCodes && sectorCodes.length > 0 ? sectorCodes : getAllIndexCodesExcludingTOPIX();

      const allSectorData: SectorData[] = [];
      const errors: string[] = [];

      const operations = codesToFetch.map((sectorCode) => async () => {
        try {
          // ApiClient.getSectorIndices now properly handles pagination
          const data = await this.apiClient.getSectorIndices(sectorCode, dateRange);
          if (data.length > 0) {
            allSectorData.push(...data);
          }
          return data;
        } catch (error) {
          const errorMsg = `Failed to fetch sector ${sectorCode}: ${error}`;
          errors.push(errorMsg);
          // Don't throw - continue with other sectors
          return [];
        }
      });

      await this.rateLimiter.executeAll(operations, {
        signal,
        onProgress: (completed, total) => {
          onProgress?.({
            stage: 'sectors',
            processed: completed,
            total,
            currentItem: codesToFetch[completed - 1] || '',
            errors,
          });
        },
      });

      // Log summary if there were errors
      if (errors.length > 0) {
        console.warn(`[SECTOR FETCH] ${errors.length} errors occurred. First few:`, errors.slice(0, 5));
      }

      return allSectorData;
    } catch (error) {
      // If cancelled, re-throw to allow proper cleanup
      if (error instanceof Error && error.message === 'Operation cancelled') {
        throw error;
      }
      throw new ApiError(
        'Failed to fetch sector indices',
        'FETCH_SECTORS_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== STATEMENTS DATA =====

  /**
   * Fetch financial statements for a single stock
   */
  async fetchStatementsData(
    stockCode: string,
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): Promise<StatementsData[]> {
    try {
      onProgress?.({
        stage: 'statements',
        processed: 0,
        total: 1,
        currentItem: `Fetching statements for ${stockCode}`,
        errors: [],
      });

      const statements = await this.rateLimiter.execute(() => this.apiClient.getStatementsData(stockCode, dateRange));

      onProgress?.({
        stage: 'statements',
        processed: 1,
        total: 1,
        currentItem: `Fetched ${statements.length} statements for ${stockCode}`,
        errors: [],
      });

      return statements;
    } catch (error) {
      throw new ApiError(
        `Failed to fetch statements for ${stockCode}`,
        'FETCH_STATEMENTS_ERROR',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Fetch statements for multiple stocks with enhanced debug logging
   * Uses concurrent processing with operation-level timeout protection
   */
  async fetchAllStatementsData(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ): Promise<Map<string, StatementsData[]>> {
    const results = new Map<string, StatementsData[]>();
    const errors: string[] = [];
    const isDebugMode = this.debugConfig.enabled;

    // Enhanced tracking for debug mode
    const processingStats: ProcessingStats = {
      startTime: Date.now(),
      totalStocks: stockCodes.length,
      successCount: 0,
      errorCount: 0,
      emptyResponseCount: 0,
      totalStatementsFound: 0,
      apiCallTimes: [],
      rateLimiterDelays: [],
      errorsByType: new Map<string, number>(),
    };

    if (isDebugMode) {
      console.log(`[STATEMENTS FETCHER] ========================================`);
      console.log(`[STATEMENTS FETCHER] ENTRY POINT: Starting statements processing for ${stockCodes.length} stocks`);
      console.log(`[STATEMENTS FETCHER] Stock codes: ${stockCodes.join(', ')}`);
      console.log(
        `[STATEMENTS FETCHER] Date range: ${dateRange ? `${dateRange.from.toISOString().split('T')[0]} to ${dateRange.to.toISOString().split('T')[0]}` : 'undefined'}`
      );
      console.log(`[STATEMENTS FETCHER] Rate limiter config:`, this.rateLimiter.getStats());
      console.log(`[STATEMENTS FETCHER] Debug config:`, this.debugConfig);
      console.log(`[STATEMENTS FETCHER] ========================================`);
    }

    const operations = stockCodes.map((stockCode, index) =>
      this.createStatementsOperation(
        stockCode,
        index,
        stockCodes.length,
        dateRange,
        processingStats,
        results,
        errors,
        isDebugMode
      )
    );

    const plan = validateJQuantsPlan(process.env.JQUANTS_PLAN);
    const concurrency = calculatePlanConcurrency(plan);

    if (isDebugMode) {
      console.log(`[STATEMENTS FETCHER] Execution config: concurrency=${concurrency}`);
    }

    try {
      await this.rateLimiter.executeAll(operations, {
        concurrency,
        signal,
        onProgress: (completed, total) => {
          const currentStock = stockCodes[completed - 1] || 'stock';
          const progressPercent = Math.round((completed / total) * 100);

          if (isDebugMode && completed % 5 === 0) {
            // Log every 5th completion
            const avgTime =
              processingStats.apiCallTimes.length > 0
                ? Math.round(
                    processingStats.apiCallTimes.reduce((a, b) => a + b, 0) / processingStats.apiCallTimes.length
                  )
                : 0;
            console.log(
              `[STATEMENTS FETCHER] Progress: ${completed}/${total} (${progressPercent}%) - Avg time: ${avgTime}ms - Success: ${processingStats.successCount}, Errors: ${processingStats.errorCount}, Empty: ${processingStats.emptyResponseCount}`
            );
          }

          onProgress?.({
            stage: 'statements',
            processed: completed,
            total,
            currentItem: currentStock || '',
            errors,
          });
        },
      });
    } catch (error) {
      // If cancelled, re-throw to allow proper cleanup
      if (error instanceof Error && error.message === 'Operation cancelled') {
        throw error;
      }
      // Some operations failed, but continue with successful ones
      if (isDebugMode) {
        console.log(`[STATEMENTS FETCHER] Some operations failed, continuing with successful ones`);
      }
    }

    // Final statistics logging
    if (isDebugMode) {
      this.logStatementsProcessingSummary(processingStats);
      // Log rate limiter performance report
      console.log(this.rateLimiter.getDetailedReport());
    }

    return results;
  }

  /**
   * Create individual statements operation for a single stock
   */
  private createStatementsOperation(
    stockCode: string,
    index: number,
    totalStocks: number,
    dateRange: DateRange | undefined,
    processingStats: ProcessingStats,
    results: Map<string, StatementsData[]>,
    errors: string[],
    isDebugMode: boolean
  ): () => Promise<StatementsData[]> {
    return async () => {
      const stockStartTime = Date.now();

      try {
        if (isDebugMode) {
          console.log(`[STATEMENTS FETCHER] ================================`);
          console.log(`[STATEMENTS FETCHER] [${index + 1}/${totalStocks}] Starting fetch for ${stockCode}`);
          console.log(
            `[STATEMENTS FETCHER] Date range: ${dateRange ? `${dateRange.from.toISOString().split('T')[0]} to ${dateRange.to.toISOString().split('T')[0]}` : 'undefined'}`
          );
          console.log(`[STATEMENTS FETCHER] ================================`);
        }

        const statements = await this.apiClient.getStatementsData(stockCode, dateRange);
        const fetchTime = Date.now() - stockStartTime;

        this.updateSuccessStats(processingStats, statements, fetchTime);
        this.logSuccessResult(isDebugMode, index, totalStocks, stockCode, statements, fetchTime);

        results.set(stockCode, statements);
        return statements;
      } catch (error) {
        const fetchTime = Date.now() - stockStartTime;
        this.updateErrorStats(processingStats, error, fetchTime);
        this.logErrorResult(isDebugMode, index, totalStocks, stockCode, error, fetchTime);

        const errorMsg = `Failed to fetch statements for ${stockCode}: ${error}`;
        errors.push(errorMsg);
        results.set(stockCode, []);

        throw new Error(errorMsg);
      }
    };
  }

  /**
   * Update processing statistics for successful operations
   */
  private updateSuccessStats(stats: ProcessingStats, statements: StatementsData[], fetchTime: number): void {
    stats.apiCallTimes.push(fetchTime);
    stats.successCount++;

    if (statements.length === 0) {
      stats.emptyResponseCount++;
    } else {
      stats.totalStatementsFound += statements.length;
    }
  }

  /**
   * Update processing statistics for failed operations
   */
  private updateErrorStats(stats: ProcessingStats, error: unknown, fetchTime: number): void {
    stats.apiCallTimes.push(fetchTime);
    stats.errorCount++;

    const errorType = this.categorizeError(error);
    const currentCount = stats.errorsByType.get(errorType) || 0;
    stats.errorsByType.set(errorType, currentCount + 1);
  }

  /**
   * Log successful operation result
   */
  private logSuccessResult(
    isDebugMode: boolean,
    index: number,
    totalStocks: number,
    stockCode: string,
    statements: StatementsData[],
    fetchTime: number
  ): void {
    if (!isDebugMode) return;

    if (statements.length === 0) {
      console.log(
        `[STATEMENTS FETCHER] [${index + 1}/${totalStocks}] Empty response for ${stockCode} (${fetchTime}ms)`
      );
    } else {
      console.log(
        `[STATEMENTS FETCHER] [${index + 1}/${totalStocks}] Found ${statements.length} statements for ${stockCode} (${fetchTime}ms)`
      );
    }
  }

  /**
   * Log error operation result
   */
  private logErrorResult(
    isDebugMode: boolean,
    index: number,
    totalStocks: number,
    stockCode: string,
    error: unknown,
    fetchTime: number
  ): void {
    if (!isDebugMode) return;

    const errorType = this.categorizeError(error);
    console.log(
      `[STATEMENTS FETCHER] [${index + 1}/${totalStocks}] ERROR for ${stockCode} (${fetchTime}ms): ${errorType} - ${error}`
    );
  }

  /**
   * Categorize error types for better debugging
   * Uses shared categorization logic from BatchExecutor
   */
  private categorizeError(error: unknown): string {
    return categorizeErrorType(error);
  }

  /**
   * Log comprehensive processing summary
   */
  private logStatementsProcessingSummary(stats: ProcessingStats): void {
    const totalTime = Date.now() - stats.startTime;
    const avgApiTime =
      stats.apiCallTimes.length > 0
        ? Math.round(stats.apiCallTimes.reduce((a: number, b: number) => a + b, 0) / stats.apiCallTimes.length)
        : 0;
    const maxApiTime = stats.apiCallTimes.length > 0 ? Math.max(...stats.apiCallTimes) : 0;
    const minApiTime = stats.apiCallTimes.length > 0 ? Math.min(...stats.apiCallTimes) : 0;

    console.log(`
[STATEMENTS FETCHER] === PROCESSING SUMMARY ===
Total Processing Time: ${Math.round(totalTime / 1000)}s
Total Stocks: ${stats.totalStocks}
Successful Fetches: ${stats.successCount}
Failed Fetches: ${stats.errorCount}
Empty Responses: ${stats.emptyResponseCount}
Total Statements Found: ${stats.totalStatementsFound}

API Call Performance:
- Average API Time: ${avgApiTime}ms
- Min API Time: ${minApiTime}ms
- Max API Time: ${maxApiTime}ms
- Total API Calls: ${stats.apiCallTimes.length}

Error Breakdown:`);

    for (const [errorType, count] of stats.errorsByType.entries()) {
      console.log(`- ${errorType}: ${count}`);
    }

    const successRate = Math.round((stats.successCount / stats.totalStocks) * 100);
    const avgStatementsPerStock =
      stats.successCount > 0 ? Math.round((stats.totalStatementsFound / stats.successCount) * 10) / 10 : 0;

    console.log(`
Success Rate: ${successRate}%
Average Statements per Successful Stock: ${avgStatementsPerStock}
Processing Rate: ${Math.round(stats.totalStocks / (totalTime / 1000))} stocks/second
================================================`);
  }

  // ===== UTILITIES =====

  /**
   * Get rate limiter statistics
   */
  getBatchExecutorStats() {
    return this.rateLimiter.getStats();
  }

  /**
   * Reset rate limiter
   */
  resetBatchExecutor() {
    this.rateLimiter.reset();
  }
}
