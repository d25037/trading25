/**
 * Dataset - Memory Efficient Streaming Fetchers
 * Generator-based streaming to optimize memory usage for large datasets
 */

import { calculatePlanConcurrency, validateJQuantsPlan } from '@trading25/clients-ts/base/BaseJQuantsClient';
import { type BatchExecutor, createBatchExecutor } from '@trading25/clients-ts/base/BatchExecutor';
import type { ApiClient } from '../api-client';
import type { DateRange, MarginData, ProgressCallback, StockData } from '../types';

/**
 * Stream result type for individual stock processing
 */
export interface StreamResult<T> {
  stockCode: string;
  data: T[];
  timestamp: Date;
  success: boolean;
  error?: string;
}

/**
 * Stream configuration options
 */
export interface StreamConfig {
  batchSize?: number; // Number of stocks to process before yielding (default: 1)
  maxConcurrency?: number; // Max concurrent API calls (default: 5)
  enableRetry?: boolean; // Enable retry on failures (default: true)
  progressInterval?: number; // Progress callback interval in ms (default: 1000)
}

const DEFAULT_STREAM_CONFIG: Required<StreamConfig> = {
  batchSize: 1,
  maxConcurrency: 5,
  enableRetry: true,
  progressInterval: 1000,
};

/**
 * Memory-efficient streaming fetchers using Generator functions
 */
export class StreamingFetchers {
  private rateLimiter: BatchExecutor;
  private config: Required<StreamConfig>;

  constructor(
    private apiClient: ApiClient,
    rateLimiter?: BatchExecutor,
    config?: Partial<StreamConfig>
  ) {
    this.rateLimiter = rateLimiter || createBatchExecutor();
    this.config = { ...DEFAULT_STREAM_CONFIG, ...config };
  }

  /**
   * Stream stock quotes one stock at a time to minimize memory usage
   */
  async *streamStockQuotes(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): AsyncGenerator<StreamResult<StockData>, void, unknown> {
    const total = stockCodes.length;
    let processed = 0;
    let lastProgressTime = Date.now();

    for (const stockCode of stockCodes) {
      try {
        // Rate-limited API call
        const quotes = await this.rateLimiter.execute(() => this.apiClient.getStockQuotes(stockCode, dateRange));

        yield {
          stockCode,
          data: quotes,
          timestamp: new Date(),
          success: true,
        };

        processed++;

        // Emit progress at regular intervals
        const now = Date.now();
        if (now - lastProgressTime >= this.config.progressInterval) {
          onProgress?.({
            stage: 'quotes',
            processed,
            total,
            currentItem: stockCode,
            errors: [],
          });
          lastProgressTime = now;
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);

        yield {
          stockCode,
          data: [],
          timestamp: new Date(),
          success: false,
          error: errorMessage,
        };

        processed++;
      }

      // Allow garbage collection between iterations
      if (processed % 10 === 0) {
        await this.sleep(1);
      }
    }

    // Final progress update
    onProgress?.({
      stage: 'quotes',
      processed,
      total,
      currentItem: 'Completed',
      errors: [],
    });
  }

  /**
   * Stream margin data with memory efficiency
   */
  async *streamMarginData(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): AsyncGenerator<StreamResult<MarginData>, void, unknown> {
    const total = stockCodes.length;
    let processed = 0;
    let lastProgressTime = Date.now();

    for (const stockCode of stockCodes) {
      try {
        const marginData = await this.rateLimiter.execute(() => this.apiClient.getMarginData(stockCode, dateRange));

        yield {
          stockCode,
          data: marginData,
          timestamp: new Date(),
          success: true,
        };

        processed++;

        // Progress reporting
        const now = Date.now();
        if (now - lastProgressTime >= this.config.progressInterval) {
          onProgress?.({
            stage: 'margin',
            processed,
            total,
            currentItem: stockCode,
            errors: [],
          });
          lastProgressTime = now;
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);

        yield {
          stockCode,
          data: [],
          timestamp: new Date(),
          success: false,
          error: errorMessage,
        };

        processed++;
      }

      // Memory management
      if (processed % 10 === 0) {
        await this.sleep(1);
      }
    }

    onProgress?.({
      stage: 'margin',
      processed,
      total,
      currentItem: 'Completed',
      errors: [],
    });
  }

  /**
   * Fetch quotes for a single stock with error handling
   */
  private async fetchStockQuotesWithErrorHandling(
    stockCode: string,
    dateRange: DateRange | undefined,
    signal: AbortSignal | undefined
  ): Promise<StreamResult<StockData>> {
    if (signal?.aborted) {
      return { stockCode, data: [], timestamp: new Date(), success: false, error: 'Operation cancelled' };
    }

    try {
      const quotes = await this.rateLimiter.execute(() => this.apiClient.getStockQuotes(stockCode, dateRange));
      return { stockCode, data: quotes, timestamp: new Date(), success: true };
    } catch (error) {
      return {
        stockCode,
        data: [],
        timestamp: new Date(),
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Convert a settled promise result to a StreamResult
   */
  private settledToStreamResult(result: PromiseSettledResult<StreamResult<StockData>>): StreamResult<StockData> {
    if (result.status === 'fulfilled') {
      return result.value;
    }
    return {
      stockCode: 'unknown',
      data: [],
      timestamp: new Date(),
      success: false,
      error: result.reason instanceof Error ? result.reason.message : String(result.reason),
    };
  }

  /**
   * Check if operation has been cancelled and throw if so
   */
  private checkCancellation(signal: AbortSignal | undefined): void {
    if (signal?.aborted) {
      throw new Error('Operation cancelled');
    }
  }

  /**
   * Wrap a promise with a timeout to prevent indefinite hangs.
   * Returns a failed StreamResult if timeout is exceeded.
   * Cleans up timer when the original promise completes first.
   */
  private withStockTimeout(
    promise: Promise<StreamResult<StockData>>,
    timeoutMs: number,
    stockCode: string
  ): Promise<StreamResult<StockData>> {
    let timeoutId: ReturnType<typeof setTimeout>;

    const timeoutPromise = new Promise<StreamResult<StockData>>((resolve) => {
      timeoutId = setTimeout(() => {
        resolve({
          stockCode,
          data: [],
          timestamp: new Date(),
          success: false,
          error: `Timeout after ${timeoutMs / 1000}s - API request hung`,
        });
      }, timeoutMs);
    });

    return Promise.race([promise, timeoutPromise]).finally(() => {
      clearTimeout(timeoutId);
    });
  }

  /**
   * Concurrent streaming with controlled memory usage
   * Processes multiple stocks concurrently but yields results one at a time
   * Uses plan-based concurrency for optimal performance within rate limits
   * Each stock request has a 2-minute timeout to prevent indefinite hangs
   */
  async *streamConcurrentQuotes(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback,
    signal?: AbortSignal
  ): AsyncGenerator<StreamResult<StockData>, void, unknown> {
    const total = stockCodes.length;
    let processed = 0;

    // Per-stock timeout: 2 minutes (accounts for pagination of up to 10 years of data)
    const STOCK_TIMEOUT_MS = 120_000;

    const plan = validateJQuantsPlan(process.env.JQUANTS_PLAN);
    const planConcurrency = calculatePlanConcurrency(plan);
    const concurrency = Math.min(planConcurrency, this.config.maxConcurrency, stockCodes.length);

    for (let i = 0; i < stockCodes.length; i += concurrency) {
      this.checkCancellation(signal);

      const chunk = stockCodes.slice(i, i + concurrency);
      // Wrap each fetch with timeout to prevent indefinite hangs
      const promises = chunk.map((stockCode) =>
        this.withStockTimeout(
          this.fetchStockQuotesWithErrorHandling(stockCode, dateRange, signal),
          STOCK_TIMEOUT_MS,
          stockCode
        )
      );
      const results = await Promise.allSettled(promises);

      for (const result of results) {
        this.checkCancellation(signal);
        yield this.settledToStreamResult(result);

        processed++;
        onProgress?.({
          stage: 'quotes',
          processed,
          total,
          currentItem: `Batch ${Math.floor(i / concurrency) + 1}/${Math.ceil(stockCodes.length / concurrency)}`,
          errors: [],
        });
      }

      await this.sleep(10);
    }
  }

  /**
   * Stream with batching for efficient database operations
   */
  async *streamBatchedQuotes(
    stockCodes: string[],
    dateRange?: DateRange,
    onProgress?: ProgressCallback
  ): AsyncGenerator<Array<StreamResult<StockData>>, void, unknown> {
    const total = stockCodes.length;
    let processed = 0;
    const batchSize = this.config.batchSize;

    for (let i = 0; i < stockCodes.length; i += batchSize) {
      const batch = stockCodes.slice(i, i + batchSize);
      const batchResults: Array<StreamResult<StockData>> = [];

      for (const stockCode of batch) {
        try {
          const quotes = await this.rateLimiter.execute(() => this.apiClient.getStockQuotes(stockCode, dateRange));

          batchResults.push({
            stockCode,
            data: quotes,
            timestamp: new Date(),
            success: true,
          });
        } catch (error) {
          batchResults.push({
            stockCode,
            data: [],
            timestamp: new Date(),
            success: false,
            error: error instanceof Error ? error.message : String(error),
          });
        }

        processed++;
      }

      yield batchResults;

      onProgress?.({
        stage: 'quotes',
        processed,
        total,
        currentItem: `Batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(stockCodes.length / batchSize)}`,
        errors: [],
      });

      // Memory management between batches
      await this.sleep(10);
    }
  }

  /**
   * Utility function for controlled delays
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get memory usage statistics (Node.js only)
   */
  getMemoryUsage(): { used: number; total: number; percentage: number } | null {
    if (typeof process !== 'undefined' && process.memoryUsage) {
      const usage = process.memoryUsage();
      const used = usage.heapUsed;
      const total = usage.heapTotal;
      return {
        used,
        total,
        percentage: (used / total) * 100,
      };
    }
    return null;
  }

  /**
   * Configure stream behavior
   */
  updateConfig(newConfig: Partial<StreamConfig>): void {
    this.config = { ...this.config, ...newConfig };
  }
}

/**
 * Collect all results from a stream into memory (use with caution)
 */
export async function collectStreamResults<T>(
  stream: AsyncGenerator<StreamResult<T>, void, unknown>
): Promise<Array<StreamResult<T>>> {
  const results: Array<StreamResult<T>> = [];

  for await (const result of stream) {
    results.push(result);
  }

  return results;
}

/**
 * Filter successful results from a stream
 */
export async function* filterSuccessfulResults<T>(
  stream: AsyncGenerator<StreamResult<T>, void, unknown>
): AsyncGenerator<StreamResult<T>, void, unknown> {
  for await (const result of stream) {
    if (result.success) {
      yield result;
    }
  }
}

/**
 * Transform stream results to Map format for backward compatibility
 */
export async function streamToMap<T>(
  stream: AsyncGenerator<StreamResult<T>, void, unknown>
): Promise<Map<string, T[]>> {
  const map = new Map<string, T[]>();

  for await (const result of stream) {
    if (result.success) {
      map.set(result.stockCode, result.data);
    }
  }

  return map;
}

/**
 * Count results and errors from a stream
 */
export async function getStreamStats<T>(
  stream: AsyncGenerator<StreamResult<T>, void, unknown>
): Promise<{ total: number; successful: number; failed: number; errors: string[] }> {
  let total = 0;
  let successful = 0;
  let failed = 0;
  const errors: string[] = [];

  for await (const result of stream) {
    total++;
    if (result.success) {
      successful++;
    } else {
      failed++;
      if (result.error) {
        errors.push(`${result.stockCode}: ${result.error}`);
      }
    }
  }

  return { total, successful, failed, errors };
}

/**
 * Streaming utilities namespace for backward compatibility
 */
export const StreamingUtils = {
  collectStreamResults,
  filterSuccessfulResults,
  streamToMap,
  getStreamStats,
};
