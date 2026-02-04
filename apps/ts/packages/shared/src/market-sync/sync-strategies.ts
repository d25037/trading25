/**
 * Market Sync - Sync Strategies
 * Initial sync and incremental update strategies
 */

import type { ApiClient } from '../dataset/api-client';
import type { StockData } from '../dataset/types';
import { getAllIndexCodes } from '../db/constants/index-master-data';
import type { DrizzleMarketDatabase as MarketDatabase } from '../db/drizzle-market-database';
import { METADATA_KEYS } from '../db/drizzle-market-database';
import type { MarketDataFetcher } from './fetcher';
import type { StockHistoryRefetcher } from './stock-history-refetcher';

/**
 * Progress callback for sync operations
 */
export type SyncProgressCallback = (stage: string, current: number, total: number, message: string) => void;

/**
 * Sync result interface
 */
export interface SyncResult {
  success: boolean;
  totalApiCalls: number;
  stocksUpdated: number;
  datesProcessed: number;
  failedDates: Date[];
  errors: string[];
}

/**
 * Initial Sync Strategy - First time setup with 2 years of data
 */
export class InitialSyncStrategy {
  private static readonly MAX_CONSECUTIVE_FAILURES = 5;

  constructor(
    private db: MarketDatabase,
    _apiClient: ApiClient,
    private fetcher: MarketDataFetcher,
    private debug: boolean = false
  ) {}

  async execute(onProgress?: SyncProgressCallback): Promise<SyncResult> {
    const result: SyncAccumulator = {
      totalApiCalls: 0,
      stocksUpdated: 0,
      datesProcessed: 0,
      failedDates: [],
      errors: [],
    };
    let consecutiveFailures = 0;

    try {
      const tradingDays = await this.fetchAndStoreTOPIX(result, onProgress);
      await this.fetchAndStoreStocksList(result, onProgress);
      consecutiveFailures = await this.fetchStockDataForTradingDays(result, tradingDays, onProgress);
      await this.fetchAndStoreIndices(result, tradingDays, onProgress);
      this.finalizeInitialization(result, consecutiveFailures);

      const success = result.errors.length === 0 && consecutiveFailures < InitialSyncStrategy.MAX_CONSECUTIVE_FAILURES;
      return {
        success,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: result.stocksUpdated,
        datesProcessed: result.datesProcessed,
        failedDates: result.failedDates,
        errors: result.errors,
      };
    } catch (error) {
      const errorMsg = `Initial sync failed: ${error instanceof Error ? error.message : String(error)}`;
      result.errors.push(errorMsg);
      console.error(`[SYNC ERROR] Fatal: ${errorMsg}`);
      if (this.debug && error instanceof Error && error.stack) {
        console.error(`[SYNC ERROR] Stack: ${error.stack}`);
      }

      return {
        success: false,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: result.stocksUpdated,
        datesProcessed: result.datesProcessed,
        failedDates: result.failedDates,
        errors: result.errors,
      };
    }
  }

  /**
   * Step 1: Fetch TOPIX data for 2 years and extract trading days
   */
  private async fetchAndStoreTOPIX(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<Date[]> {
    if (this.debug) console.error('[SYNC DEBUG] Step 1/5: Fetching TOPIX data for 2 years...');
    onProgress?.('topix', 0, 1, 'Fetching TOPIX data for 2 years...');

    const twoYearsAgo = new Date();
    twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
    const today = new Date();

    const topixData = await this.fetcher.fetchTopixRange(twoYearsAgo, today);
    result.totalApiCalls++;

    if (topixData.length === 0) {
      throw new Error('No TOPIX data returned from API');
    }

    // Insert TOPIX data
    this.db.insertTopixData(topixData);
    if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${topixData.length} TOPIX records`);
    onProgress?.('topix', 1, 1, `Stored ${topixData.length} TOPIX records`);

    // Extract trading days from TOPIX data
    const tradingDays = topixData.map((t) => t.date).sort((a, b) => a.getTime() - b.getTime());
    if (this.debug) console.error(`[SYNC DEBUG] Extracted ${tradingDays.length} trading days`);

    return tradingDays;
  }

  /**
   * Step 2: Fetch and store stocks list
   */
  private async fetchAndStoreStocksList(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    if (this.debug) console.error('[SYNC DEBUG] Step 2/5: Fetching stocks list...');
    onProgress?.('stocks', 0, 1, 'Fetching stocks list...');

    const stocks = await this.fetcher.fetchStocksList();
    result.totalApiCalls++;

    if (stocks.length === 0) {
      throw new Error('No stocks returned from API');
    }

    this.db.updateStocksList(stocks);
    result.stocksUpdated = stocks.length;
    if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${stocks.length} stocks (Prime + Standard)`);
    onProgress?.('stocks', 1, 1, `Stored ${stocks.length} stocks (Prime + Standard)`);
  }

  /**
   * Step 3: Fetch stock data for all trading days
   */
  private async fetchStockDataForTradingDays(
    result: SyncAccumulator,
    tradingDays: Date[],
    onProgress?: SyncProgressCallback
  ): Promise<number> {
    if (this.debug)
      console.error(`[SYNC DEBUG] Step 3/5: Fetching daily quotes for ${tradingDays.length} trading days...`);
    onProgress?.('quotes', 0, tradingDays.length, 'Fetching daily quotes...');

    if (this.debug) console.error(`[SYNC DEBUG] ==================== Starting quotes fetch loop ====================`);

    return await this.processStockDataLoop(result, tradingDays, onProgress);
  }

  /**
   * Handle successful stock data fetch
   */
  private handleSuccessfulFetch(date: Date, stockData: StockData[], result: SyncAccumulator, dateStr: string): number {
    if (stockData.length > 0) {
      this.db.insertStockDataForDate(date, stockData);
      result.datesProcessed++;
      if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${stockData.length} quotes for ${dateStr}`);
      return 0; // Reset consecutive failures
    }

    console.error(`[SYNC WARN] No quotes returned for ${dateStr}, skipping...`);
    return -1; // Don't reset, but don't increment
  }

  /**
   * Handle failed stock data fetch
   */
  private handleFailedFetch(
    date: Date,
    error: unknown,
    result: SyncAccumulator,
    consecutiveFailures: number,
    dateStr: string
  ): number {
    if (this.debug) console.error(`[SYNC DEBUG] !!! Exception caught for ${dateStr}`);

    const errorMsg = `Failed to fetch data for ${dateStr}: ${error instanceof Error ? error.message : String(error)}`;
    result.errors.push(errorMsg);
    result.failedDates.push(date);
    this.db.recordFailedDate(date);

    const newFailureCount = consecutiveFailures + 1;
    console.error(`[SYNC ERROR] ${errorMsg} (consecutive failures: ${newFailureCount})`);

    if (this.debug && error instanceof Error && error.stack) {
      console.error(`[SYNC ERROR] Stack trace:`, error.stack);
    }

    return newFailureCount;
  }

  /**
   * Process a single trading day
   */
  private async processSingleTradingDay(
    date: Date,
    index: number,
    totalDays: number,
    result: SyncAccumulator,
    consecutiveFailures: number,
    onProgress?: SyncProgressCallback
  ): Promise<number> {
    const parts = date.toISOString().split('T');
    const dateStr = parts[0] ?? '';
    if (this.debug) console.error(`[SYNC DEBUG] Loop iteration ${index + 1}/${totalDays}: Processing date ${dateStr}`);

    try {
      if (this.debug) console.error(`[SYNC DEBUG] >>> About to call fetchStocksByDate(${dateStr})...`);
      const stockData = await this.fetcher.fetchStocksByDate(date);
      if (this.debug)
        console.error(`[SYNC DEBUG] <<< fetchStocksByDate returned ${stockData.length} items for ${dateStr}`);
      result.totalApiCalls++;

      const newFailures = this.handleSuccessfulFetch(date, stockData, result, dateStr);

      onProgress?.(
        'quotes',
        index + 1,
        totalDays,
        `Fetched ${stockData.length} quotes for ${dateStr} (${index + 1}/${totalDays})`
      );

      return newFailures === 0 ? 0 : consecutiveFailures;
    } catch (error) {
      const newFailures = this.handleFailedFetch(date, error, result, consecutiveFailures, dateStr);

      onProgress?.('quotes', index + 1, totalDays, `Error on ${dateStr}, will retry later (${index + 1}/${totalDays})`);

      return newFailures;
    }
  }

  /**
   * Process stock data fetch loop with early termination
   */
  private async processStockDataLoop(
    result: SyncAccumulator,
    tradingDays: Date[],
    onProgress?: SyncProgressCallback
  ): Promise<number> {
    let consecutiveFailures = 0;

    for (let i = 0; i < tradingDays.length; i++) {
      const date = tradingDays[i];
      if (!date) {
        if (this.debug) console.error(`[SYNC WARN] Skipping undefined date at index ${i}`);
        continue;
      }

      consecutiveFailures = await this.processSingleTradingDay(
        date,
        i,
        tradingDays.length,
        result,
        consecutiveFailures,
        onProgress
      );

      // Early termination check
      if (this.checkEarlyTermination(consecutiveFailures, result)) {
        break;
      }
    }

    return consecutiveFailures;
  }

  /**
   * Check if early termination is needed
   */
  private checkEarlyTermination(consecutiveFailures: number, result: SyncAccumulator): boolean {
    if (consecutiveFailures >= InitialSyncStrategy.MAX_CONSECUTIVE_FAILURES) {
      const earlyTermMsg = `Stopping sync: ${InitialSyncStrategy.MAX_CONSECUTIVE_FAILURES} consecutive failures detected. This likely indicates an API or configuration issue.`;
      result.errors.push(earlyTermMsg);
      console.error(`[SYNC ERROR] ${earlyTermMsg}`);
      return true;
    }
    return false;
  }

  /**
   * Step 4: Fetch and store indices data using code-based fetching
   * More efficient: 52 API calls vs 250 (one per index vs one per date)
   */
  private async fetchAndStoreIndices(
    result: SyncAccumulator,
    _tradingDays: Date[],
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    if (this.debug) console.error('[SYNC DEBUG] Step 4/5: Initializing index master and fetching indices by code...');

    this.db.initializeIndexMaster();

    const indexCodes = getAllIndexCodes();
    const total = indexCodes.length;
    onProgress?.('indices', 0, total, `Fetching ${total} indices by code...`);

    if (this.debug) console.error(`[SYNC DEBUG] Fetching ${total} indices by code (more efficient than date-based)...`);

    // Get date range from TOPIX data
    const topixRange = this.db.getTopixDateRange();
    const from = topixRange.min ?? undefined;
    const to = topixRange.max ?? undefined;

    for (let i = 0; i < indexCodes.length; i++) {
      const code = indexCodes[i];
      if (code) {
        await this.fetchAndStoreSingleIndexByCode(code, i, total, from, to, result, onProgress);
      }
    }

    if (this.debug) console.error('[SYNC DEBUG] ✓ Indices sync completed');
  }

  /**
   * Fetch and store all data for a single index code
   */
  private async fetchAndStoreSingleIndexByCode(
    code: string,
    index: number,
    total: number,
    from: Date | undefined,
    to: Date | undefined,
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    try {
      const indicesData = await this.fetcher.fetchIndicesByCode(code, from, to);
      result.totalApiCalls++;

      if (indicesData.length > 0) {
        this.db.insertIndicesData(
          indicesData.map((idx) => ({
            code: idx.code,
            date: idx.date,
            open: idx.open,
            high: idx.high,
            low: idx.low,
            close: idx.close,
          }))
        );
        if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${indicesData.length} records for index ${code}`);
      }

      onProgress?.('indices', index + 1, total, `Fetched index ${code} (${index + 1}/${total})`);
    } catch (error) {
      const errorMsg = `Failed to fetch index ${code}: ${error instanceof Error ? error.message : String(error)}`;
      result.errors.push(errorMsg);
      if (this.debug) console.error(`[SYNC ERROR] ${errorMsg}`);
      onProgress?.('indices', index + 1, total, `Error on ${code}, continuing... (${index + 1}/${total})`);
    }
  }

  /**
   * Step 5: Finalize initialization
   */
  private finalizeInitialization(result: SyncAccumulator, consecutiveFailures: number): void {
    if (this.debug) console.error(`[SYNC DEBUG] Step 5/5: Finalizing sync...`);
    if (this.debug)
      console.error(
        `[SYNC DEBUG] Summary: ${result.datesProcessed} dates processed, ${result.failedDates.length} failed, ${consecutiveFailures} consecutive failures`
      );

    if (result.datesProcessed > 0 && consecutiveFailures < InitialSyncStrategy.MAX_CONSECUTIVE_FAILURES) {
      this.db.markInitialized();
      this.db.setMetadata(METADATA_KEYS.LAST_STOCKS_REFRESH, new Date().toISOString());

      // Mark existing adjustment events as processed to prevent unnecessary refetching
      const adjustmentCount = this.db.markExistingAdjustmentsAsProcessed();
      if (this.debug && adjustmentCount > 0) {
        console.error(`[SYNC DEBUG] ✓ Marked ${adjustmentCount} stocks with existing adjustments as processed`);
      }

      if (this.debug) console.error('[SYNC DEBUG] ✓ Marked as initialized');
    } else {
      console.error('[SYNC WARN] Not marking as initialized due to insufficient data or too many failures');
    }
  }
}

/**
 * Result accumulator for sync operations
 */
interface SyncAccumulator {
  totalApiCalls: number;
  stocksUpdated: number;
  datesProcessed: number;
  failedDates: Date[];
  errors: string[];
}

/**
 * TOPIX fetch result
 */
interface TopixFetchResult {
  missingDays: Date[];
  apiCalls: number;
}

/**
 * Incremental Sync Strategy - Daily updates with retry mechanism
 */
export class IncrementalSyncStrategy {
  constructor(
    private db: MarketDatabase,
    _apiClient: ApiClient,
    private fetcher: MarketDataFetcher,
    private debug: boolean = false,
    private refetcher?: StockHistoryRefetcher
  ) {}

  async execute(onProgress?: SyncProgressCallback): Promise<SyncResult> {
    const result: SyncAccumulator = {
      totalApiCalls: 0,
      stocksUpdated: 0,
      datesProcessed: 0,
      failedDates: [],
      errors: [],
    };

    try {
      const topixResult = await this.fetchAndProcessTOPIX(result, onProgress);

      // Always refresh stocks list, even if no new trading days
      await this.refreshStocksList(result, onProgress);

      // Check if indices need initialization (empty indices_data)
      const indicesNeedInit = this.checkIndicesNeedInit();

      if (topixResult.missingDays.length === 0 && topixResult.apiCalls === 1 && !indicesNeedInit) {
        // Early return only if no new stock data AND indices are already synced
        return {
          success: true,
          totalApiCalls: result.totalApiCalls,
          stocksUpdated: result.stocksUpdated,
          datesProcessed: 0,
          failedDates: [],
          errors: [],
        };
      }
      await this.processMissingDays(result, topixResult.missingDays, onProgress);
      await this.processMissingIndices(result, onProgress);
      await this.retryFailedDates(result, onProgress);
      await this.detectAndRefetchSplits(result, onProgress);
      this.finalizeSync();

      return {
        success: result.errors.length === 0,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: result.stocksUpdated,
        datesProcessed: result.datesProcessed,
        failedDates: result.failedDates,
        errors: result.errors,
      };
    } catch (error) {
      const errorMsg = `Incremental sync failed: ${error instanceof Error ? error.message : String(error)}`;
      result.errors.push(errorMsg);

      return {
        success: false,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: result.stocksUpdated,
        datesProcessed: result.datesProcessed,
        failedDates: result.failedDates,
        errors: result.errors,
      };
    }
  }

  /**
   * Step 1: Fetch TOPIX data and identify missing trading days
   */
  private async fetchAndProcessTOPIX(
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<TopixFetchResult> {
    if (this.debug) console.error('[SYNC DEBUG] Step 1: Fetching TOPIX for past 2 weeks...');
    onProgress?.('topix', 0, 1, 'Checking for new trading days...');

    const twoWeeksAgo = new Date();
    twoWeeksAgo.setDate(twoWeeksAgo.getDate() - 14);
    const today = new Date();

    const topixData = await this.fetcher.fetchTopixRange(twoWeeksAgo, today);
    result.totalApiCalls++;
    if (this.debug) console.error(`[SYNC DEBUG] ✓ Fetched ${topixData.length} TOPIX records`);

    if (topixData.length === 0) {
      onProgress?.('topix', 1, 1, 'No new TOPIX data');
      return { missingDays: [], apiCalls: 1 };
    }

    // Extract TOPIX dates and find missing trading days
    const topixDates = topixData.map((t) => t.date);
    const missingDays = this.db.getMissingTradingDays(topixDates);
    if (this.debug) console.error(`[SYNC DEBUG] Detected ${missingDays.length} missing trading days`);

    // Insert new TOPIX data
    this.db.insertTopixData(topixData);
    onProgress?.('topix', 1, 1, `Found ${missingDays.length} new trading days`);

    return { missingDays, apiCalls: 1 };
  }

  /**
   * Step 2: Refresh stocks list
   */
  private async refreshStocksList(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    if (this.debug) console.error('[SYNC DEBUG] Step 2: Refreshing stocks list...');
    onProgress?.('stocks', 0, 1, 'Refreshing stocks list...');

    const stocks = await this.fetcher.fetchStocksList();
    result.totalApiCalls++;

    this.db.updateStocksList(stocks);
    result.stocksUpdated = stocks.length;
    this.db.setMetadata(METADATA_KEYS.LAST_STOCKS_REFRESH, new Date().toISOString());
    if (this.debug) console.error(`[SYNC DEBUG] ✓ Updated ${stocks.length} stocks`);

    onProgress?.('stocks', 1, 1, `Updated ${stocks.length} stocks`);
  }

  /**
   * Handle successful fetch for missing day (incremental sync)
   */
  private handleMissingDaySuccess(date: Date, stockData: StockData[], result: SyncAccumulator, dateStr: string): void {
    if (stockData.length > 0) {
      this.db.insertStockDataForDate(date, stockData);
      result.datesProcessed++;
      this.db.clearFailedDate(date);
      if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${stockData.length} quotes for ${dateStr}`);
    }
  }

  /**
   * Handle failed fetch for missing day (incremental sync)
   */
  private handleMissingDayFailure(date: Date, error: unknown, result: SyncAccumulator, dateStr: string): void {
    const errorMsg = `Failed to fetch data for ${dateStr}: ${error instanceof Error ? error.message : String(error)}`;
    result.errors.push(errorMsg);
    result.failedDates.push(date);
    this.db.recordFailedDate(date);
  }

  /**
   * Process a single missing day
   */
  private async processSingleMissingDay(
    date: Date,
    index: number,
    totalDays: number,
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    const parts = date.toISOString().split('T');
    const dateStr = parts[0] ?? '';
    if (this.debug) console.error(`[SYNC DEBUG] Processing missing date ${dateStr} (${index + 1}/${totalDays})`);

    try {
      const stockData = await this.fetcher.fetchStocksByDate(date);
      result.totalApiCalls++;

      this.handleMissingDaySuccess(date, stockData, result, dateStr);

      onProgress?.(
        'quotes',
        index + 1,
        totalDays,
        `Fetched ${stockData.length} quotes for ${dateStr} (${index + 1}/${totalDays})`
      );
    } catch (error) {
      this.handleMissingDayFailure(date, error, result, dateStr);

      onProgress?.('quotes', index + 1, totalDays, `Error on ${dateStr}, will retry later (${index + 1}/${totalDays})`);
    }
  }

  /**
   * Step 3: Fetch stock data for missing trading days
   */
  private async processMissingDays(
    result: SyncAccumulator,
    missingDays: Date[],
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    if (missingDays.length === 0) {
      return;
    }

    if (this.debug) console.error(`[SYNC DEBUG] Step 3: Processing ${missingDays.length} missing dates...`);
    onProgress?.('quotes', 0, missingDays.length, `Fetching ${missingDays.length} missing dates...`);

    for (let i = 0; i < missingDays.length; i++) {
      const date = missingDays[i];
      if (!date) continue;

      await this.processSingleMissingDay(date, i, missingDays.length, result, onProgress);
    }
  }

  /**
   * Check if indices need initialization (empty indices_data)
   */
  private checkIndicesNeedInit(): boolean {
    this.ensureIndexMasterInitialized();
    const indicesRange = this.db.getIndicesDataRange();
    return indicesRange.count === 0;
  }

  /**
   * Step 3.5: Process missing indices data
   * Auto-detects empty indices_data and uses code-based fetching for efficiency
   */
  private async processMissingIndices(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    this.ensureIndexMasterInitialized();

    // Auto-detect: if indices_data is empty, use code-based fetching (52 API calls)
    const indicesRange = this.db.getIndicesDataRange();
    if (indicesRange.count === 0) {
      if (this.debug)
        console.error('[SYNC DEBUG] Indices data is empty, using code-based fetching (more efficient)...');
      await this.initializeIndicesByCode(result, onProgress);
      return;
    }

    // Normal incremental: fetch missing dates only
    const missingDates = this.db.getMissingIndicesDates();
    if (missingDates.length === 0) {
      if (this.debug) console.error('[SYNC DEBUG] No missing indices dates');
      return;
    }

    if (this.debug) console.error(`[SYNC DEBUG] Processing ${missingDates.length} missing indices dates...`);
    onProgress?.('indices', 0, missingDates.length, `Fetching ${missingDates.length} missing indices dates...`);

    for (let i = 0; i < missingDates.length; i++) {
      const date = missingDates[i];
      if (date) {
        await this.fetchMissingIndicesForDate(date, i, missingDates.length, result, onProgress);
      }
    }
  }

  /**
   * Initialize indices by code (used when indices_data is empty)
   * More efficient: 52 API calls vs ~250 date-based calls
   */
  private async initializeIndicesByCode(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    const indexCodes = getAllIndexCodes();
    const total = indexCodes.length;
    onProgress?.('indices', 0, total, `Initializing ${total} indices by code...`);

    // Get date range from existing TOPIX data
    const topixRange = this.db.getTopixDateRange();
    if (!topixRange.min || !topixRange.max) {
      if (this.debug) console.error('[SYNC WARN] No TOPIX data found, skipping indices initialization');
      return;
    }

    const from = topixRange.min;
    const to = topixRange.max;

    if (this.debug) {
      const fromStr = from.toISOString().split('T')[0] ?? '';
      const toStr = to.toISOString().split('T')[0] ?? '';
      console.error(`[SYNC DEBUG] Fetching ${total} indices by code (${fromStr} to ${toStr})...`);
    }

    for (let i = 0; i < indexCodes.length; i++) {
      const code = indexCodes[i];
      if (code) {
        await this.fetchSingleIndexByCode(code, i, total, from, to, result, onProgress);
      }
    }

    if (this.debug) console.error(`[SYNC DEBUG] ✓ Indices initialization completed: ${result.totalApiCalls} API calls`);
  }

  /**
   * Fetch a single index by code for initialization
   */
  private async fetchSingleIndexByCode(
    code: string,
    index: number,
    total: number,
    from: Date,
    to: Date,
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    try {
      const indicesData = await this.fetcher.fetchIndicesByCode(code, from, to);
      result.totalApiCalls++;

      if (indicesData.length > 0) {
        this.db.insertIndicesData(
          indicesData.map((idx) => ({
            code: idx.code,
            date: idx.date,
            open: idx.open,
            high: idx.high,
            low: idx.low,
            close: idx.close,
          }))
        );
        if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${indicesData.length} records for index ${code}`);
      }

      onProgress?.('indices', index + 1, total, `Fetched index ${code} (${index + 1}/${total})`);
    } catch (error) {
      const errorMsg = `Failed to fetch index ${code}: ${error instanceof Error ? error.message : String(error)}`;
      if (this.debug) console.error(`[SYNC ERROR] ${errorMsg}`);
      onProgress?.('indices', index + 1, total, `Error on ${code}, continuing... (${index + 1}/${total})`);
    }
  }

  /**
   * Ensure index master is initialized
   */
  private ensureIndexMasterInitialized(): void {
    if (!this.db.isIndexMasterInitialized()) {
      if (this.debug) console.error('[SYNC DEBUG] Initializing index master...');
      this.db.initializeIndexMaster();
    }
  }

  /**
   * Fetch missing indices for a single date
   */
  private async fetchMissingIndicesForDate(
    date: Date,
    index: number,
    total: number,
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    const dateStr = date.toISOString().split('T')[0] ?? '';

    try {
      const indicesData = await this.fetcher.fetchIndicesByDate(date);
      result.totalApiCalls++;

      if (indicesData.length > 0) {
        this.db.insertIndicesDataForDate(
          date,
          indicesData.map((idx) => ({ code: idx.code, open: idx.open, high: idx.high, low: idx.low, close: idx.close }))
        );
        if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${indicesData.length} indices for ${dateStr}`);
      }

      onProgress?.('indices', index + 1, total, `Fetched indices for ${dateStr} (${index + 1}/${total})`);
    } catch (error) {
      const errorMsg = `Failed to fetch indices for ${dateStr}: ${error instanceof Error ? error.message : String(error)}`;
      if (this.debug) console.error(`[SYNC ERROR] ${errorMsg}`);
      onProgress?.('indices', index + 1, total, `Error on ${dateStr}, continuing... (${index + 1}/${total})`);
    }
  }

  /**
   * Step 4: Retry previously failed dates (max 5)
   */
  private async retryFailedDates(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    const previouslyFailed = this.db.getFailedDates().slice(0, 5);
    if (previouslyFailed.length === 0) {
      return;
    }

    if (this.debug)
      console.error(`[SYNC DEBUG] Step 4: Retrying ${previouslyFailed.length} previously failed dates...`);
    onProgress?.('retry', 0, previouslyFailed.length, `Retrying ${previouslyFailed.length} failed dates...`);

    for (let i = 0; i < previouslyFailed.length; i++) {
      const date = previouslyFailed[i];
      if (!date) continue;

      const parts = date.toISOString().split('T');
      const dateStr = parts[0] ?? '';
      if (this.debug)
        console.error(`[SYNC DEBUG] Retrying failed date ${dateStr} (${i + 1}/${previouslyFailed.length})`);

      try {
        const stockData = await this.fetcher.fetchStocksByDate(date);
        result.totalApiCalls++;

        if (stockData.length > 0) {
          this.db.insertStockDataForDate(date, stockData);
          this.db.clearFailedDate(date);
          if (this.debug) console.error(`[SYNC DEBUG] ✓ Retry successful for ${dateStr}`);
        }

        onProgress?.(
          'retry',
          i + 1,
          previouslyFailed.length,
          `Retry success: ${dateStr} (${i + 1}/${previouslyFailed.length})`
        );
      } catch (_error) {
        onProgress?.(
          'retry',
          i + 1,
          previouslyFailed.length,
          `Retry failed: ${dateStr} (${i + 1}/${previouslyFailed.length})`
        );
        // Keep in failed list for next retry
      }
    }
  }

  /**
   * Step 5: Detect stock splits/mergers and refetch historical data
   */
  private async detectAndRefetchSplits(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    if (!this.refetcher) {
      if (this.debug) console.error('[SYNC DEBUG] Step 5: Skipping split detection (no refetcher provided)');
      return;
    }

    // Get stocks that need refresh (adjustment_factor != 1.0)
    const stocksNeedingRefresh = this.db.getStocksNeedingRefresh();

    // Filter to only stocks that actually need refetch (new splits or changed adjustment factor)
    const stocksToRefetch = stocksNeedingRefresh.filter((code) => this.db.needsRefetch(code));

    if (stocksToRefetch.length === 0) {
      if (this.debug) console.error('[SYNC DEBUG] Step 5: No new stock splits detected');
      return;
    }

    if (this.debug) {
      console.error(
        `[SYNC DEBUG] Step 5: Detected ${stocksToRefetch.length} stocks with splits/mergers requiring refetch: ${stocksToRefetch.join(', ')}`
      );
    }

    onProgress?.('refetch', 0, stocksToRefetch.length, `Refetching ${stocksToRefetch.length} stocks with splits...`);

    // Refetch historical data for detected stocks
    const refetchResult = await this.refetcher.refetchMultipleStocks(stocksToRefetch, (completed, total, code) => {
      onProgress?.('refetch', completed, total, `Refetching ${code} (${completed}/${total})`);
    });

    // Update result statistics
    result.totalApiCalls += refetchResult.totalApiCalls;
    result.errors.push(...refetchResult.errors);

    if (this.debug) {
      console.error(
        `[SYNC DEBUG] ✓ Refetch completed: ${refetchResult.successCount}/${refetchResult.totalStocks} successful, ${refetchResult.totalRecordsStored} records updated`
      );
    }

    onProgress?.(
      'refetch',
      stocksToRefetch.length,
      stocksToRefetch.length,
      `Refetch completed: ${refetchResult.successCount}/${refetchResult.totalStocks} successful`
    );
  }

  /**
   * Step 6: Finalize sync by updating last sync timestamp
   */
  private finalizeSync(): void {
    if (this.debug) console.error('[SYNC DEBUG] Step 6: Updating last sync timestamp...');
    this.db.setMetadata(METADATA_KEYS.LAST_SYNC_DATE, new Date().toISOString());
  }
}

/**
 * Indices Only Sync Strategy - Initialize indices data only
 * Used when stock_data exists but indices_data is empty
 * Efficient: 52 API calls (one per index) instead of ~250 (one per date)
 */
export class IndicesOnlySyncStrategy {
  constructor(
    private db: MarketDatabase,
    _apiClient: ApiClient,
    private fetcher: MarketDataFetcher,
    private debug: boolean = false
  ) {}

  async execute(onProgress?: SyncProgressCallback): Promise<SyncResult> {
    const result: SyncAccumulator = {
      totalApiCalls: 0,
      stocksUpdated: 0,
      datesProcessed: 0,
      failedDates: [],
      errors: [],
    };

    try {
      await this.fetchAndStoreAllIndices(result, onProgress);

      return {
        success: result.errors.length === 0,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: 0,
        datesProcessed: result.datesProcessed,
        failedDates: [],
        errors: result.errors,
      };
    } catch (error) {
      const errorMsg = `Indices sync failed: ${error instanceof Error ? error.message : String(error)}`;
      result.errors.push(errorMsg);
      console.error(`[SYNC ERROR] Fatal: ${errorMsg}`);

      return {
        success: false,
        totalApiCalls: result.totalApiCalls,
        stocksUpdated: 0,
        datesProcessed: result.datesProcessed,
        failedDates: [],
        errors: result.errors,
      };
    }
  }

  /**
   * Fetch and store all indices using code-based fetching
   */
  private async fetchAndStoreAllIndices(result: SyncAccumulator, onProgress?: SyncProgressCallback): Promise<void> {
    if (this.debug) console.error('[SYNC DEBUG] Initializing index master and fetching indices by code...');

    this.db.initializeIndexMaster();

    const indexCodes = getAllIndexCodes();
    const total = indexCodes.length;
    onProgress?.('indices', 0, total, `Fetching ${total} indices by code...`);

    // Get date range from existing TOPIX data
    const topixRange = this.db.getTopixDateRange();
    if (!topixRange.min || !topixRange.max) {
      throw new Error('No TOPIX data found. Run full sync first.');
    }

    const from = topixRange.min;
    const to = topixRange.max;

    if (this.debug) {
      const fromStr = from.toISOString().split('T')[0] ?? '';
      const toStr = to.toISOString().split('T')[0] ?? '';
      console.error(`[SYNC DEBUG] Using date range from TOPIX: ${fromStr} to ${toStr}`);
    }

    for (let i = 0; i < indexCodes.length; i++) {
      const code = indexCodes[i];
      if (code) {
        await this.fetchSingleIndex(code, i, total, from, to, result, onProgress);
      }
    }

    result.datesProcessed = topixRange.count;
    if (this.debug) console.error(`[SYNC DEBUG] ✓ Indices sync completed: ${result.totalApiCalls} API calls`);
  }

  /**
   * Fetch a single index by code
   */
  private async fetchSingleIndex(
    code: string,
    index: number,
    total: number,
    from: Date,
    to: Date,
    result: SyncAccumulator,
    onProgress?: SyncProgressCallback
  ): Promise<void> {
    try {
      const indicesData = await this.fetcher.fetchIndicesByCode(code, from, to);
      result.totalApiCalls++;

      if (indicesData.length > 0) {
        this.db.insertIndicesData(
          indicesData.map((idx) => ({
            code: idx.code,
            date: idx.date,
            open: idx.open,
            high: idx.high,
            low: idx.low,
            close: idx.close,
          }))
        );
        if (this.debug) console.error(`[SYNC DEBUG] ✓ Stored ${indicesData.length} records for index ${code}`);
      }

      onProgress?.('indices', index + 1, total, `Fetched index ${code} (${index + 1}/${total})`);
    } catch (error) {
      const errorMsg = `Failed to fetch index ${code}: ${error instanceof Error ? error.message : String(error)}`;
      result.errors.push(errorMsg);
      if (this.debug) console.error(`[SYNC ERROR] ${errorMsg}`);
      onProgress?.('indices', index + 1, total, `Error on ${code}, continuing... (${index + 1}/${total})`);
    }
  }
}
