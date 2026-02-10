/**
 * Dataset Builder
 * Single class orchestrating the entire dataset creation process
 */

import * as fs from 'node:fs';
import type { JQuantsClient } from '@trading25/clients-ts/JQuantsClient';
import { DATASET_METADATA_KEYS, DrizzleDatasetDatabase } from '../db';
import { ApiClient } from './api-client';
import { getDateRange, validateConfig } from './config';
import { DataFetcher } from './fetchers';
import { ProgressTracker } from './progress';
import type {
  BuildResult,
  DatasetConfig,
  DateRange,
  DebugConfig,
  ProgressCallback,
  StatementsData,
  StockInfo,
} from './types';
import { DatasetError, DEFAULT_DEBUG_CONFIG } from './types';
import {
  filterStocksByMarkets,
  filterStocksByScaleCategories,
  filterStocksBySector33Requirements,
  filterStocksExcludingScaleCategories,
  filterStocksExcludingSectorCodes,
  getErrorMessage,
  randomSample,
} from './utils';

/**
 * Simplified dataset builder with clear sequential workflow
 */
export class DatasetBuilder {
  private apiClient: ApiClient;
  private fetcher: DataFetcher;
  private dateRange: DateRange | undefined;
  private debugConfig: DebugConfig;
  private signal?: AbortSignal;

  constructor(
    private config: DatasetConfig,
    jquantsClient: JQuantsClient,
    debugConfig: DebugConfig = DEFAULT_DEBUG_CONFIG,
    signal?: AbortSignal
  ) {
    this.signal = signal;
    validateConfig(config);
    this.debugConfig = debugConfig;
    this.apiClient = new ApiClient(jquantsClient, debugConfig);
    this.fetcher = new DataFetcher(this.apiClient, undefined, undefined, debugConfig);
    this.dateRange = getDateRange(config);

    // Debug log configuration
    if (this.debugConfig.enabled) {
      console.log(`[DATASET BUILDER] SAMPLING CONFIG CHECK:`, config.samplingConfig);
      console.log(`[DATASET BUILDER] Configuration initialized:`, {
        outputPath: config.outputPath,
        markets: config.markets,
        maxStocks: config.maxStocks,
        includeMargin: config.includeMargin,
        includeTOPIX: config.includeTOPIX,
        includeSectorIndices: config.includeSectorIndices,
        includeStatements: config.includeStatements,
        samplingConfig: config.samplingConfig,
        dateRange: this.dateRange
          ? {
              from: this.dateRange.from.toISOString().split('T')[0],
              to: this.dateRange.to.toISOString().split('T')[0],
            }
          : 'API default (based on subscription plan)',
        debugConfig: this.debugConfig,
      });
    }
  }

  /**
   * Check if the build has been cancelled
   */
  private checkCancellation(): void {
    if (this.signal?.aborted) {
      throw new Error('Build cancelled');
    }
  }

  /**
   * Build the complete dataset
   */
  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: multi-step dataset build with progress tracking
  async build(onProgress?: ProgressCallback): Promise<BuildResult> {
    const progress = new ProgressTracker(onProgress);
    const errors: string[] = [];
    const warnings: string[] = [];
    let database: DrizzleDatasetDatabase | null = null;

    try {
      // Check for cancellation before starting
      this.checkCancellation();

      // Delete existing database files if overwrite is enabled
      if (this.config.overwrite) {
        this.deleteExistingDatabase(this.config.outputPath);
      }

      // Initialize database
      database = new DrizzleDatasetDatabase(this.config.outputPath);

      // Store snapshot metadata
      if (this.config.preset) {
        database.setMetadata(DATASET_METADATA_KEYS.PRESET, this.config.preset);
      }
      database.setMetadata(DATASET_METADATA_KEYS.CREATED_AT, new Date().toISOString());

      // Step 1: Fetch and filter stocks
      progress.startStage('stocks', 1);
      progress.setCurrentItem('Fetching stock list');

      const allStocks = await this.fetcher.fetchStockList(onProgress);
      const filteredStocks = this.filterStocks(allStocks);

      progress.updateProgress('Stock list processed');

      if (filteredStocks.length === 0) {
        throw new DatasetError('No stocks found matching the criteria');
      }

      // Store stock information
      if (process.env.DATASET_DEBUG === 'true') {
        console.log(`DEBUG: About to insert ${filteredStocks.length} stocks (maxStocks: ${this.config.maxStocks})`);
        console.log(
          `DEBUG: First 3 stocks:`,
          filteredStocks.slice(0, 3).map((s) => ({ code: s.code, companyName: s.companyName }))
        );
      }
      await database.insertStocks(filteredStocks);

      // Step 2: Fetch stock quotes
      this.checkCancellation();
      await this.processStockQuotes(filteredStocks, database, progress, errors);

      // Step 3: Process optional data sources
      if (this.config.includeMargin) {
        this.checkCancellation();
        await this.processMarginData(filteredStocks, database, progress, warnings);
      }

      if (this.config.includeTOPIX) {
        this.checkCancellation();
        await this.processTOPIXData(database, progress, warnings);
      }

      if (this.config.includeSectorIndices) {
        this.checkCancellation();
        await this.processSectorData(database, progress, warnings);
      }

      if (this.config.includeStatements) {
        this.checkCancellation();
        if (this.debugConfig.enabled) {
          console.log(
            `[DATASET BUILDER] ✅ STATEMENTS PROCESSING ENABLED - Starting statements processing for ${filteredStocks.length} stocks`
          );
        }
        await this.processStatementsData(filteredStocks, database, progress, warnings);
      } else {
        if (this.debugConfig.enabled) {
          console.log(
            `[DATASET BUILDER] ❌ STATEMENTS PROCESSING DISABLED - includeStatements: ${this.config.includeStatements}`
          );
        }
      }

      // Step 4: Finalize
      progress.startStage('saving', 1);
      progress.setCurrentItem('Finalizing database');
      progress.updateProgress('Database completed');

      return {
        success: errors.length === 0,
        totalStocks: allStocks.length,
        processedStocks: filteredStocks.length,
        errors,
        warnings,
        databasePath: this.config.outputPath,
      };
    } catch (error) {
      const errorMsg = getErrorMessage(error);
      errors.push(errorMsg);

      return {
        success: false,
        totalStocks: 0,
        processedStocks: 0,
        errors,
        warnings,
        databasePath: this.config.outputPath,
      };
    } finally {
      if (database) {
        await database.close();
      }
    }
  }

  /**
   * Convert missing stock codes to StockInfo objects using the lookup map
   */
  private getMissingStocks(missingCodes: string[], stockInfoMap: Map<string, StockInfo>): StockInfo[] {
    return missingCodes.flatMap((code) => {
      const stock = stockInfoMap.get(code);
      return stock ? [stock] : [];
    });
  }

  /**
   * Process missing per-stock data (quotes, margin, statements)
   */
  private async processResumeMissingStockData(
    database: DrizzleDatasetDatabase,
    stockInfoMap: Map<string, StockInfo>,
    progress: ProgressTracker,
    errors: string[],
    warnings: string[]
  ): Promise<void> {
    const resumeStatus = database.getResumeStatus();

    if (resumeStatus.missingQuotes > 0) {
      this.checkCancellation();
      const missingStocks = this.getMissingStocks(database.getStocksWithMissingQuotes(), stockInfoMap);
      console.log(`[RESUME] Fetching quotes for ${missingStocks.length} stocks`);
      await this.processStockQuotes(missingStocks, database, progress, errors);
    }

    if (this.config.includeMargin && resumeStatus.missingMargin > 0) {
      this.checkCancellation();
      const missingStocks = this.getMissingStocks(database.getStocksWithMissingMargin(), stockInfoMap);
      console.log(`[RESUME] Fetching margin data for ${missingStocks.length} stocks`);
      await this.processMarginData(missingStocks, database, progress, warnings);
    }

    if (this.config.includeStatements && resumeStatus.missingStatements > 0) {
      this.checkCancellation();
      const missingStocks = this.getMissingStocks(database.getStocksWithMissingStatements(), stockInfoMap);
      console.log(`[RESUME] Fetching statements for ${missingStocks.length} stocks`);
      await this.processStatementsData(missingStocks, database, progress, warnings);
    }
  }

  /**
   * Process missing index data (TOPIX, sector indices)
   */
  private async processResumeMissingIndexData(
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[]
  ): Promise<void> {
    if (this.config.includeTOPIX && !database.hasTopixData()) {
      this.checkCancellation();
      console.log(`[RESUME] Fetching TOPIX data`);
      await this.processTOPIXData(database, progress, warnings);
    }

    if (this.config.includeSectorIndices && !database.hasSectorIndicesData()) {
      this.checkCancellation();
      console.log(`[RESUME] Fetching sector indices data`);
      await this.processSectorData(database, progress, warnings);
    }
  }

  /**
   * Resume an incomplete dataset build by fetching missing data
   * Opens existing database and fetches only missing quotes/statements/margin data
   */
  async buildResume(onProgress?: ProgressCallback): Promise<BuildResult> {
    const progress = new ProgressTracker(onProgress);
    const errors: string[] = [];
    const warnings: string[] = [];
    let database: DrizzleDatasetDatabase | null = null;

    try {
      this.checkCancellation();

      if (!fs.existsSync(this.config.outputPath)) {
        throw new DatasetError(`Database not found: ${this.config.outputPath}`);
      }

      database = new DrizzleDatasetDatabase(this.config.outputPath);
      const resumeStatus = database.getResumeStatus();
      console.log(`[RESUME] Database status:`, resumeStatus);

      if (this.isResumeComplete(database, resumeStatus)) {
        console.log(`[RESUME] No missing data found, database is complete`);
        return {
          success: true,
          totalStocks: resumeStatus.totalStocks,
          processedStocks: resumeStatus.totalStocks,
          errors: [],
          warnings: ['Database already complete, nothing to resume'],
          databasePath: this.config.outputPath,
        };
      }

      const existingStocks = database.getStockList();
      const stockInfoMap = new Map<string, StockInfo>(existingStocks.map((s) => [s.code, s]));

      await this.processResumeMissingStockData(database, stockInfoMap, progress, errors, warnings);
      await this.processResumeMissingIndexData(database, progress, warnings);

      progress.startStage('saving', 1);
      progress.setCurrentItem('Finalizing database');
      progress.updateProgress('Resume completed');

      return {
        success: errors.length === 0,
        totalStocks: resumeStatus.totalStocks,
        processedStocks: resumeStatus.totalStocks,
        errors,
        warnings,
        databasePath: this.config.outputPath,
      };
    } catch (error) {
      errors.push(getErrorMessage(error));
      return {
        success: false,
        totalStocks: 0,
        processedStocks: 0,
        errors,
        warnings,
        databasePath: this.config.outputPath,
      };
    } finally {
      if (database) await database.close();
    }
  }

  /**
   * Check if resume is complete (no missing data)
   */
  private isResumeComplete(
    database: DrizzleDatasetDatabase,
    resumeStatus: ReturnType<typeof database.getResumeStatus>
  ): boolean {
    const needsQuotes = resumeStatus.missingQuotes > 0;
    const needsStatements = this.config.includeStatements && resumeStatus.missingStatements > 0;
    const needsMargin = this.config.includeMargin && resumeStatus.missingMargin > 0;
    const needsTopix = this.config.includeTOPIX && !database.hasTopixData();
    const needsSector = this.config.includeSectorIndices && !database.hasSectorIndicesData();

    return !needsQuotes && !needsStatements && !needsMargin && !needsTopix && !needsSector;
  }

  /**
   * Filter stocks based on configuration
   */
  private filterStocks(stocks: StockInfo[]): StockInfo[] {
    const isDebugMode = this.debugConfig.enabled;
    this.logInitialState(stocks, isDebugMode);

    let filtered = this.applyMarketFilters(stocks, isDebugMode);
    filtered = this.applySectorFilters(filtered, isDebugMode);
    filtered = this.applyScaleFilters(filtered, isDebugMode);
    filtered = this.applySampling(filtered, isDebugMode);

    this.logFinalState(filtered, isDebugMode);
    return filtered;
  }

  /**
   * Log initial filtering state
   */
  private logInitialState(stocks: StockInfo[], isDebugMode: boolean): void {
    if (isDebugMode) {
      console.log(`DEBUG: Starting with ${stocks.length} stocks`);
      console.log(`DEBUG: Filtering by markets: ${this.config.markets.join(', ')}`);
    }
  }

  /**
   * Apply market-based filters
   */
  private applyMarketFilters(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    const filtered = filterStocksByMarkets(stocks, this.config.markets);
    if (isDebugMode) {
      console.log(`DEBUG: After market filtering: ${filtered.length} stocks`);
    }
    return filtered;
  }

  /**
   * Apply sector-based filters
   */
  private applySectorFilters(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    const excludeEmptySector33 = this.config.excludeEmptySector33 ?? true;
    let filtered = filterStocksBySector33Requirements(stocks, excludeEmptySector33);

    if (isDebugMode) {
      console.log(
        `DEBUG: After sector33 requirements filtering (excludeEmptySector33=${excludeEmptySector33}): ${filtered.length} stocks`
      );
    }

    const excludeSectorCodes = this.config.excludeSectorCodes ?? ['9999'];
    filtered = filterStocksExcludingSectorCodes(filtered, excludeSectorCodes);

    if (isDebugMode) {
      console.log(`DEBUG: After excluding sector codes ${excludeSectorCodes.join(', ')}: ${filtered.length} stocks`);
    }

    return filtered;
  }

  /**
   * Apply scale category filters (both inclusion and exclusion)
   */
  private applyScaleFilters(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    let filtered = stocks;

    // First apply inclusion filter (if specified)
    if (this.config.scaleCategories && this.config.scaleCategories.length > 0) {
      if (isDebugMode) {
        console.log(`DEBUG: Filtering by scale categories (inclusion): ${this.config.scaleCategories.join(', ')}`);
      }
      filtered = filterStocksByScaleCategories(filtered, this.config.scaleCategories);
      if (isDebugMode) {
        console.log(`DEBUG: After scale categories inclusion filtering: ${filtered.length} stocks`);
      }
    }

    // Then apply exclusion filter (if specified)
    if (this.config.excludeScaleCategories && this.config.excludeScaleCategories.length > 0) {
      if (isDebugMode) {
        console.log(`DEBUG: Excluding scale categories: ${this.config.excludeScaleCategories.join(', ')}`);
      }
      filtered = filterStocksExcludingScaleCategories(filtered, this.config.excludeScaleCategories);
      if (isDebugMode) {
        console.log(`DEBUG: After scale categories exclusion filtering: ${filtered.length} stocks`);
      }
    }

    return filtered;
  }

  /**
   * Apply sampling configuration
   */
  private applySampling(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    if (isDebugMode) {
      console.log(`DEBUG: samplingConfig check:`, {
        hasSamplingConfig: !!this.config.samplingConfig,
        samplingConfig: this.config.samplingConfig,
        maxStocks: this.config.maxStocks,
      });
    }

    if (this.config.samplingConfig) {
      return this.applyConfiguredSampling(stocks, isDebugMode);
    }

    return this.applyMaxStocksLimit(stocks, isDebugMode);
  }

  /**
   * Apply configured sampling method
   */
  private applyConfiguredSampling(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    if (!this.config.samplingConfig) {
      return stocks;
    }

    const { method, size, seed } = this.config.samplingConfig;

    if (isDebugMode) {
      console.log(`DEBUG: Applying sampling: method=${method}, size=${size}, seed=${seed || 'none'}`);
    }

    if (method === 'random') {
      const sampled = randomSample(stocks, size, seed);
      if (isDebugMode) {
        console.log(`DEBUG: After random sampling: ${sampled.length} stocks`);
      }
      return sampled;
    }

    return stocks;
  }

  /**
   * Apply simple maxStocks limit
   */
  private applyMaxStocksLimit(stocks: StockInfo[], isDebugMode: boolean): StockInfo[] {
    if (!this.config.maxStocks || this.config.maxStocks <= 0) {
      return stocks;
    }

    if (isDebugMode) {
      console.log(`DEBUG: Applying maxStocks limit: ${this.config.maxStocks}`);
    }

    const limited = stocks.slice(0, this.config.maxStocks);

    if (isDebugMode) {
      console.log(`DEBUG: After maxStocks filtering: ${limited.length} stocks`);
    }

    return limited;
  }

  /**
   * Log final filtering state
   */
  private logFinalState(stocks: StockInfo[], isDebugMode: boolean): void {
    if (!isDebugMode || stocks.length === 0) {
      return;
    }

    console.log(`DEBUG: About to insert ${stocks.length} stocks (maxStocks: ${this.config.maxStocks || 'unlimited'})`);
    console.log(
      `DEBUG: First 3 stocks: ${JSON.stringify(
        stocks.slice(0, 3).map((s) => ({
          code: s.code,
          companyName: s.companyName,
          sector33Code: s.sector33Code,
        })),
        null,
        2
      )}`
    );
  }

  /**
   * Process stock quotes data with memory-efficient streaming
   */
  private async processStockQuotes(
    stocks: StockInfo[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    errors: string[]
  ): Promise<void> {
    progress.startStage('quotes', stocks.length);
    const stockCodes = stocks.map((s) => s.code);

    // Use streaming for large datasets to minimize memory usage
    const useStreaming = stockCodes.length > 50 || process.env.DATASET_STREAMING === 'true';

    if (useStreaming) {
      await this.processStockQuotesStreaming(stockCodes, database, progress, errors);
    } else {
      await this.processStockQuotesBatch(stockCodes, database, progress, errors);
    }
  }

  /**
   * Traditional batch processing for smaller datasets
   */
  private async processStockQuotesBatch(
    stockCodes: string[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    errors: string[]
  ): Promise<void> {
    const quotesData = await this.fetcher.fetchAllStockQuotes(
      stockCodes,
      this.dateRange,
      (progressInfo) => {
        progress.setProgress(progressInfo.processed, progressInfo.total, progressInfo.currentItem ?? '');
      },
      this.signal
    );

    // Store quotes data
    for (const [stockCode, quotes] of quotesData.entries()) {
      try {
        if (quotes.length > 0) {
          await database.insertStockData(stockCode, quotes);
        }
        progress.updateProgress(`Stored ${quotes.length} quotes for ${stockCode}`);
      } catch (error) {
        const errorMsg = `Failed to store quotes for ${stockCode}: ${getErrorMessage(error)}`;
        errors.push(errorMsg);
        progress.addError(errorMsg);
        progress.updateProgress(`Failed to store ${stockCode}`);
      }
    }
  }

  /**
   * Memory-efficient streaming processing for larger datasets
   * Uses concurrent streaming for improved throughput while maintaining memory efficiency
   */
  private async processStockQuotesStreaming(
    stockCodes: string[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    errors: string[]
  ): Promise<void> {
    // Use concurrent streaming for better performance with cancellation support
    const stream = this.fetcher.streamConcurrentStockQuotes(
      stockCodes,
      this.dateRange,
      (progressInfo) => {
        progress.setProgress(progressInfo.processed, progressInfo.total, progressInfo.currentItem ?? '');
      },
      this.signal
    );

    // Process results one at a time to minimize memory usage
    for await (const result of stream) {
      // Check for cancellation before processing each result
      this.checkCancellation();

      try {
        if (result.success && result.data.length > 0) {
          await database.insertStockData(result.stockCode, result.data);
          progress.updateProgress(`Stored ${result.data.length} quotes for ${result.stockCode}`);
        } else if (!result.success) {
          const errorMsg = `Failed to fetch quotes for ${result.stockCode}: ${result.error || 'Unknown error'}`;
          errors.push(errorMsg);
          progress.addError(errorMsg);
          progress.updateProgress(`Failed to fetch ${result.stockCode}`);
        } else {
          progress.updateProgress(`No quotes found for ${result.stockCode}`);
        }
      } catch (error) {
        const errorMsg = `Failed to store quotes for ${result.stockCode}: ${getErrorMessage(error)}`;
        errors.push(errorMsg);
        progress.addError(errorMsg);
        progress.updateProgress(`Failed to store ${result.stockCode}`);
      }

      // Allow garbage collection after each stock
      if (global.gc) {
        global.gc();
      }
    }
  }

  /**
   * Process margin data
   */
  private async processMarginData(
    stocks: StockInfo[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[]
  ): Promise<void> {
    progress.startStage('margin', stocks.length);

    try {
      const marginData = await this.fetcher.fetchAllMarginData(
        stocks.map((s) => s.code),
        this.dateRange,
        (progressInfo) => {
          progress.setProgress(progressInfo.processed, progressInfo.total, progressInfo.currentItem ?? '');
        },
        this.signal
      );

      // Store margin data
      for (const [stockCode, margin] of marginData.entries()) {
        try {
          if (margin.length > 0) {
            await database.insertMarginData(stockCode, margin);
          }
          progress.updateProgress(`Stored ${margin.length} margin records for ${stockCode}`);
        } catch (error) {
          const warningMsg = `Failed to store margin data for ${stockCode}: ${getErrorMessage(error)}`;
          warnings.push(warningMsg);
          progress.updateProgress(`Warning: margin data failed for ${stockCode}`);
        }
      }
    } catch (error) {
      const warningMsg = `Margin data processing failed: ${getErrorMessage(error)}`;
      warnings.push(warningMsg);
      progress.addError(warningMsg);
    }
  }

  /**
   * Process TOPIX data
   */
  private async processTOPIXData(
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[]
  ): Promise<void> {
    progress.startStage('topix', 1);

    console.log(
      `[TOPIX] Starting TOPIX fetch, dateRange:`,
      this.dateRange
        ? { from: this.dateRange.from.toISOString().split('T')[0], to: this.dateRange.to.toISOString().split('T')[0] }
        : 'API default'
    );

    try {
      const topixData = await this.fetcher.fetchTopixData(this.dateRange);
      console.log(`[TOPIX] Fetched ${topixData.length} records`);

      if (topixData.length > 0) {
        await database.insertTopixData(topixData);
        progress.updateProgress(`Stored ${topixData.length} TOPIX records`);
      } else {
        console.log(`[TOPIX] WARNING: No data returned from API`);
        warnings.push('No TOPIX data found for the specified date range');
        progress.updateProgress('No TOPIX data found');
      }
    } catch (error) {
      console.error(`[TOPIX] ERROR:`, error);
      const warningMsg = `TOPIX data processing failed: ${getErrorMessage(error)}`;
      warnings.push(warningMsg);
      progress.addError(warningMsg);
      progress.updateProgress('TOPIX processing failed');
    }
  }

  /**
   * Process sector indices data
   */
  private async processSectorData(
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[]
  ): Promise<void> {
    progress.startStage('sectors', 1);

    try {
      const sectorData = await this.fetcher.fetchSectorIndices(
        undefined, // Fetch all sectors
        this.dateRange,
        (progressInfo) => {
          progress.setProgress(
            progressInfo.processed,
            progressInfo.total,
            progressInfo.currentItem || 'Processing sectors'
          );
        },
        this.signal
      );

      if (sectorData.length > 0) {
        await database.insertSectorData(sectorData);
        progress.updateProgress(`Stored ${sectorData.length} sector records`);
      } else {
        warnings.push('No sector indices data found for the specified date range');
        progress.updateProgress('No sector data found');
      }
    } catch (error) {
      const warningMsg = `Sector data processing failed: ${getErrorMessage(error)}`;
      warnings.push(warningMsg);
      progress.addError(warningMsg);
      progress.updateProgress('Sector processing failed');
    }
  }

  /**
   * Store individual stock statements data with error handling
   */
  private async storeStockStatements(
    stockCode: string,
    statements: StatementsData[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[],
    isDebugMode: boolean
  ): Promise<{ stored: number; hasData: boolean }> {
    try {
      if (statements.length > 0) {
        await database.insertStatementsData(stockCode, statements);
        if (isDebugMode) {
          console.log(`DEBUG: Successfully stored ${statements.length} statements for ${stockCode}`);
        }
        progress.updateProgress(`Stored ${statements.length} statements for ${stockCode}`);
        return { stored: statements.length, hasData: true };
      }

      if (isDebugMode) {
        console.log(`DEBUG: No statements data for ${stockCode}`);
      }
      progress.updateProgress(`Stored ${statements.length} statements for ${stockCode}`);
      return { stored: 0, hasData: false };
    } catch (error) {
      const warningMsg = `Failed to store statements for ${stockCode}: ${getErrorMessage(error)}`;
      warnings.push(warningMsg);
      progress.updateProgress(`Warning: statements failed for ${stockCode}`);
      if (isDebugMode) {
        console.error(`DEBUG: Error storing statements for ${stockCode}:`, error);
      }
      return { stored: 0, hasData: false };
    }
  }

  /**
   * Process financial statements data
   */
  private async processStatementsData(
    stocks: StockInfo[],
    database: DrizzleDatasetDatabase,
    progress: ProgressTracker,
    warnings: string[]
  ): Promise<void> {
    progress.startStage('statements', stocks.length);
    const isDebugMode = this.debugConfig.enabled;

    // Set debug environment variable for downstream components
    if (isDebugMode) {
      process.env.DATASET_DEBUG = 'true';
      console.log(`[DATASET BUILDER] 🚀 Enhanced debugging enabled for statements processing`);
      console.log(`[DATASET BUILDER] Starting statements processing for ${stocks.length} stocks`);
      console.log(`[DATASET BUILDER] Stock codes:`, stocks.map((s) => s.code).join(', '));
      console.log(
        `[DATASET BUILDER] Date range:`,
        this.dateRange
          ? { from: this.dateRange.from.toISOString().split('T')[0], to: this.dateRange.to.toISOString().split('T')[0] }
          : 'API default'
      );
    }

    try {
      if (isDebugMode) {
        console.log(`[DATASET BUILDER] Calling fetcher.fetchAllStatementsData with ${stocks.length} stocks...`);
      }

      const statementsData = await this.fetcher.fetchAllStatementsData(
        stocks.map((s) => s.code),
        this.dateRange,
        (progressInfo) => {
          if (isDebugMode) {
            console.log(`[DATASET BUILDER] Progress callback:`, progressInfo);
          }
          progress.setProgress(progressInfo.processed, progressInfo.total, progressInfo.currentItem ?? '');
        },
        this.signal
      );

      if (isDebugMode) {
        console.log(`DEBUG: Fetched statements data for ${statementsData.size} stocks`);
      }

      let totalStatementsStored = 0;
      let stocksWithStatements = 0;

      // Store statements data for each stock
      for (const [stockCode, statements] of statementsData.entries()) {
        const result = await this.storeStockStatements(
          stockCode,
          statements,
          database,
          progress,
          warnings,
          isDebugMode
        );
        totalStatementsStored += result.stored;
        if (result.hasData) stocksWithStatements++;
      }

      if (isDebugMode) {
        console.log(`[DATASET BUILDER] 🎯 STATEMENTS PROCESSING SUMMARY:`);
        console.log(`[DATASET BUILDER] ✅ Total statements stored: ${totalStatementsStored}`);
        console.log(`[DATASET BUILDER] 📊 Stocks with statements: ${stocksWithStatements}/${stocks.length}`);
        console.log(`[DATASET BUILDER] 📈 Success rate: ${Math.round((stocksWithStatements / stocks.length) * 100)}%`);
        console.log(
          `[DATASET BUILDER] 💰 Average statements per stock: ${stocksWithStatements > 0 ? Math.round(totalStatementsStored / stocksWithStatements) : 0}`
        );

        // Reset debug environment variable
        process.env.DATASET_DEBUG = undefined;
      }

      progress.updateProgress(
        `Statements complete: ${totalStatementsStored} records across ${stocksWithStatements} stocks`
      );
    } catch (error) {
      const warningMsg = `Statements processing failed: ${getErrorMessage(error)}`;
      warnings.push(warningMsg);
      progress.addError(warningMsg);
      if (isDebugMode) {
        console.error(`DEBUG: Critical error in statements processing:`, error);
      }
    }
  }

  /**
   * Get configuration summary
   */
  getConfigSummary(): string {
    const dateRangeStr = this.dateRange
      ? `${this.dateRange.from.toISOString().split('T')[0]} to ${this.dateRange.to.toISOString().split('T')[0]}`
      : 'API default (based on subscription plan)';
    const parts = [
      `Markets: ${this.config.markets.join(', ')}`,
      `Date range: ${dateRangeStr}`,
      `Include margin: ${this.config.includeMargin}`,
      `Include TOPIX: ${this.config.includeTOPIX}`,
      `Include sectors: ${this.config.includeSectorIndices}`,
      `Include statements: ${this.config.includeStatements}`,
    ];

    if (this.config.maxStocks) {
      parts.push(`Max stocks: ${this.config.maxStocks}`);
    }

    return parts.join('\n');
  }

  /**
   * Estimate dataset size (rough calculation)
   */
  estimateDatasetSize(stockCount: number): {
    estimatedRecords: number;
    estimatedSizeBytes: number;
    estimatedSizeMB: number;
  } {
    // Default to 10 years (~3650 days) when date range is not specified
    const daysInRange = this.dateRange
      ? Math.ceil((this.dateRange.to.getTime() - this.dateRange.from.getTime()) / (1000 * 60 * 60 * 24))
      : 3650;

    let records = stockCount * daysInRange; // Daily quotes

    if (this.config.includeMargin) {
      records += stockCount * Math.ceil(daysInRange / 7); // Weekly margin data
    }

    if (this.config.includeTOPIX) {
      records += daysInRange; // Daily TOPIX
    }

    if (this.config.includeSectorIndices) {
      records += 33 * daysInRange; // 33 sector indices daily
    }

    if (this.config.includeStatements) {
      records += stockCount * 4; // Rough estimate: 4 statements per stock
    }

    // Rough estimate: 200 bytes per record on average
    const estimatedSizeBytes = records * 200;
    const estimatedSizeMB = estimatedSizeBytes / (1024 * 1024);

    return {
      estimatedRecords: records,
      estimatedSizeBytes,
      estimatedSizeMB: Math.round(estimatedSizeMB * 10) / 10,
    };
  }

  /**
   * Delete existing database files (including WAL and SHM files)
   */
  private deleteExistingDatabase(dbPath: string): void {
    const suffixes = ['', '-wal', '-shm'];
    for (const suffix of suffixes) {
      const filePath = `${dbPath}${suffix}`;
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        if (this.debugConfig.enabled) {
          console.log(`[DATASET BUILDER] Deleted existing file: ${filePath}`);
        }
      }
    }
  }
}
