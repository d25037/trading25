/**
 * Simplified Output manager for CLI commands
 * Focuses on CLI-specific output while delegating to standard libraries
 */

import type { ProgressInfo as DatasetProgress, ProgressCallback } from '@trading25/shared/dataset';
import chalk from 'chalk';

/**
 * Simple debug configuration interface
 */
export interface DebugOptions {
  debug?: boolean;
  verbose?: boolean;
  trace?: boolean;
}

export type OutputMode = 'production' | 'debug';

/**
 * Simple performance timing utilities integrated into OutputManager
 */

/**
 * Log memory usage if debug enabled
 */
function logMemoryUsage(label = 'Memory'): void {
  if (process.env.DEBUG) {
    const mem = process.memoryUsage();
    console.log(
      `💾 ${label}: RSS=${(mem.rss / 1024 / 1024).toFixed(1)}MB, Heap=${(mem.heapUsed / 1024 / 1024).toFixed(1)}MB`
    );
  }
}

/**
 * Simplified output manager - delegates complex functionality to standard libraries
 */
export class OutputManager {
  private progressCallback?: ProgressCallback;
  private lastProgressUpdate = 0;
  private readonly progressUpdateThreshold = 100; // ms between updates
  private readonly isDebugMode: boolean;

  constructor(mode: OutputMode = 'production') {
    this.isDebugMode = mode === 'debug';
  }

  /**
   * Initialize output manager for dataset operations
   */
  initialize(): void {
    this.progressCallback = this.createCustomProgressCallback();

    if (this.isDebugMode) {
      this.info('🔍 DEBUG MODE: Enhanced debugging enabled');
      logMemoryUsage('Initial memory');
    }
  }

  /**
   * Get progress callback for dataset operations
   */
  getProgressCallback(): ProgressCallback | undefined {
    return this.progressCallback;
  }

  /**
   * Standard info message
   */
  info(message: string): void {
    console.log(message);
  }

  /**
   * Success message with checkmark
   */
  success(message: string): void {
    console.log(chalk.green(`✅ ${message}`));
  }

  /**
   * Warning message
   */
  warn(message: string): void {
    console.log(chalk.yellow(`⚠️  ${message}`));
  }

  /**
   * Error message
   */
  error(message: string): void {
    console.error(chalk.red(`❌ ${message}`));
  }

  /**
   * Debug message - shows only in debug mode
   */
  debug(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.log(chalk.blue(`[DEBUG] ${message}`), context);
      } else {
        console.log(chalk.blue(`[DEBUG] ${message}`));
      }
    }
  }

  /**
   * Verbose debug message - shows only in debug mode
   */
  verbose(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.log(chalk.gray(`[VERBOSE] ${message}`), context);
      } else {
        console.log(chalk.gray(`[VERBOSE] ${message}`));
      }
    }
  }

  /**
   * Debug info message with context
   */
  debugInfo(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.log(chalk.blue(`[INFO] ${message}`), context);
      } else {
        console.log(chalk.blue(`[INFO] ${message}`));
      }
    }
  }

  /**
   * Trace message for detailed debugging
   */
  trace(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.log(chalk.gray(`[TRACE] ${message}`), context);
      } else {
        console.log(chalk.gray(`[TRACE] ${message}`));
      }
    }
  }

  /**
   * Debug warning message
   */
  debugWarn(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.warn(chalk.yellow(`[WARN] ${message}`), context);
      } else {
        console.warn(chalk.yellow(`[WARN] ${message}`));
      }
    }
  }

  /**
   * Debug error message
   */
  debugError(message: string, context?: Record<string, unknown>): void {
    if (this.isDebugMode) {
      if (context) {
        console.error(chalk.red(`[ERROR] ${message}`), context);
      } else {
        console.error(chalk.red(`[ERROR] ${message}`));
      }
    }
  }

  /**
   * Cleanup resources (no-op for now)
   */
  cleanup(): void {
    // Currently no cleanup needed
    if (this.isDebugMode) {
      this.debug('OutputManager cleanup completed');
    }
  }

  /**
   * Show final summary
   */
  showSummary(operation: string, startTime: number, stats?: Record<string, unknown>): void {
    const duration = ((performance.now() - startTime) / 1000).toFixed(1);
    this.success(`${operation} completed in ${duration}s`);

    if (this.isDebugMode && stats) {
      this.debug('Final statistics', stats);
      logMemoryUsage('Final memory');
    }
  }

  /**
   * Create custom progress callback with rate limiting
   */
  private createCustomProgressCallback(): ProgressCallback {
    return (progress: DatasetProgress) => {
      if (!this.shouldUpdateProgress(progress)) {
        return;
      }

      this.lastProgressUpdate = Date.now();
      this.renderProgressDisplay(progress);
      this.handleDebugMode(progress.currentItem);
      this.handleCompletion(progress);
    };
  }

  /**
   * Check if progress should be updated based on rate limiting
   */
  private shouldUpdateProgress(progress: DatasetProgress): boolean {
    const now = Date.now();
    return now - this.lastProgressUpdate > this.progressUpdateThreshold || progress.processed === progress.total;
  }

  /**
   * Render the main progress display
   */
  private renderProgressDisplay(progress: DatasetProgress): void {
    const { processed, total } = progress;
    const percentage = Math.round((processed / total) * 100);
    const progressBar = this.createProgressBar(percentage);
    const stocksText = total > 1 ? 'stocks' : 'stock';

    process.stdout.write(`\r${progressBar} ${percentage}% (${processed}/${total} ${stocksText})`);

    this.displayCurrentItem(progress.currentItem);
  }

  /**
   * Display current item if not in debug mode
   */
  private displayCurrentItem(currentItem: unknown): void {
    if (currentItem && !this.isDebugMode) {
      const itemStr = typeof currentItem === 'string' ? currentItem : String(currentItem);
      const shortCurrentItem = itemStr.length > 40 ? `${itemStr.substring(0, 37)}...` : itemStr;
      process.stdout.write(` - ${shortCurrentItem}`);
    }
  }

  /**
   * Handle debug mode verbose logging
   */
  private handleDebugMode(currentItem: unknown): void {
    if (this.isDebugMode && currentItem) {
      console.log(chalk.gray(`\n[VERBOSE] ${currentItem}`));
    }
  }

  /**
   * Handle completion state
   */
  private handleCompletion(progress: DatasetProgress): void {
    if (progress.processed === progress.total) {
      console.log(); // Move to next line
    }
  }

  /**
   * Create simple progress bar
   */
  private createProgressBar(percentage: number): string {
    const barLength = 20;
    const filled = Math.round((percentage / 100) * barLength);
    const empty = barLength - filled;
    return `[${'█'.repeat(filled)}${' '.repeat(empty)}]`;
  }

  /**
   * Dataset-specific output methods for backward compatibility
   */

  showDatasetStart(): void {
    this.info('🚀 Starting dataset creation...');
  }

  showAuthCheck(): void {
    this.info('🔐 Checking authentication...');
  }

  showAuthSuccess(): void {
    this.success('Authentication verified');
  }

  showPresetConfig(preset: string): void {
    this.info(`📋 Using preset configuration: ${preset}`);
  }

  showDatasetConfig(config: {
    outputPath: string;
    startDate?: string;
    endDate?: string;
    marketCodes?: string[];
    includeMarginData?: boolean;
    includeStatements?: boolean;
    includeTOPIX?: boolean;
    includeSectorIndices?: boolean;
  }): void {
    this.info('📊 Dataset configuration:');
    this.info(`   Output: ${config.outputPath}`);
    if (config.startDate && config.endDate) {
      this.info(`   Date range: ${config.startDate} to ${config.endDate}`);
    }
    if (config.marketCodes) {
      this.info(`   Markets: ${config.marketCodes.join(', ')}`);
    }
    if (config.includeMarginData) {
      this.info('   ✓ Margin data included');
    }
    if (config.includeStatements) {
      this.info('   ✓ Financial statements included');
    }
    if (config.includeTOPIX) {
      this.info('   ✓ TOPIX data included');
    }
    if (config.includeSectorIndices) {
      this.info('   ✓ Sector indices included');
    }
  }

  showBuildStart(): void {
    this.info('🔨 Starting dataset build...');
  }

  showSuccessResults(result: {
    totalStocks?: number;
    processedStocks?: number;
    warnings?: string[];
    duration?: number;
    outputPath?: string;
  }): void {
    this.success('Dataset creation completed successfully!');

    if (result.totalStocks !== undefined) {
      this.info(`📊 Total stocks: ${result.totalStocks.toLocaleString()}`);
    }
    if (result.processedStocks !== undefined) {
      this.info(`✅ Processed stocks: ${result.processedStocks.toLocaleString()}`);
    }
    if (result.duration !== undefined) {
      this.info(`⏱️  Duration: ${result.duration.toFixed(1)}s`);
    }
    if (result.outputPath) {
      this.info(`📁 Output: ${result.outputPath}`);
    }
    if (result.warnings && result.warnings.length > 0) {
      this.warn(`⚠️  ${result.warnings.length} warnings occurred`);
      if (this.isDebugMode) {
        for (const warning of result.warnings) {
          this.warn(`   ${warning}`);
        }
      }
    }
  }

  showFailureResults(result: { errors?: string[]; warnings?: string[]; success?: boolean }): void {
    this.error('Dataset creation failed!');

    if (result.errors) {
      for (const error of result.errors) {
        this.error(`   ${error}`);
      }
    }
    if (result.warnings && result.warnings.length > 0) {
      this.warn(`${result.warnings.length} warnings also occurred:`);
      for (const warning of result.warnings) {
        this.warn(`   ${warning}`);
      }
    }
  }

  // Static factory methods for backward compatibility
  static createDebug(): OutputManager {
    return new OutputManager('debug');
  }

  static createProduction(): OutputManager {
    return new OutputManager('production');
  }

  static createFromEnvironment(options?: DebugOptions): OutputManager {
    const isDebug =
      options?.debug || options?.verbose || options?.trace || process.env.DEBUG || process.env.DATASET_DEBUG;
    return new OutputManager(isDebug ? 'debug' : 'production');
  }
}
