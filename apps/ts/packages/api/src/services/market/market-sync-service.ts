import { type BatchExecutor, createBatchExecutor } from '@trading25/shared/clients/base/BatchExecutor';
import { ApiClient } from '@trading25/shared/dataset/api-client';
import type { SyncResult } from '@trading25/shared/market-sync';
import {
  IncrementalSyncStrategy,
  IndicesOnlySyncStrategy,
  InitialSyncStrategy,
  MarketDatabase,
  MarketDataFetcher,
  StockHistoryRefetcher,
} from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { CreateSyncJobResponse, SyncJobResponse, SyncMode } from '../../schemas/market-sync';
import { createJQuantsClient } from '../../utils/jquants-client-factory';
import { jobManager, type SyncJob } from './market-job-manager';

/**
 * Convert internal job to API response format
 */
function jobToResponse(job: SyncJob): SyncJobResponse {
  return {
    jobId: job.jobId,
    status: job.status,
    mode: job.mode,
    progress: job.progress,
    result: job.result,
    startedAt: job.startedAt.toISOString(),
    completedAt: job.completedAt?.toISOString(),
    error: job.error,
  };
}

/**
 * Determine actual sync mode based on request and database state
 */
function determineActualMode(
  requestedMode: SyncMode,
  isInitialized: boolean
): { mode: 'initial' | 'incremental' | 'indices-only'; error?: string } {
  if (requestedMode === 'initial') {
    return { mode: 'initial' };
  }

  if (requestedMode === 'indices-only') {
    if (!isInitialized) {
      return { mode: 'indices-only', error: 'Database not initialized. TOPIX data required for indices sync.' };
    }
    return { mode: 'indices-only' };
  }

  if (requestedMode === 'incremental') {
    if (!isInitialized) {
      return { mode: 'incremental', error: 'Database not initialized. Use mode=initial first.' };
    }
    return { mode: 'incremental' };
  }

  // Auto mode
  return { mode: isInitialized ? 'incremental' : 'initial' };
}

/**
 * Create sync strategy based on mode
 */
function createSyncStrategy(
  mode: 'initial' | 'incremental' | 'indices-only',
  db: MarketDatabase,
  apiClient: ApiClient,
  fetcher: MarketDataFetcher,
  rateLimiter: BatchExecutor
) {
  if (mode === 'initial') {
    return new InitialSyncStrategy(db, apiClient, fetcher);
  }

  if (mode === 'indices-only') {
    return new IndicesOnlySyncStrategy(db, apiClient, fetcher);
  }

  const refetcher = new StockHistoryRefetcher(db, apiClient, rateLimiter);
  return new IncrementalSyncStrategy(db, apiClient, fetcher, false, refetcher);
}

/**
 * Convert sync result to job result format
 */
function convertResultToJobResult(result: SyncResult) {
  return {
    success: result.success,
    totalApiCalls: result.totalApiCalls,
    stocksUpdated: result.stocksUpdated,
    datesProcessed: result.datesProcessed,
    failedDates: result.failedDates.map((d) => d.toISOString().split('T')[0] || ''),
    errors: result.errors,
  };
}

/**
 * Service for market sync operations
 */
export class MarketSyncService {
  /**
   * Start a new sync job
   */
  startSync(mode: SyncMode): CreateSyncJobResponse | null {
    // Check for active job
    const activeJob = jobManager.getActiveJob();
    if (activeJob && (activeJob.status === 'pending' || activeJob.status === 'running')) {
      logger.warn('Sync already in progress', { activeJobId: activeJob.jobId });
      return null;
    }

    // Create new job
    const job = jobManager.createJob(mode);
    if (!job) {
      return null;
    }

    // Start sync in background
    this.executeSyncInBackground(job.jobId, mode);

    // Estimate API calls based on mode
    let estimatedApiCalls: number;
    if (mode === 'initial') {
      estimatedApiCalls = 552; // ~500 trading days + 52 indices
    } else if (mode === 'indices-only') {
      estimatedApiCalls = 52; // 52 indices
    } else {
      estimatedApiCalls = 5; // Incremental
    }

    return {
      jobId: job.jobId,
      status: job.status,
      mode: job.mode,
      estimatedApiCalls,
      message: 'Sync job started',
    };
  }

  /**
   * Get job status
   */
  getJobStatus(jobId: string): SyncJobResponse | null {
    const job = jobManager.getJob(jobId);
    if (!job) {
      return null;
    }
    return jobToResponse(job);
  }

  /**
   * Cancel a running job
   */
  cancelJob(jobId: string): { success: boolean; message: string } {
    const job = jobManager.getJob(jobId);
    if (!job) {
      return { success: false, message: 'Job not found' };
    }

    if (job.status !== 'pending' && job.status !== 'running') {
      return { success: false, message: `Cannot cancel job in ${job.status} state` };
    }

    const cancelled = jobManager.cancelJob(jobId);
    return {
      success: cancelled,
      message: cancelled ? 'Job cancelled successfully' : 'Failed to cancel job',
    };
  }

  /**
   * Execute sync operation in background
   */
  private executeSyncInBackground(jobId: string, requestedMode: SyncMode): void {
    setImmediate(() => this.runSyncExecution(jobId, requestedMode));
  }

  /**
   * Run the actual sync execution
   */
  private async runSyncExecution(jobId: string, requestedMode: SyncMode): Promise<void> {
    let db: MarketDatabase | null = null;

    try {
      jobManager.updateStatus(jobId, 'running');

      // Initialize clients
      const { db: database, apiClient, fetcher, rateLimiter } = this.initializeClients();
      db = database;

      // Determine actual mode
      const { mode: actualMode, error } = determineActualMode(requestedMode, db.isInitialized());
      if (error) {
        jobManager.failJob(jobId, error);
        return;
      }

      logger.info('Starting sync execution', { jobId, requestedMode, actualMode });

      // Create and execute strategy
      const strategy = createSyncStrategy(actualMode, db, apiClient, fetcher, rateLimiter);
      const result = await strategy.execute((stage, current, total, message) => {
        this.handleProgressUpdate(jobId, stage, current, total, message);
      });

      // Complete job
      jobManager.completeJob(jobId, convertResultToJobResult(result));
    } catch (error) {
      this.handleSyncError(jobId, error);
    } finally {
      if (db) {
        db.close();
      }
    }
  }

  /**
   * Initialize all required clients
   */
  private initializeClients() {
    const dbPath = getMarketDbPath();
    const jquantsClient = createJQuantsClient();
    const apiClient = new ApiClient(jquantsClient);
    const rateLimiter = createBatchExecutor();
    const fetcher = new MarketDataFetcher(apiClient, rateLimiter);
    const db = new MarketDatabase(dbPath);

    return { db, apiClient, fetcher, rateLimiter };
  }

  /**
   * Handle progress update during sync
   */
  private handleProgressUpdate(jobId: string, stage: string, current: number, total: number, message: string): void {
    if (jobManager.isJobCancelled(jobId)) {
      throw new Error('Job cancelled');
    }

    const percentage = total > 0 ? Math.round((current / total) * 100 * 100) / 100 : 0;
    jobManager.updateProgress(jobId, { stage, current, total, percentage, message });
  }

  /**
   * Handle sync execution error
   */
  private handleSyncError(jobId: string, error: unknown): void {
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (errorMessage === 'Job cancelled') {
      logger.info('Sync job was cancelled', { jobId });
      return;
    }

    logger.error('Sync execution failed', { jobId, error: errorMessage });
    jobManager.failJob(jobId, errorMessage);
  }
}

// Singleton instance
export const marketSyncService = new MarketSyncService();
