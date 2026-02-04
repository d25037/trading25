/**
 * Market Job Manager
 *
 * Type-safe job manager for market sync operations.
 * Uses the unified job manager internally for consistent behavior.
 */

import type { JobProgress, JobStatus, SyncJobResult, SyncMode } from '../../schemas/market-sync';
import { UnifiedJobManager } from '../job';

/**
 * Market sync specific job data
 */
export interface MarketJobData {
  mode: SyncMode;
}

/**
 * Internal job state
 * This interface maintains backward compatibility with existing code
 */
export interface SyncJob {
  jobId: string;
  status: JobStatus;
  mode: SyncMode;
  progress?: JobProgress;
  result?: SyncJobResult;
  startedAt: Date;
  completedAt?: Date;
  error?: string;
  abortController?: AbortController;
}

/**
 * Market Job Manager
 *
 * Provides a type-safe interface for market sync job operations while
 * delegating to the unified job manager for common functionality.
 */
export class MarketJobManager {
  private readonly manager: UnifiedJobManager<MarketJobData, JobProgress, SyncJobResult>;

  constructor() {
    this.manager = new UnifiedJobManager<MarketJobData, JobProgress, SyncJobResult>({
      jobTypeName: 'sync',
      maxCompletedJobs: 10,
    });
  }

  /**
   * Create a new sync job
   * Returns null if a job is already running
   */
  createJob(mode: SyncMode): SyncJob | null {
    const job = this.manager.createJob({ mode });
    if (!job) return null;
    return this.toSyncJob(job);
  }

  /**
   * Get job by ID
   */
  getJob(jobId: string): SyncJob | undefined {
    const job = this.manager.getJob(jobId);
    if (!job) return undefined;
    return this.toSyncJob(job);
  }

  /**
   * Get the currently active job
   */
  getActiveJob(): SyncJob | null {
    const job = this.manager.getActiveJob();
    if (!job) return null;
    return this.toSyncJob(job);
  }

  /**
   * Update job status
   */
  updateStatus(jobId: string, status: JobStatus): void {
    this.manager.updateStatus(jobId, status);
  }

  /**
   * Update job progress
   */
  updateProgress(jobId: string, progress: JobProgress): void {
    this.manager.updateProgress(jobId, progress);
  }

  /**
   * Complete job with result
   */
  completeJob(jobId: string, result: SyncJobResult): void {
    this.manager.completeJob(jobId, result);
  }

  /**
   * Mark job as failed
   */
  failJob(jobId: string, error: string): void {
    this.manager.failJob(jobId, error);
  }

  /**
   * Cancel job
   */
  cancelJob(jobId: string): boolean {
    return this.manager.cancelJob(jobId);
  }

  /**
   * Check if job was cancelled
   */
  isJobCancelled(jobId: string): boolean {
    return this.manager.isJobCancelled(jobId);
  }

  /**
   * Convert internal job to SyncJob interface for backward compatibility
   */
  private toSyncJob(job: ReturnType<typeof this.manager.getJob> & object): SyncJob {
    return {
      jobId: job.jobId,
      status: job.status as JobStatus,
      mode: job.data.mode,
      progress: job.progress,
      result: job.result,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
      error: job.error,
      abortController: job.abortController,
    };
  }
}

// Singleton instance
export const jobManager = new MarketJobManager();
