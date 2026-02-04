/**
 * Dataset Job Manager
 *
 * Type-safe job manager for dataset creation operations.
 * Uses the unified job manager internally for consistent behavior.
 */

import type { DatasetJobProgress, DatasetJobResult, DatasetJobStatus, DatasetPreset } from '../../schemas/dataset';
import { UnifiedJobManager } from '../job';

/**
 * Dataset-specific job data
 */
export interface DatasetJobData {
  name: string;
  preset: DatasetPreset;
}

/**
 * Internal job state for dataset creation
 * This interface maintains backward compatibility with existing code
 */
export interface DatasetJob {
  jobId: string;
  status: DatasetJobStatus;
  name: string;
  preset: DatasetPreset;
  progress?: DatasetJobProgress;
  result?: DatasetJobResult;
  startedAt: Date;
  completedAt?: Date;
  error?: string;
  abortController?: AbortController;
  /** Last time progress was updated (for stall detection) */
  lastProgressUpdate?: Date;
  /** Timeout ID for job timeout cleanup */
  timeoutId?: ReturnType<typeof setTimeout>;
}

/**
 * Dataset Job Manager
 *
 * Provides a type-safe interface for dataset job operations while
 * delegating to the unified job manager for common functionality.
 */
export class DatasetJobManager {
  private readonly manager: UnifiedJobManager<DatasetJobData, DatasetJobProgress, DatasetJobResult>;

  constructor() {
    this.manager = new UnifiedJobManager<DatasetJobData, DatasetJobProgress, DatasetJobResult>({
      jobTypeName: 'dataset',
      maxCompletedJobs: 10,
    });
  }

  /**
   * Create a new dataset creation job
   * Returns null if a job is already running
   */
  createJob(name: string, preset: DatasetPreset): DatasetJob | null {
    const job = this.manager.createJob({ name, preset });
    if (!job) return null;
    return this.toDatasetJob(job);
  }

  /**
   * Get job by ID
   */
  getJob(jobId: string): DatasetJob | undefined {
    const job = this.manager.getJob(jobId);
    if (!job) return undefined;
    return this.toDatasetJob(job);
  }

  /**
   * Get the currently active job
   */
  getActiveJob(): DatasetJob | null {
    const job = this.manager.getActiveJob();
    if (!job) return null;
    return this.toDatasetJob(job);
  }

  /**
   * Update job status
   */
  updateStatus(jobId: string, status: DatasetJobStatus): void {
    this.manager.updateStatus(jobId, status);
  }

  /**
   * Update job progress
   */
  updateProgress(jobId: string, progress: DatasetJobProgress): void {
    this.manager.updateProgress(jobId, progress);
  }

  /**
   * Complete job with result
   */
  completeJob(jobId: string, result: DatasetJobResult): void {
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
   * Convert internal job to DatasetJob interface for backward compatibility
   */
  private toDatasetJob(job: ReturnType<typeof this.manager.getJob> & object): DatasetJob {
    return {
      jobId: job.jobId,
      status: job.status as DatasetJobStatus,
      name: job.data.name,
      preset: job.data.preset,
      progress: job.progress,
      result: job.result,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
      error: job.error,
      abortController: job.abortController,
      lastProgressUpdate: job.lastProgressUpdate,
      timeoutId: job.timeoutId,
    };
  }
}

// Singleton instance
export const datasetJobManager = new DatasetJobManager();
