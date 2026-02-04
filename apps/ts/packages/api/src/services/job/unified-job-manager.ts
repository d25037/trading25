/**
 * Unified Job Manager
 *
 * A generic, type-safe job manager for handling long-running operations.
 * Provides a single implementation for common job management patterns:
 * - Single active job constraint
 * - Progress tracking
 * - Cancellation support via AbortController
 * - Automatic cleanup of old completed jobs
 *
 * @example
 * ```typescript
 * // Define job-specific types
 * interface DatasetJobData { name: string; preset: string; }
 * interface DatasetResult { success: boolean; totalStocks: number; }
 *
 * // Create typed job manager
 * const manager = new UnifiedJobManager<DatasetJobData, BaseProgress, DatasetResult>({
 *   jobTypeName: 'dataset',
 *   maxCompletedJobs: 10,
 * });
 *
 * // Create a job
 * const job = manager.createJob({ name: 'test.db', preset: 'primeMarket' });
 * ```
 */

import { logger } from '@trading25/shared/utils/logger';
import {
  type BaseJob,
  type BaseProgress,
  DEFAULT_JOB_MANAGER_CONFIG,
  type JobManagerConfig,
  type JobStatus,
} from './types';

/**
 * Generic job manager for long-running operations
 */
export class UnifiedJobManager<TData, TProgress extends BaseProgress = BaseProgress, TResult = unknown> {
  private jobs: Map<string, BaseJob<TData, TProgress, TResult>> = new Map();
  private activeJobId: string | null = null;
  private readonly config: JobManagerConfig;

  constructor(config: Partial<JobManagerConfig> = {}) {
    this.config = { ...DEFAULT_JOB_MANAGER_CONFIG, ...config };
  }

  /**
   * Create a new job
   * Returns null if a job is already running (single active job constraint)
   */
  createJob(data: TData): BaseJob<TData, TProgress, TResult> | null {
    if (this.activeJobId) {
      const activeJob = this.jobs.get(this.activeJobId);
      if (activeJob && (activeJob.status === 'pending' || activeJob.status === 'running')) {
        logger.warn(`Cannot create ${this.config.jobTypeName} job: another job is already running`, {
          activeJobId: this.activeJobId,
        });
        return null;
      }
    }

    const jobId = crypto.randomUUID();
    const job: BaseJob<TData, TProgress, TResult> = {
      jobId,
      status: 'pending',
      data,
      startedAt: new Date(),
      abortController: new AbortController(),
    };

    this.jobs.set(jobId, job);
    this.activeJobId = jobId;
    this.cleanupOldJobs();

    logger.info(`Created ${this.config.jobTypeName} job`, { jobId, data });
    return job;
  }

  /**
   * Get job by ID
   */
  getJob(jobId: string): BaseJob<TData, TProgress, TResult> | undefined {
    return this.jobs.get(jobId);
  }

  /**
   * Get the currently active job
   */
  getActiveJob(): BaseJob<TData, TProgress, TResult> | null {
    if (!this.activeJobId) return null;
    return this.jobs.get(this.activeJobId) ?? null;
  }

  /**
   * Check if there's an active job
   */
  hasActiveJob(): boolean {
    if (!this.activeJobId) return false;
    const job = this.jobs.get(this.activeJobId);
    return job !== undefined && (job.status === 'pending' || job.status === 'running');
  }

  /**
   * Update job status
   */
  updateStatus(jobId: string, status: JobStatus): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.status = status;
      if (status === 'running') {
        logger.info(`${this.config.jobTypeName} job started`, { jobId });
      }
    }
  }

  /**
   * Update job progress
   */
  updateProgress(jobId: string, progress: TProgress): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.progress = progress;
      job.lastProgressUpdate = new Date();
    }
  }

  /**
   * Complete job with result
   */
  completeJob(jobId: string, result: TResult): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.status = 'completed';
      job.result = result;
      job.completedAt = new Date();
      if (this.activeJobId === jobId) {
        this.activeJobId = null;
      }
      logger.info(`${this.config.jobTypeName} job completed`, { jobId, result });
    }
  }

  /**
   * Mark job as failed
   */
  failJob(jobId: string, error: string): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.status = 'failed';
      job.error = error;
      job.completedAt = new Date();
      if (this.activeJobId === jobId) {
        this.activeJobId = null;
      }
      logger.error(`${this.config.jobTypeName} job failed`, { jobId, error });
    }
  }

  /**
   * Cancel job
   */
  cancelJob(jobId: string): boolean {
    const job = this.jobs.get(jobId);
    if (!job) {
      return false;
    }

    if (job.status !== 'pending' && job.status !== 'running') {
      return false;
    }

    // Signal abort to running operation
    if (job.abortController) {
      job.abortController.abort();
    }

    job.status = 'cancelled';
    job.completedAt = new Date();
    if (this.activeJobId === jobId) {
      this.activeJobId = null;
    }

    logger.info(`${this.config.jobTypeName} job cancelled`, { jobId });
    return true;
  }

  /**
   * Check if job was cancelled
   */
  isJobCancelled(jobId: string): boolean {
    const job = this.jobs.get(jobId);
    return job?.status === 'cancelled' || job?.abortController?.signal.aborted === true;
  }

  /**
   * Get abort signal for a job (for passing to async operations)
   */
  getAbortSignal(jobId: string): AbortSignal | undefined {
    return this.jobs.get(jobId)?.abortController?.signal;
  }

  /**
   * Set timeout ID for cleanup tracking
   */
  setTimeoutId(jobId: string, timeoutId: ReturnType<typeof setTimeout>): void {
    const job = this.jobs.get(jobId);
    if (job) {
      job.timeoutId = timeoutId;
    }
  }

  /**
   * Get timeout ID for cleanup
   */
  getTimeoutId(jobId: string): ReturnType<typeof setTimeout> | undefined {
    return this.jobs.get(jobId)?.timeoutId;
  }

  /**
   * Get last progress update time (for stall detection)
   */
  getLastProgressUpdate(jobId: string): Date | undefined {
    return this.jobs.get(jobId)?.lastProgressUpdate;
  }

  /**
   * Get all jobs (for debugging/monitoring)
   */
  getAllJobs(): Array<BaseJob<TData, TProgress, TResult>> {
    return Array.from(this.jobs.values());
  }

  /**
   * Get job count by status
   */
  getJobCountByStatus(): Record<JobStatus, number> {
    const counts: Record<JobStatus, number> = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
    };

    for (const job of this.jobs.values()) {
      counts[job.status]++;
    }

    return counts;
  }

  /**
   * Clean up old completed jobs
   */
  private cleanupOldJobs(): void {
    const completedJobs: string[] = [];

    for (const [id, job] of this.jobs) {
      if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
        completedJobs.push(id);
      }
    }

    // Sort by completion time and remove oldest
    completedJobs.sort((a, b) => {
      const jobA = this.jobs.get(a);
      const jobB = this.jobs.get(b);
      const timeA = jobA?.completedAt?.getTime() ?? 0;
      const timeB = jobB?.completedAt?.getTime() ?? 0;
      return timeA - timeB;
    });

    while (completedJobs.length > this.config.maxCompletedJobs) {
      const oldestId = completedJobs.shift();
      if (oldestId) {
        this.jobs.delete(oldestId);
      }
    }
  }
}
