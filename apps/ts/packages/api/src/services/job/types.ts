/**
 * Unified job management types
 *
 * This module provides shared types for long-running job management
 * across different job types (dataset creation, market sync, etc.)
 */

/**
 * Common job status for all job types
 */
export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

/**
 * Base progress interface that all job types must implement
 */
export interface BaseProgress {
  stage: string;
  current: number;
  total: number;
  percentage: number;
  message: string;
}

/**
 * Base job interface that all job types extend
 */
export interface BaseJob<TData = unknown, TProgress extends BaseProgress = BaseProgress, TResult = unknown> {
  /** Unique job identifier */
  jobId: string;
  /** Current job status */
  status: JobStatus;
  /** Type-specific job data (e.g., name/preset for dataset, mode for sync) */
  data: TData;
  /** Current progress (undefined until running) */
  progress?: TProgress;
  /** Final result (undefined until completed) */
  result?: TResult;
  /** Job start time */
  startedAt: Date;
  /** Job completion time (undefined until finished) */
  completedAt?: Date;
  /** Error message if failed */
  error?: string;
  /** Abort controller for cancellation support */
  abortController?: AbortController;
  /** Last progress update time (for stall detection) */
  lastProgressUpdate?: Date;
  /** Timeout ID for job timeout cleanup */
  timeoutId?: ReturnType<typeof setTimeout>;
}

/**
 * Configuration for job manager behavior
 */
export interface JobManagerConfig {
  /** Maximum number of completed jobs to retain */
  maxCompletedJobs: number;
  /** Job type name for logging */
  jobTypeName: string;
}

/**
 * Default job manager configuration
 */
export const DEFAULT_JOB_MANAGER_CONFIG: JobManagerConfig = {
  maxCompletedJobs: 10,
  jobTypeName: 'job',
};
