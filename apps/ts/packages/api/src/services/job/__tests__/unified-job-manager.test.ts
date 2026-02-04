import { beforeEach, describe, expect, it } from 'bun:test';
import type { BaseProgress } from '../types';
import { UnifiedJobManager } from '../unified-job-manager';

/**
 * Helper to assert a value is not null/undefined and return it typed
 */
function assertDefined<T>(value: T | null | undefined, message = 'Expected value to be defined'): T {
  if (value === null || value === undefined) {
    throw new Error(message);
  }
  return value;
}

// Test-specific types
interface TestJobData {
  name: string;
  value: number;
}

interface TestProgress extends BaseProgress {
  customField?: string;
}

interface TestResult {
  success: boolean;
  count: number;
}

describe('UnifiedJobManager', () => {
  let manager: UnifiedJobManager<TestJobData, TestProgress, TestResult>;

  beforeEach(() => {
    manager = new UnifiedJobManager<TestJobData, TestProgress, TestResult>({
      jobTypeName: 'test',
      maxCompletedJobs: 3,
    });
  });

  describe('createJob', () => {
    it('should create a job with pending status', () => {
      const job = manager.createJob({ name: 'test', value: 42 });

      expect(job).not.toBeNull();
      expect(job?.status).toBe('pending');
      expect(job?.data.name).toBe('test');
      expect(job?.data.value).toBe(42);
      expect(job?.jobId).toBeDefined();
      expect(job?.startedAt).toBeInstanceOf(Date);
      expect(job?.abortController).toBeInstanceOf(AbortController);
    });

    it('should return null when a job is already running', () => {
      const job1 = manager.createJob({ name: 'first', value: 1 });
      expect(job1).not.toBeNull();

      const job2 = manager.createJob({ name: 'second', value: 2 });
      expect(job2).toBeNull();
    });

    it('should allow creating a new job after previous one completes', () => {
      const job1 = assertDefined(manager.createJob({ name: 'first', value: 1 }));

      manager.completeJob(job1.jobId, { success: true, count: 10 });

      const job2 = assertDefined(manager.createJob({ name: 'second', value: 2 }));
      expect(job2.data.name).toBe('second');
    });

    it('should allow creating a new job after previous one fails', () => {
      const job1 = assertDefined(manager.createJob({ name: 'first', value: 1 }));

      manager.failJob(job1.jobId, 'Something went wrong');

      const job2 = manager.createJob({ name: 'second', value: 2 });
      expect(job2).not.toBeNull();
    });
  });

  describe('getJob', () => {
    it('should return job by ID', () => {
      const created = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      const found = manager.getJob(created.jobId);

      expect(found).toBeDefined();
      expect(found?.jobId).toBe(created.jobId);
    });

    it('should return undefined for non-existent job', () => {
      const found = manager.getJob('non-existent-id');
      expect(found).toBeUndefined();
    });
  });

  describe('getActiveJob', () => {
    it('should return active job when one exists', () => {
      const created = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      const active = manager.getActiveJob();

      expect(active).not.toBeNull();
      expect(active?.jobId).toBe(created.jobId);
    });

    it('should return null when no active job', () => {
      expect(manager.getActiveJob()).toBeNull();
    });

    it('should return null after job completes', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.completeJob(job.jobId, { success: true, count: 10 });

      expect(manager.getActiveJob()).toBeNull();
    });
  });

  describe('updateStatus', () => {
    it('should update job status', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.updateStatus(job.jobId, 'running');

      const updated = manager.getJob(job.jobId);
      expect(updated?.status).toBe('running');
    });
  });

  describe('updateProgress', () => {
    it('should update job progress', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.updateProgress(job.jobId, {
        stage: 'processing',
        current: 5,
        total: 10,
        percentage: 50,
        message: 'Halfway there',
        customField: 'custom',
      });

      const updated = manager.getJob(job.jobId);
      expect(updated?.progress?.stage).toBe('processing');
      expect(updated?.progress?.current).toBe(5);
      expect(updated?.progress?.percentage).toBe(50);
      expect(updated?.progress?.customField).toBe('custom');
      expect(updated?.lastProgressUpdate).toBeInstanceOf(Date);
    });
  });

  describe('completeJob', () => {
    it('should complete job with result', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.completeJob(job.jobId, { success: true, count: 42 });

      const completed = manager.getJob(job.jobId);
      expect(completed?.status).toBe('completed');
      expect(completed?.result?.success).toBe(true);
      expect(completed?.result?.count).toBe(42);
      expect(completed?.completedAt).toBeInstanceOf(Date);
    });
  });

  describe('failJob', () => {
    it('should mark job as failed with error', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.failJob(job.jobId, 'Something went wrong');

      const failed = manager.getJob(job.jobId);
      expect(failed?.status).toBe('failed');
      expect(failed?.error).toBe('Something went wrong');
      expect(failed?.completedAt).toBeInstanceOf(Date);
    });
  });

  describe('cancelJob', () => {
    it('should cancel pending job', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      const result = manager.cancelJob(job.jobId);

      expect(result).toBe(true);
      const cancelled = manager.getJob(job.jobId);
      expect(cancelled?.status).toBe('cancelled');
    });

    it('should cancel running job', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.updateStatus(job.jobId, 'running');
      const result = manager.cancelJob(job.jobId);

      expect(result).toBe(true);
      const cancelled = manager.getJob(job.jobId);
      expect(cancelled?.status).toBe('cancelled');
    });

    it('should return false for non-existent job', () => {
      const result = manager.cancelJob('non-existent');
      expect(result).toBe(false);
    });

    it('should return false for already completed job', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.completeJob(job.jobId, { success: true, count: 1 });

      const result = manager.cancelJob(job.jobId);
      expect(result).toBe(false);
    });

    it('should trigger abort signal', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      const signal = manager.getAbortSignal(job.jobId);

      expect(signal?.aborted).toBe(false);

      manager.cancelJob(job.jobId);

      expect(signal?.aborted).toBe(true);
    });
  });

  describe('isJobCancelled', () => {
    it('should return true for cancelled job', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.cancelJob(job.jobId);

      expect(manager.isJobCancelled(job.jobId)).toBe(true);
    });

    it('should return false for non-cancelled job', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      expect(manager.isJobCancelled(job.jobId)).toBe(false);
    });

    it('should return true when abort signal is triggered', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      job.abortController?.abort();

      expect(manager.isJobCancelled(job.jobId)).toBe(true);
    });
  });

  describe('hasActiveJob', () => {
    it('should return false when no jobs', () => {
      expect(manager.hasActiveJob()).toBe(false);
    });

    it('should return true when pending job exists', () => {
      manager.createJob({ name: 'test', value: 1 });
      expect(manager.hasActiveJob()).toBe(true);
    });

    it('should return true when running job exists', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.updateStatus(job.jobId, 'running');
      expect(manager.hasActiveJob()).toBe(true);
    });

    it('should return false after job completes', () => {
      const job = assertDefined(manager.createJob({ name: 'test', value: 1 }));
      manager.completeJob(job.jobId, { success: true, count: 1 });
      expect(manager.hasActiveJob()).toBe(false);
    });
  });

  describe('cleanup', () => {
    it('should keep only maxCompletedJobs completed jobs after creating new one', () => {
      // Create and complete 5 jobs (maxCompletedJobs is 3)
      for (let i = 1; i <= 5; i++) {
        const job = assertDefined(manager.createJob({ name: `job-${i}`, value: i }));
        manager.completeJob(job.jobId, { success: true, count: i });
      }

      // Cleanup happens when creating a new job, so create one more
      const finalJob = manager.createJob({ name: 'final', value: 99 });
      expect(finalJob).not.toBeNull();

      const allJobs = manager.getAllJobs();
      // Should have 3 completed + 1 pending = 4 total
      // But oldest completed jobs should be removed
      expect(allJobs.length).toBe(4);

      // Should keep the most recent 3 completed jobs plus the new pending one
      const names = allJobs.map((j) => j.data.name);
      expect(names).toContain('job-3');
      expect(names).toContain('job-4');
      expect(names).toContain('job-5');
      expect(names).toContain('final');
      // job-1 and job-2 should be cleaned up
      expect(names).not.toContain('job-1');
      expect(names).not.toContain('job-2');
    });
  });

  describe('getJobCountByStatus', () => {
    it('should return correct counts', () => {
      // Create some jobs with various statuses
      const job1 = assertDefined(manager.createJob({ name: 'job1', value: 1 }));
      manager.completeJob(job1.jobId, { success: true, count: 1 });

      const job2 = assertDefined(manager.createJob({ name: 'job2', value: 2 }));
      manager.failJob(job2.jobId, 'error');

      const job3 = manager.createJob({ name: 'job3', value: 3 });
      expect(job3).not.toBeNull();
      // job3 remains pending

      const counts = manager.getJobCountByStatus();
      expect(counts.completed).toBe(1);
      expect(counts.failed).toBe(1);
      expect(counts.pending).toBe(1);
      expect(counts.running).toBe(0);
      expect(counts.cancelled).toBe(0);
    });
  });
});
