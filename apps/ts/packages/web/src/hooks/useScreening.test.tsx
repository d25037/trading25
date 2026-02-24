import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, apiGet, apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { logger } from '@/utils/logger';
import { useCancelScreeningJob, useRunScreeningJob, useScreeningJobStatus, useScreeningResult } from './useScreening';

vi.mock('@/lib/api-client', () => ({
  ApiError: class ApiError extends Error {
    constructor(
      message: string,
      public readonly status: number
    ) {
      super(message);
      this.name = 'ApiError';
    }
  },
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
  apiDelete: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useRunScreeningJob', () => {
  it('starts screening job and maps request params', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      job_id: 'job-1',
      status: 'pending',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useRunScreeningJob(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        markets: 'prime',
        strategies: 'production/range_break_v15',
        recentDays: 10,
        sortBy: 'matchedDate',
        order: 'desc',
      });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/analytics/screening/jobs', {
      markets: 'prime',
      strategies: 'production/range_break_v15',
      recentDays: 10,
      date: undefined,
      sortBy: 'matchedDate',
      order: 'desc',
      limit: undefined,
    });
  });

  it('logs error when starting screening job fails', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('network'));

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useRunScreeningJob(), { wrapper });

    await act(async () => {
      await expect(
        result.current.mutateAsync({
          markets: 'prime',
          recentDays: 10,
        })
      ).rejects.toThrow('network');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to start screening job', { error: 'network' });
  });
});

describe('useScreeningJobStatus', () => {
  it('fetches screening job status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      job_id: 'job-1',
      status: 'running',
      progress: 0.5,
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobStatus('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/screening/jobs/job-1');
  });

  it('is disabled when jobId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobStatus(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('does not retry when job status returns 404', async () => {
    vi.mocked(apiGet).mockRejectedValue(new ApiError('not found', 404));

    const { queryClient, wrapper } = createTestWrapper();
    queryClient.setDefaultOptions({
      queries: {
        retryDelay: 0,
      },
    });

    const { result } = renderHook(() => useScreeningJobStatus('missing-job'), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(apiGet).toHaveBeenCalledTimes(1);
  });

  it('retries up to two times for non-404 errors', async () => {
    vi.mocked(apiGet).mockRejectedValue(new ApiError('server error', 500));

    const { queryClient, wrapper } = createTestWrapper();
    queryClient.setDefaultOptions({
      queries: {
        retryDelay: 0,
      },
    });

    const { result } = renderHook(() => useScreeningJobStatus('job-500'), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(apiGet).toHaveBeenCalledTimes(3);
  });
});

describe('useScreeningResult', () => {
  it('fetches completed screening result', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      results: [],
      summary: {
        totalStocksScreened: 0,
        matchCount: 0,
        skippedCount: 0,
        byStrategy: {},
        strategiesEvaluated: [],
        strategiesWithoutBacktestMetrics: [],
        warnings: [],
      },
      markets: ['prime'],
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
      lastUpdated: '2026-01-01T00:00:00Z',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningResult('job-2', true), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/screening/result/job-2');
  });

  it('is disabled when explicit enabled flag is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningResult('job-2', false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(apiGet).not.toHaveBeenCalled();
  });
});

describe('useCancelScreeningJob', () => {
  it('cancels screening job', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      job_id: 'job-3',
      status: 'cancelled',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCancelScreeningJob(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('job-3');
    });

    expect(apiPost).toHaveBeenCalledWith('/api/analytics/screening/jobs/job-3/cancel');
  });

  it('logs error when cancellation fails', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('cancel failed'));

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCancelScreeningJob(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync('job-3')).rejects.toThrow('cancel failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to cancel screening job', {
      error: 'cancel failed',
    });
  });
});
