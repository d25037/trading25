import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet, apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import {
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobStatus,
  useScreeningResult,
} from './useScreening';

vi.mock('@/lib/api-client', () => ({
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
});
