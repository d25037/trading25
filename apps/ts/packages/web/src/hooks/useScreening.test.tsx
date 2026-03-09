import { act, renderHook, waitFor } from '@testing-library/react';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { logger } from '@/utils/logger';
import {
  screeningKeys,
  useCancelScreeningJob,
  useRunScreeningJob,
  useScreeningJobSSE,
  useScreeningJobStatus,
  useScreeningResult,
} from './useScreening';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    createScreeningJob: vi.fn(),
    getScreeningJobStatus: vi.fn(),
    getScreeningResult: vi.fn(),
    cancelScreeningJob: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;
  listeners: Record<string, Array<(event: { data: string }) => void>> = {};

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  close() {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: { data: string }) => void) {
    if (!this.listeners[type]) {
      this.listeners[type] = [];
    }
    this.listeners[type].push(listener);
  }

  removeEventListener(type: string, listener: (event: { data: string }) => void) {
    const entries = this.listeners[type];
    if (!entries) {
      return;
    }
    this.listeners[type] = entries.filter((entry) => entry !== listener);
  }

  simulateOpen() {
    this.onopen?.();
  }

  simulateMessage(data: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }

  simulateNamedMessage(type: string, data: Record<string, unknown>) {
    for (const listener of this.listeners[type] ?? []) {
      listener({ data: JSON.stringify(data) });
    }
  }

  simulateError() {
    this.onerror?.();
  }
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('useRunScreeningJob', () => {
  it('starts screening job and maps request params', async () => {
    vi.mocked(analyticsClient.createScreeningJob).mockResolvedValueOnce({
      job_id: 'job-1',
      status: 'pending',
      created_at: '2026-02-01T00:00:00Z',
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

    expect(analyticsClient.createScreeningJob).toHaveBeenCalledWith({
      mode: undefined,
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
    vi.mocked(analyticsClient.createScreeningJob).mockRejectedValueOnce(new Error('network'));

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

  it('passes oracle mode through to the job request', async () => {
    vi.mocked(analyticsClient.createScreeningJob).mockResolvedValueOnce({
      job_id: 'job-oracle',
      status: 'pending',
      created_at: '2026-02-01T00:00:00Z',
      mode: 'oracle',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useRunScreeningJob(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        mode: 'oracle',
        markets: 'prime',
        recentDays: 10,
      });
    });

    expect(analyticsClient.createScreeningJob).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'oracle',
      })
    );
  });
});

describe('useScreeningJobStatus', () => {
  it('fetches screening job status', async () => {
    vi.mocked(analyticsClient.getScreeningJobStatus).mockResolvedValueOnce({
      job_id: 'job-1',
      status: 'running',
      progress: 0.5,
      created_at: '2026-02-01T00:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobStatus('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getScreeningJobStatus).toHaveBeenCalledWith('job-1');
  });

  it('is disabled when jobId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobStatus(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('does not retry when job status returns 404', async () => {
    vi.mocked(analyticsClient.getScreeningJobStatus).mockRejectedValue(
      new HttpRequestError('not found', 'http', { status: 404 })
    );

    const { queryClient, wrapper } = createTestWrapper();
    queryClient.setDefaultOptions({
      queries: {
        retryDelay: 0,
      },
    });

    const { result } = renderHook(() => useScreeningJobStatus('missing-job'), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(analyticsClient.getScreeningJobStatus).toHaveBeenCalledTimes(1);
  });

  it('retries up to two times for non-404 errors', async () => {
    vi.mocked(analyticsClient.getScreeningJobStatus).mockRejectedValue(
      new HttpRequestError('server error', 'http', { status: 500 })
    );

    const { queryClient, wrapper } = createTestWrapper();
    queryClient.setDefaultOptions({
      queries: {
        retryDelay: 0,
      },
    });

    const { result } = renderHook(() => useScreeningJobStatus('job-500'), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(analyticsClient.getScreeningJobStatus).toHaveBeenCalledTimes(3);
  });

  it('uses 2-second polling while pending/running and stops on completion', () => {
    vi.mocked(analyticsClient.getScreeningJobStatus).mockResolvedValueOnce({
      job_id: 'job-poll',
      status: 'pending',
      created_at: '2026-02-01T00:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useScreeningJobStatus('job-poll'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: screeningKeys.job('job-poll') });
    const refetchInterval = (query?.options as { refetchInterval?: unknown } | undefined)?.refetchInterval;

    expect(typeof refetchInterval).toBe('function');
    if (typeof refetchInterval === 'function') {
      expect(refetchInterval({ state: { data: { status: 'pending' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'running' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'completed' } } } as never)).toBe(false);
      expect(refetchInterval({ state: { data: undefined } } as never)).toBe(false);
    }
  });

  it('stops polling while SSE is connected', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobStatus('job-sse', true), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getScreeningJobStatus).not.toHaveBeenCalled();

    const query = queryClient.getQueryCache().find({ queryKey: screeningKeys.job('job-sse') });
    const refetchInterval = (query?.options as { refetchInterval?: unknown } | undefined)?.refetchInterval;

    expect(typeof refetchInterval).toBe('function');
    if (typeof refetchInterval === 'function') {
      expect(refetchInterval({ state: { data: { status: 'running' } } } as never)).toBe(false);
    }
  });
});

describe('useScreeningJobSSE', () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    vi.stubGlobal('EventSource', MockEventSource);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('returns disconnected state when jobId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobSSE(null), { wrapper });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances).toHaveLength(0);
  });

  it('updates job cache from snapshot and job events', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobSSE('job-1'), { wrapper });

    expect(MockEventSource.instances).toHaveLength(1);
    expect(MockEventSource.instances[0]?.url).toBe('/api/analytics/screening/jobs/job-1/stream');

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('snapshot', {
        job_id: 'job-1',
        status: 'pending',
        created_at: '2026-02-01T00:00:00Z',
        markets: 'prime',
        recentDays: 10,
        sortBy: 'matchedDate',
        order: 'desc',
      });
    });

    expect(result.current.isConnected).toBe(true);
    expect(queryClient.getQueryData(screeningKeys.job('job-1'))).toEqual({
      job_id: 'job-1',
      status: 'pending',
      created_at: '2026-02-01T00:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });

    act(() => {
      MockEventSource.instances[0]?.simulateNamedMessage('job', {
        job_id: 'job-1',
        status: 'running',
        progress: 0.4,
        created_at: '2026-02-01T00:00:00Z',
        markets: 'prime',
        recentDays: 10,
        sortBy: 'matchedDate',
        order: 'desc',
      });
    });

    expect(queryClient.getQueryData(screeningKeys.job('job-1'))).toEqual({
      job_id: 'job-1',
      status: 'running',
      progress: 0.4,
      created_at: '2026-02-01T00:00:00Z',
      markets: 'prime',
      recentDays: 10,
      sortBy: 'matchedDate',
      order: 'desc',
    });
  });

  it('closes the stream when a terminal job event arrives', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobSSE('job-1'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateMessage({
        job_id: 'job-1',
        status: 'completed',
        created_at: '2026-02-01T00:00:00Z',
        markets: 'prime',
        recentDays: 10,
        sortBy: 'matchedDate',
        order: 'desc',
      });
    });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances[0]?.closed).toBe(true);
  });

  it('falls back to disconnected state on SSE error and retries', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningJobSSE('job-1'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
    });
    expect(result.current.isConnected).toBe(true);

    act(() => {
      MockEventSource.instances[0]?.simulateError();
    });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances[0]?.closed).toBe(true);

    act(() => {
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[1]?.url).toBe('/api/analytics/screening/jobs/job-1/stream');
  });

  it('resets retry state when jobId changes', () => {
    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(({ jobId }: { jobId: string | null }) => useScreeningJobSSE(jobId), {
      initialProps: { jobId: 'job-1' },
      wrapper,
    });

    act(() => {
      MockEventSource.instances[0]?.simulateError();
      vi.advanceTimersByTime(1000);
      MockEventSource.instances[1]?.simulateError();
    });

    rerender({ jobId: 'job-2' });

    act(() => {
      MockEventSource.instances[2]?.simulateError();
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances[2]?.url).toBe('/api/analytics/screening/jobs/job-2/stream');
    expect(MockEventSource.instances[3]?.url).toBe('/api/analytics/screening/jobs/job-2/stream');
  });
});

describe('useScreeningResult', () => {
  it('fetches completed screening result', async () => {
    vi.mocked(analyticsClient.getScreeningResult).mockResolvedValueOnce({
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
    expect(analyticsClient.getScreeningResult).toHaveBeenCalledWith('job-2');
  });

  it('is disabled when explicit enabled flag is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreeningResult('job-2', false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getScreeningResult).not.toHaveBeenCalled();
  });
});

describe('useCancelScreeningJob', () => {
  it('cancels screening job', async () => {
    vi.mocked(analyticsClient.cancelScreeningJob).mockResolvedValueOnce({
      job_id: 'job-3',
      status: 'cancelled',
      created_at: '2026-02-01T00:00:00Z',
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

    expect(analyticsClient.cancelScreeningJob).toHaveBeenCalledWith('job-3');
  });

  it('logs error when cancellation fails', async () => {
    vi.mocked(analyticsClient.cancelScreeningJob).mockRejectedValueOnce(new Error('cancel failed'));

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
