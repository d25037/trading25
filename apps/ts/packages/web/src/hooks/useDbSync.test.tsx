import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { logger } from '@/utils/logger';
import {
  syncKeys,
  useActiveSyncJob,
  useCancelSync,
  useDbStats,
  useDbValidation,
  useRefreshStocks,
  useStartSync,
  useSyncFetchDetails,
  useSyncJobStatus,
  useSyncSSE,
} from './useDbSync';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
  apiDelete: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  onopen: (() => void) | null = null;
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

  simulateNamedMessage(type: string, data: Record<string, unknown>) {
    for (const listener of this.listeners[type] ?? []) {
      listener({ data: JSON.stringify(data) });
    }
  }

  simulateRawNamedMessage(type: string, rawData: string) {
    for (const listener of this.listeners[type] ?? []) {
      listener({ data: rawData });
    }
  }

  simulateError() {
    this.onerror?.();
  }
}

afterEach(() => {
  vi.clearAllMocks();
});

describe('useDbSync hooks', () => {
  it('useStartSync starts a sync job', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ jobId: 'abc', mode: 'initial' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStartSync(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync({
        mode: 'initial',
        dataPlane: { backend: 'duckdb-parquet' },
      });
    });
    expect(apiPost).toHaveBeenCalledWith('/api/db/sync', {
      mode: 'initial',
      dataPlane: { backend: 'duckdb-parquet' },
    });
  });

  it('useSyncJobStatus fetches job status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'abc', status: 'running' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncJobStatus('abc'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/sync/jobs/abc');
  });

  it('useSyncFetchDetails fetches structured fetch details', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      latest: {
        eventType: 'strategy',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:00Z',
      },
      items: [],
    });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncFetchDetails('abc'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/sync/jobs/abc/fetch-details');
  });

  it('useSyncJobStatus is disabled when jobId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncJobStatus(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('useSyncJobStatus keeps polling when no status data is available', () => {
    vi.mocked(apiGet).mockRejectedValueOnce(new Error('temporary error'));
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncJobStatus('abc'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: ['sync-job', 'abc'] });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: undefined } })).toBe(1000);
  });

  it('useSyncJobStatus stops polling for terminal status', () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'abc', status: 'completed' });
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncJobStatus('abc'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: ['sync-job', 'abc'] });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: { status: 'completed' } } })).toBe(false);
  });

  it('useSyncJobStatus stops polling while SSE is connected', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncJobStatus('abc', true), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(apiGet).not.toHaveBeenCalled();

    const query = queryClient.getQueryCache().find({ queryKey: syncKeys.job('abc') });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: { status: 'running' } } })).toBe(false);
  });

  it('useSyncFetchDetails stops polling while SSE is connected', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncFetchDetails('abc', true), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(apiGet).not.toHaveBeenCalled();

    const query = queryClient.getQueryCache().find({ queryKey: syncKeys.fetchDetails('abc') });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: { status: 'running' } } })).toBe(false);
  });

  it('useSyncFetchDetails stops polling for terminal status', () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      jobId: 'abc',
      status: 'completed',
      mode: 'incremental',
      items: [],
    });
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncFetchDetails('abc'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: syncKeys.fetchDetails('abc') });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: { status: 'completed' } } })).toBe(false);
  });

  it('useActiveSyncJob fetches active job status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'active-1', status: 'running' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useActiveSyncJob(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/sync/jobs/active');
  });

  it('useActiveSyncJob can be disabled', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useActiveSyncJob(false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(apiGet).not.toHaveBeenCalled();
  });

  it('useActiveSyncJob uses slower polling when no active job is running', () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'active-1', status: 'completed' });
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useActiveSyncJob(), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: syncKeys.active() });
    const options = query?.options as { refetchInterval?: unknown } | undefined;
    const refetchInterval = options?.refetchInterval as
      | ((query: { state: { data?: { status?: string } } }) => number | false)
      | undefined;

    expect(refetchInterval).toBeTypeOf('function');
    expect(refetchInterval?.({ state: { data: { status: 'completed' } } })).toBe(5000);
    expect(refetchInterval?.({ state: { data: undefined } })).toBe(5000);
  });

  it('useCancelSync cancels a job and invalidates', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ jobId: 'abc' });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelSync(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync('abc');
    });
    expect(apiDelete).toHaveBeenCalledWith('/api/db/sync/jobs/abc');
    expect(spy).toHaveBeenCalledWith({ queryKey: ['sync-job', 'abc'] });
  });

  it('useCancelSync logs error when cancellation fails', async () => {
    vi.mocked(apiDelete).mockRejectedValueOnce(new Error('cancel failed'));
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCancelSync(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync('abc')).rejects.toThrow('cancel failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to cancel sync', { error: 'cancel failed' });
  });

  it('useDbStats fetches db stats', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ initialized: true });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useDbStats(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/stats');
  });

  it('useDbValidation fetches db validation', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ status: 'healthy' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useDbValidation(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/validate');
  });

  it('useDbStats uses fast polling while sync is running', () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ initialized: true });
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useDbStats({ isSyncRunning: true }), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: ['db-stats', 'running'] });
    const options = query?.options as { refetchInterval?: unknown; staleTime?: unknown } | undefined;
    expect(options?.refetchInterval).toBe(2000);
    expect(options?.staleTime).toBe(0);
  });

  it('useDbValidation uses slower polling while sync is idle', () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ status: 'healthy' });
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useDbValidation({ isSyncRunning: false }), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: ['db-validation', 'idle'] });
    const options = query?.options as { refetchInterval?: unknown; staleTime?: unknown } | undefined;
    expect(options?.refetchInterval).toBe(30000);
    expect(options?.staleTime).toBe(5000);
  });

  it('useRefreshStocks posts request and invalidates db queries', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      totalStocks: 1,
      successCount: 1,
      failedCount: 0,
      totalApiCalls: 1,
      totalRecordsStored: 10,
      results: [{ code: '7203', success: true, recordsFetched: 10, recordsStored: 10 }],
      errors: [],
      lastUpdated: '2026-03-03T00:00:00Z',
    });

    const { queryClient, wrapper } = createTestWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRefreshStocks(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ codes: ['7203'] });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/db/stocks/refresh', { codes: ['7203'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-stats'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['db-validation'] });
  });

  it('useRefreshStocks logs error when refresh fails', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('refresh failed'));
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useRefreshStocks(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ codes: ['7203'] })).rejects.toThrow('refresh failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to refresh stocks', { error: 'refresh failed' });
  });
});

describe('useSyncSSE', () => {
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
    const { result } = renderHook(() => useSyncSSE(null), { wrapper });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances).toHaveLength(0);
  });

  it('updates job and fetch detail caches from snapshot events', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncSSE('abc'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('snapshot', {
        job: {
          jobId: 'abc',
          status: 'running',
          mode: 'incremental',
          enforceBulkForStockData: false,
          startedAt: '2026-03-05T00:00:00Z',
        },
        fetchDetails: {
          jobId: 'abc',
          status: 'running',
          mode: 'incremental',
          latest: {
            eventType: 'strategy',
            stage: 'stock_data',
            endpoint: '/equities/bars/daily',
            method: 'bulk',
            timestamp: '2026-03-05T00:00:00Z',
          },
          items: [
            {
              eventType: 'strategy',
              stage: 'stock_data',
              endpoint: '/equities/bars/daily',
              method: 'bulk',
              timestamp: '2026-03-05T00:00:00Z',
            },
          ],
        },
      });
    });

    expect(result.current.isConnected).toBe(true);
    expect(queryClient.getQueryData(syncKeys.job('abc'))).toEqual({
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      enforceBulkForStockData: false,
      startedAt: '2026-03-05T00:00:00Z',
    });
    expect(queryClient.getQueryData(syncKeys.fetchDetails('abc'))).toEqual({
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      latest: {
        eventType: 'strategy',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:00Z',
      },
      items: [
        {
          eventType: 'strategy',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:00Z',
        },
      ],
    });
  });

  it('merges incremental fetch-detail events into the cache', () => {
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncSSE('abc'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('fetch-detail', {
        jobId: 'abc',
        status: 'running',
        mode: 'incremental',
        detail: {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:00Z',
        },
      });
      MockEventSource.instances[0]?.simulateNamedMessage('fetch-detail', {
        jobId: 'abc',
        status: 'running',
        mode: 'incremental',
        detail: {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:01Z',
        },
      });
    });

    expect(queryClient.getQueryData(syncKeys.fetchDetails('abc'))).toEqual({
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      latest: {
        eventType: 'execution',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:01Z',
      },
      items: [
        {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:00Z',
        },
        {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:01Z',
        },
      ],
    });
  });

  it('keeps existing fetch details when incremental payload detail is null', () => {
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncSSE('abc'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('fetch-detail', {
        jobId: 'abc',
        status: 'running',
        mode: 'incremental',
        detail: {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:00Z',
        },
      });
      MockEventSource.instances[0]?.simulateNamedMessage('fetch-detail', {
        jobId: 'abc',
        status: 'running',
        mode: 'incremental',
        detail: null,
      });
    });

    expect(queryClient.getQueryData(syncKeys.fetchDetails('abc'))).toEqual({
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      latest: {
        eventType: 'execution',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:00Z',
      },
      items: [
        {
          eventType: 'execution',
          stage: 'stock_data',
          endpoint: '/equities/bars/daily',
          method: 'bulk',
          timestamp: '2026-03-05T00:00:00Z',
        },
      ],
    });
  });

  it('closes the stream on terminal job events', () => {
    const { queryClient, wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncSSE('abc'), { wrapper });

    queryClient.setQueryData(syncKeys.fetchDetails('abc'), {
      jobId: 'abc',
      status: 'running',
      mode: 'incremental',
      latest: {
        eventType: 'execution',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:00Z',
      },
      items: [],
    });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('job', {
        jobId: 'abc',
        status: 'completed',
        mode: 'incremental',
        enforceBulkForStockData: false,
        startedAt: '2026-03-05T00:00:00Z',
        completedAt: '2026-03-05T00:01:00Z',
      });
    });

    expect(result.current.isConnected).toBe(false);
    expect(MockEventSource.instances[0]?.closed).toBe(true);
    expect(queryClient.getQueryData(syncKeys.fetchDetails('abc'))).toEqual({
      jobId: 'abc',
      status: 'completed',
      mode: 'incremental',
      latest: {
        eventType: 'execution',
        stage: 'stock_data',
        endpoint: '/equities/bars/daily',
        method: 'bulk',
        timestamp: '2026-03-05T00:00:00Z',
      },
      items: [],
    });
  });

  it('falls back to disconnected state on SSE error and retries', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncSSE('abc'), { wrapper });

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
    expect(MockEventSource.instances[1]?.url).toBe('/api/db/sync/jobs/abc/stream');
  });

  it('resets retry state when jobId changes', () => {
    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(({ jobId }: { jobId: string | null }) => useSyncSSE(jobId), {
      initialProps: { jobId: 'abc' },
      wrapper,
    });

    act(() => {
      MockEventSource.instances[0]?.simulateError();
      vi.advanceTimersByTime(1000);
      MockEventSource.instances[1]?.simulateError();
    });

    rerender({ jobId: 'def' });

    act(() => {
      MockEventSource.instances[2]?.simulateError();
      vi.advanceTimersByTime(1000);
    });

    expect(MockEventSource.instances[2]?.url).toBe('/api/db/sync/jobs/def/stream');
    expect(MockEventSource.instances[3]?.url).toBe('/api/db/sync/jobs/def/stream');
  });

  it('logs parse errors and ignores invalid payloads', () => {
    const { queryClient, wrapper } = createTestWrapper();
    renderHook(() => useSyncSSE('abc'), { wrapper });

    act(() => {
      MockEventSource.instances[0]?.simulateOpen();
      MockEventSource.instances[0]?.simulateNamedMessage('snapshot', {
        job: { invalid: true },
      });
      MockEventSource.instances[0]?.simulateNamedMessage('job', {
        status: 'running',
      });
      MockEventSource.instances[0]?.simulateNamedMessage('fetch-detail', {
        detail: { endpoint: '/equities/bars/daily' },
      });
      MockEventSource.instances[0]?.simulateRawNamedMessage('job', 'invalid-json');
      MockEventSource.instances[0]?.simulateRawNamedMessage('fetch-detail', 'invalid-json');
    });

    expect(queryClient.getQueryData(syncKeys.job('abc'))).toBeUndefined();
    expect(queryClient.getQueryData(syncKeys.fetchDetails('abc'))).toBeUndefined();

    act(() => {
      MockEventSource.instances[0]?.simulateRawNamedMessage('snapshot', 'invalid-json');
    });

    expect(logger.error).toHaveBeenCalledWith(
      'Failed to parse sync SSE snapshot',
      expect.objectContaining({ jobId: 'abc' })
    );
    expect(logger.error).toHaveBeenCalledWith(
      'Failed to parse sync SSE job event',
      expect.objectContaining({ jobId: 'abc' })
    );
    expect(logger.error).toHaveBeenCalledWith(
      'Failed to parse sync SSE fetch-detail event',
      expect.objectContaining({ jobId: 'abc' })
    );
  });

  it('stops reconnecting after max retries', () => {
    const { wrapper } = createTestWrapper();
    renderHook(() => useSyncSSE('abc'), { wrapper });

    for (let attempt = 0; attempt < 4; attempt += 1) {
      act(() => {
        MockEventSource.instances[attempt]?.simulateError();
      });
      act(() => {
        vi.advanceTimersByTime((attempt + 1) * 1000);
      });
    }

    expect(MockEventSource.instances).toHaveLength(4);
    expect(logger.error).toHaveBeenCalledWith('Sync SSE max retries exceeded', { jobId: 'abc' });
  });
});
