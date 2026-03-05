import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import {
  useActiveSyncJob,
  useCancelSync,
  useDbStats,
  useDbValidation,
  useRefreshStocks,
  useSyncFetchDetails,
  useStartSync,
  useSyncJobStatus,
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

  it('useActiveSyncJob fetches active job status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'active-1', status: 'running' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useActiveSyncJob(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/sync/jobs/active');
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
});
