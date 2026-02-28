import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useCancelSync, useStartSync, useSyncJobStatus } from './useDbSync';

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
        dataPlane: { backend: 'duckdb-parquet', sqliteMirror: false },
      });
    });
    expect(apiPost).toHaveBeenCalledWith('/api/db/sync', {
      mode: 'initial',
      dataPlane: { backend: 'duckdb-parquet', sqliteMirror: false },
    });
  });

  it('useSyncJobStatus fetches job status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'abc', status: 'running' });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncJobStatus('abc'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/db/sync/jobs/abc');
  });

  it('useSyncJobStatus is disabled when jobId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSyncJobStatus(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
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
});
