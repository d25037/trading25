import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type { DatasetCreateRequest } from '@/types/dataset';
import {
  datasetKeys,
  useCancelDatasetJob,
  useCreateDataset,
  useDatasetInfo,
  useDatasetJobStatus,
  useDatasets,
  useDeleteDataset,
  useResumeDataset,
} from './useDataset';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
  apiDelete: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

const createWrapper = () => {
  const queryClient = createTestQueryClient();
  return {
    queryClient,
    wrapper: createQueryWrapper(queryClient),
  };
};

describe('datasetKeys', () => {
  it('generates correct query keys', () => {
    expect(datasetKeys.all).toEqual(['dataset']);
    expect(datasetKeys.list()).toEqual(['dataset', 'list']);
    expect(datasetKeys.info('prime.db')).toEqual(['dataset', 'info', 'prime.db']);
    expect(datasetKeys.job('job-1')).toEqual(['dataset', 'job', 'job-1']);
  });
});

describe('useDatasets', () => {
  it('fetches datasets list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasets(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset');
  });
});

describe('useDatasetInfo', () => {
  it('fetches dataset info when name is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ name: 'prime.db', rows: 100 });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('prime.db'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset/prime.db/info');
  });

  it('does not fetch when name is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useDatasetJobStatus', () => {
  it('fetches job status when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ jobId: 'job-1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetJobStatus('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset/jobs/job-1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useCreateDataset', () => {
  it('creates dataset and invalidates list', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ jobId: 'job-1' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCreateDataset(), { wrapper });

    const request: DatasetCreateRequest = { name: 'prime.db', preset: 'primeMarket' };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/dataset', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: datasetKeys.list() });
  });

  it('logs error on failure', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Network error'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCreateDataset(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync({ name: 'fail.db', preset: 'primeMarket' });
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to create dataset', { error: 'Network error' });
  });
});

describe('useResumeDataset', () => {
  it('resumes dataset and invalidates list', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ jobId: 'job-2' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useResumeDataset(), { wrapper });

    const request: DatasetCreateRequest = { name: 'prime.db', preset: 'primeMarket' };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/dataset/resume', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: datasetKeys.list() });
  });
});

describe('useDeleteDataset', () => {
  it('deletes dataset and invalidates list', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ name: 'prime.db' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeleteDataset(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('prime.db');
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/dataset/prime.db');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: datasetKeys.list() });
  });
});

describe('useCancelDatasetJob', () => {
  it('cancels a dataset job', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ jobId: 'job-1', status: 'cancelled' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCancelDatasetJob(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('job-1');
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/dataset/jobs/job-1');
  });

  it('logs error on cancel failure', async () => {
    vi.mocked(apiDelete).mockRejectedValueOnce(new Error('Cancel failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCancelDatasetJob(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync('job-1');
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to cancel dataset job', { error: 'Cancel failed' });
  });
});
