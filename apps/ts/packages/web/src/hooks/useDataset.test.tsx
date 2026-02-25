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
    vi.mocked(apiGet).mockResolvedValueOnce({
      name: 'prime.db',
      path: '/tmp/prime.db',
      fileSize: 100,
      lastModified: '2026-01-01T00:00:00Z',
      snapshot: {
        preset: 'primeMarket',
        totalStocks: 10,
        stocksWithQuotes: 9,
        dateRange: { min: '2025-01-01', max: '2025-12-31' },
        validation: {
          isValid: true,
          errors: [],
          warnings: [],
        },
      },
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('prime.db'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset/prime.db/info');
    expect(result.current.data?.stats.totalStocks).toBe(10);
    expect(result.current.data?.stats.dateRange.from).toBe('2025-01-01');
    expect(result.current.data?.validation.details?.dataCoverage?.stocksWithQuotes).toBe(9);
  });

  it('passes through modern dataset info response', async () => {
    const modern = {
      name: 'modern.db',
      path: '/tmp/modern.db',
      fileSize: 200,
      lastModified: '2026-01-02T00:00:00Z',
      snapshot: {
        preset: 'primeMarket',
        createdAt: '2026-01-01T00:00:00Z',
      },
      stats: {
        totalStocks: 20,
        totalQuotes: 100,
        dateRange: { from: '2025-01-01', to: '2025-12-31' },
        hasMarginData: true,
        hasTOPIXData: true,
        hasSectorData: true,
        hasStatementsData: true,
        statementsFieldCoverage: null,
      },
      validation: {
        isValid: true,
        errors: [],
        warnings: [],
        details: {
          dataCoverage: {
            totalStocks: 20,
            stocksWithQuotes: 20,
            stocksWithStatements: 20,
            stocksWithMargin: 20,
          },
        },
      },
    };
    vi.mocked(apiGet).mockResolvedValueOnce(modern);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('modern.db'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(modern);
  });

  it('normalizes legacy response defaults when optional fields are missing', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      name: 'legacy.db',
      path: '/tmp/legacy.db',
      fileSize: 300,
      lastModified: '2026-01-03T00:00:00Z',
      snapshot: {},
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('legacy.db'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.stats.dateRange.from).toBe('-');
    expect(result.current.data?.stats.dateRange.to).toBe('-');
    expect(result.current.data?.stats.hasTOPIXData).toBe(true);
    expect(result.current.data?.validation.isValid).toBe(true);
    expect(result.current.data?.validation.details?.dataCoverage?.totalStocks).toBe(0);
  });

  it('maps legacy TOPIX warning to hasTOPIXData=false', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      name: 'legacy-topix.db',
      path: '/tmp/legacy-topix.db',
      fileSize: 400,
      lastModified: '2026-01-04T00:00:00Z',
      snapshot: {
        totalStocks: 5,
        stocksWithQuotes: 5,
        validation: {
          isValid: true,
          errors: [],
          warnings: ['No TOPIX data'],
        },
      },
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('legacy-topix.db'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.stats.hasTOPIXData).toBe(false);
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

  it('keeps polling while status is running', async () => {
    vi.mocked(apiGet).mockClear();
    vi.mocked(apiGet)
      .mockResolvedValueOnce({ jobId: 'job-1', status: 'running' })
      .mockResolvedValueOnce({ jobId: 'job-1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetJobStatus('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalled();

    await waitFor(() => expect(apiGet).toHaveBeenCalledTimes(2), { timeout: 3500 });
  }, 8000);
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

  it('logs error when resume fails', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Resume failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useResumeDataset(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync({ name: 'fail.db', preset: 'primeMarket' });
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to resume dataset', { error: 'Resume failed' });
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

  it('logs error when delete fails', async () => {
    vi.mocked(apiDelete).mockRejectedValueOnce(new Error('Delete failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDeleteDataset(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync('fail.db');
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to delete dataset', { error: 'Delete failed' });
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
