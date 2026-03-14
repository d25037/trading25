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
    expect(datasetKeys.info('prime')).toEqual(['dataset', 'info', 'prime']);
    expect(datasetKeys.job('job-1')).toEqual(['dataset', 'job', 'job-1']);
  });
});

describe('useDatasets', () => {
  it('fetches datasets list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([
      {
        name: 'snapshot',
        path: '/tmp/snapshot',
        fileSize: 200,
        lastModified: '2026-01-02T00:00:00Z',
        preset: 'primeMarket',
        createdAt: '2026-01-02T00:00:00Z',
        backend: 'duckdb-parquet',
        hasCompatibilityArtifact: false,
      },
    ]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasets(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset');
    expect(result.current.data).toEqual([
      {
        name: 'snapshot',
        path: '/tmp/snapshot',
        fileSize: 200,
        lastModified: '2026-01-02T00:00:00Z',
        preset: 'primeMarket',
        createdAt: '2026-01-02T00:00:00Z',
        backend: 'duckdb-parquet',
        hasCompatibilityArtifact: false,
      },
    ]);
  });

  it('normalizes legacy dataset list items', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([
      {
        name: 'legacy',
        path: '/tmp/legacy.db',
        fileSize: 10,
        lastModified: '2026-01-03T00:00:00Z',
      },
    ]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasets(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      {
        name: 'legacy',
        path: '/tmp/legacy.db',
        fileSize: 10,
        lastModified: '2026-01-03T00:00:00Z',
        preset: null,
        createdAt: null,
        backend: 'sqlite-legacy',
        hasCompatibilityArtifact: false,
      },
    ]);
  });
});

describe('useDatasetInfo', () => {
  it('fetches dataset info when name is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      name: 'prime',
      path: '/tmp/prime',
      fileSize: 100,
      lastModified: '2026-01-01T00:00:00Z',
      storage: {
        backend: 'duckdb-parquet',
        primaryPath: '/tmp/prime',
        duckdbPath: '/tmp/prime/dataset.duckdb',
        compatibilityDbPath: null,
        manifestPath: '/tmp/prime/manifest.v2.json',
        hasCompatibilityArtifact: false,
      },
      snapshot: {
        preset: 'primeMarket',
        createdAt: '2026-01-01T00:00:00Z',
      },
      stats: {
        totalStocks: 10,
        totalQuotes: 9,
        dateRange: { from: '2025-01-01', to: '2025-12-31' },
        hasMarginData: false,
        hasTOPIXData: true,
        hasSectorData: false,
        hasStatementsData: false,
        statementsFieldCoverage: null,
      },
      validation: {
        isValid: true,
        errors: [],
        warnings: [],
        details: {
          dataCoverage: {
            totalStocks: 10,
            stocksWithQuotes: 9,
            stocksWithStatements: 0,
            stocksWithMargin: 0,
          },
        },
      },
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('prime'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/dataset/prime/info');
    expect(result.current.data?.stats.totalStocks).toBe(10);
    expect(result.current.data?.stats.dateRange.from).toBe('2025-01-01');
    expect(result.current.data?.validation.details?.dataCoverage?.stocksWithQuotes).toBe(9);
    expect(result.current.data?.storage.manifestPath).toBe('/tmp/prime/manifest.v2.json');
  });

  it('normalizes legacy dataset info payloads', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      name: 'legacy',
      path: '/tmp/legacy.db',
      fileSize: 100,
      lastModified: '2026-01-01T00:00:00Z',
      snapshot: {
        preset: 'quickTesting',
        createdAt: '2026-01-01T00:00:00Z',
        totalStocks: 3,
        stocksWithQuotes: 2,
        dateRange: { min: '2025-01-01', max: '2025-01-31' },
        validation: {
          isValid: true,
          errors: [],
          warnings: ['No TOPIX data'],
        },
      },
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDatasetInfo('legacy'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.storage.backend).toBe('sqlite-legacy');
    expect(result.current.data?.storage.primaryPath).toBe('/tmp/legacy.db');
    expect(result.current.data?.storage.hasCompatibilityArtifact).toBe(false);
    expect(result.current.data?.stats.totalStocks).toBe(3);
    expect(result.current.data?.stats.dateRange.from).toBe('2025-01-01');
    expect(result.current.data?.stats.hasTOPIXData).toBe(false);
    expect(result.current.data?.validation.details?.dataCoverage?.stocksWithQuotes).toBe(2);
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

    const request: DatasetCreateRequest = { name: 'prime', preset: 'primeMarket' };

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
        await result.current.mutateAsync({ name: 'fail', preset: 'primeMarket' });
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to create dataset', { error: 'Network error' });
  });
});

describe('useDeleteDataset', () => {
  it('deletes dataset and invalidates list', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ name: 'prime' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeleteDataset(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('prime');
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/dataset/prime');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: datasetKeys.list() });
  });

  it('logs error when delete fails', async () => {
    vi.mocked(apiDelete).mockRejectedValueOnce(new Error('Delete failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDeleteDataset(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync('fail');
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
