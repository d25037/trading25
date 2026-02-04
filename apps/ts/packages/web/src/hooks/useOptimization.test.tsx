import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type { OptimizationGridSaveRequest, OptimizationRequest } from '@/types/backtest';
import {
  optimizationKeys,
  useDeleteOptimizationGrid,
  useDeleteOptimizationHtmlFile,
  useOptimizationGridConfig,
  useOptimizationGridConfigs,
  useOptimizationHtmlFileContent,
  useOptimizationHtmlFiles,
  useOptimizationJobStatus,
  useRenameOptimizationHtmlFile,
  useRunOptimization,
  useSaveOptimizationGrid,
} from './useOptimization';

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

describe('optimizationKeys', () => {
  it('generates correct query keys', () => {
    expect(optimizationKeys.all).toEqual(['optimization']);
    expect(optimizationKeys.job('j1')).toEqual(['optimization', 'job', 'j1']);
    expect(optimizationKeys.gridConfigs()).toEqual(['optimization', 'grid-configs']);
    expect(optimizationKeys.gridConfig('s1')).toEqual(['optimization', 'grid-config', 's1']);
    expect(optimizationKeys.htmlFilesPrefix()).toEqual(['optimization', 'html-files']);
    expect(optimizationKeys.htmlFiles('s1')).toEqual(['optimization', 'html-files', 's1']);
    expect(optimizationKeys.htmlFileContent('s1', 'f1')).toEqual(['optimization', 'html-file-content', 's1', 'f1']);
  });
});

describe('useRunOptimization', () => {
  it('runs optimization', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'opt-1', status: 'running' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRunOptimization(), { wrapper });

    const request = { strategy_name: 'test.yml' } as OptimizationRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/bt/api/optimize/run', request);
  });

  it('logs error on failure', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRunOptimization(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync({ strategy_name: 'test.yml' } as OptimizationRequest);
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to start optimization', { error: 'Failed' });
  });
});

describe('useOptimizationJobStatus', () => {
  it('fetches job status when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ job_id: 'opt-1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationJobStatus('opt-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/jobs/opt-1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useOptimizationGridConfigs', () => {
  it('fetches all grid configs', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ configs: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfigs(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/grid-configs');
  });
});

describe('useOptimizationGridConfig', () => {
  it('fetches grid config for strategy', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ strategy_name: 'Alpha', grid: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfig('Alpha Strategy'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/grid-configs/Alpha%20Strategy');
  });

  it('does not fetch when strategy is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfig(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useSaveOptimizationGrid', () => {
  it('saves grid config and invalidates queries', async () => {
    vi.mocked(apiPut).mockResolvedValueOnce({ success: true, strategy_name: 'Alpha' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useSaveOptimizationGrid(), { wrapper });

    const request = { content: '' } as OptimizationGridSaveRequest;

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'Alpha', request });
    });

    expect(apiPut).toHaveBeenCalledWith('/bt/api/optimize/grid-configs/Alpha', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfigs() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfig('Alpha') });
  });
});

describe('useDeleteOptimizationGrid', () => {
  it('deletes grid config and clears cache', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true, strategy_name: 'Beta' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteOptimizationGrid(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('Beta');
    });

    expect(apiDelete).toHaveBeenCalledWith('/bt/api/optimize/grid-configs/Beta');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfigs() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfig('Beta') });
  });
});

describe('useOptimizationHtmlFiles', () => {
  it('fetches optimization HTML files with strategy', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ files: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFiles('myStrat'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/html-files?strategy=myStrat&limit=100');
  });

  it('fetches without strategy filter', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ files: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFiles(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/html-files?limit=100');
  });
});

describe('useOptimizationHtmlFileContent', () => {
  it('fetches HTML file content', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ content: '<html>' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFileContent('strat', 'opt.html'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/bt/api/optimize/html-files/strat/opt.html');
  });

  it('does not fetch when strategy or filename is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFileContent(null, null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useRenameOptimizationHtmlFile', () => {
  it('renames HTML file and invalidates cache', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      old_filename: 'old.html',
      new_filename: 'new.html',
      strategy_name: 'strat',
    });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useRenameOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        strategy: 'strat',
        filename: 'old.html',
        request: { new_filename: 'new.html' },
      });
    });

    expect(apiPost).toHaveBeenCalledWith('/bt/api/optimize/html-files/strat/old.html/rename', {
      new_filename: 'new.html',
    });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.htmlFilesPrefix() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.htmlFileContent('strat', 'old.html'),
    });
  });
});

describe('useDeleteOptimizationHtmlFile', () => {
  it('deletes HTML file and invalidates cache', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({
      strategy_name: 'strat',
      filename: 'opt.html',
    });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'strat', filename: 'opt.html' });
    });

    expect(apiDelete).toHaveBeenCalledWith('/bt/api/optimize/html-files/strat/opt.html');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.htmlFilesPrefix() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.htmlFileContent('strat', 'opt.html'),
    });
  });
});
