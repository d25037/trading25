import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { backtestClient } from '@/lib/backtest-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type { OptimizationGridSaveRequest, OptimizationRequest } from '@/types/backtest';
import {
  optimizationKeys,
  useCancelOptimization,
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

vi.mock('@/lib/backtest-client', () => ({
  backtestClient: {
    runOptimization: vi.fn(),
    getOptimizationJobStatus: vi.fn(),
    cancelOptimizationJob: vi.fn(),
    getOptimizationGridConfigs: vi.fn(),
    getOptimizationGridConfig: vi.fn(),
    saveOptimizationGridConfig: vi.fn(),
    deleteOptimizationGridConfig: vi.fn(),
    listOptimizationHtmlFiles: vi.fn(),
    getOptimizationHtmlFileContent: vi.fn(),
    renameOptimizationHtmlFile: vi.fn(),
    deleteOptimizationHtmlFile: vi.fn(),
  },
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
    vi.mocked(backtestClient.runOptimization).mockResolvedValueOnce({ job_id: 'opt-1', status: 'running' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRunOptimization(), { wrapper });

    const request = { strategy_name: 'test.yml' } as OptimizationRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(backtestClient.runOptimization).toHaveBeenCalledWith(request);
  });

  it('logs error on failure', async () => {
    vi.mocked(backtestClient.runOptimization).mockRejectedValueOnce(new Error('Failed'));

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
    vi.mocked(backtestClient.getOptimizationJobStatus).mockResolvedValueOnce({
      job_id: 'opt-1',
      status: 'completed',
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationJobStatus('opt-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationJobStatus).toHaveBeenCalledWith('opt-1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('uses 2-second polling while pending/running and stops on completion', () => {
    vi.mocked(backtestClient.getOptimizationJobStatus).mockResolvedValueOnce({
      job_id: 'opt-poll',
      status: 'pending',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    renderHook(() => useOptimizationJobStatus('opt-poll'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: optimizationKeys.job('opt-poll') });
    const refetchInterval = (query?.options as { refetchInterval?: unknown } | undefined)?.refetchInterval;

    expect(typeof refetchInterval).toBe('function');
    if (typeof refetchInterval === 'function') {
      expect(refetchInterval({ state: { data: { status: 'pending' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'running' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'completed' } } } as never)).toBe(false);
      expect(refetchInterval({ state: { data: undefined } } as never)).toBe(false);
    }
  });

  it('covers create/status/result lifecycle for optimize jobs', async () => {
    vi.mocked(backtestClient.runOptimization).mockResolvedValueOnce({
      job_id: 'opt-e2e-1',
      status: 'pending',
    } as never);
    vi.mocked(backtestClient.getOptimizationJobStatus).mockResolvedValueOnce({
      job_id: 'opt-e2e-1',
      status: 'completed',
      best_score: 1.42,
      best_params: { lookback_days: 60 },
      worst_score: -0.21,
      worst_params: { lookback_days: 10 },
      total_combinations: 48,
      html_path: '/tmp/optimization_opt-e2e-1.html',
    } as never);
    vi.mocked(backtestClient.listOptimizationHtmlFiles).mockResolvedValueOnce({
      files: [
        {
          strategy_name: 'production/range_break_v5',
          filename: 'optimization_opt-e2e-1.html',
          dataset_name: 'dataset-1',
          created_at: '2026-03-02T00:00:00Z',
          size_bytes: 1024,
        },
      ],
      total: 1,
    } as never);

    const { wrapper } = createWrapper();
    const runHook = renderHook(() => useRunOptimization(), { wrapper });

    let submittedJob: { job_id: string };
    await act(async () => {
      submittedJob = (await runHook.result.current.mutateAsync({
        strategy_name: 'production/range_break_v5',
      } as OptimizationRequest)) as { job_id: string };
    });

    const statusHook = renderHook(() => useOptimizationJobStatus(submittedJob.job_id), { wrapper });
    await waitFor(() => expect(statusHook.result.current.isSuccess).toBe(true));
    expect(statusHook.result.current.data?.best_score).toBe(1.42);
    expect(statusHook.result.current.data?.best_params).toEqual({ lookback_days: 60 });
    expect(statusHook.result.current.data?.worst_score).toBe(-0.21);

    const resultHook = renderHook(() => useOptimizationHtmlFiles('production/range_break_v5', 10), { wrapper });
    await waitFor(() => expect(resultHook.result.current.isSuccess).toBe(true));

    expect(backtestClient.runOptimization).toHaveBeenCalledWith({ strategy_name: 'production/range_break_v5' });
    expect(backtestClient.getOptimizationJobStatus).toHaveBeenCalledWith('opt-e2e-1');
    expect(backtestClient.listOptimizationHtmlFiles).toHaveBeenCalledWith({
      strategy: 'production/range_break_v5',
      limit: 10,
    });
  });

  it('covers cancel/retry/resume lifecycle for optimize jobs', async () => {
    vi.mocked(backtestClient.runOptimization)
      .mockResolvedValueOnce({
        job_id: 'opt-retry-1',
        status: 'pending',
      } as never)
      .mockResolvedValueOnce({
        job_id: 'opt-retry-2',
        status: 'pending',
      } as never);
    vi.mocked(backtestClient.cancelOptimizationJob).mockResolvedValueOnce({
      job_id: 'opt-retry-1',
      status: 'cancelled',
      message: 'cancelled by user',
    } as never);
    vi.mocked(backtestClient.getOptimizationJobStatus).mockResolvedValueOnce({
      job_id: 'opt-retry-2',
      status: 'completed',
      best_score: 0.9,
      best_params: { lookback_days: 40 },
      worst_score: -0.4,
      worst_params: { lookback_days: 5 },
      total_combinations: 24,
    } as never);

    const { wrapper } = createWrapper();
    const runHook = renderHook(() => useRunOptimization(), { wrapper });
    const cancelHook = renderHook(() => useCancelOptimization(), { wrapper });

    await act(async () => {
      await runHook.result.current.mutateAsync({ strategy_name: 'production/range_break_v5' } as OptimizationRequest);
    });
    await act(async () => {
      await cancelHook.result.current.mutateAsync('opt-retry-1');
    });
    let retriedJob: { job_id: string };
    await act(async () => {
      retriedJob = (await runHook.result.current.mutateAsync({
        strategy_name: 'production/range_break_v5',
      } as OptimizationRequest)) as { job_id: string };
    });

    const statusHook = renderHook(() => useOptimizationJobStatus(retriedJob.job_id), { wrapper });
    await waitFor(() => expect(statusHook.result.current.isSuccess).toBe(true));

    expect(vi.mocked(backtestClient.runOptimization).mock.calls.length).toBeGreaterThanOrEqual(2);
    expect(backtestClient.cancelOptimizationJob).toHaveBeenCalledWith('opt-retry-1');
    expect(backtestClient.getOptimizationJobStatus).toHaveBeenCalledWith('opt-retry-2');
  });
});

describe('useCancelOptimization', () => {
  it('cancels optimization job and invalidates job query', async () => {
    vi.mocked(backtestClient.cancelOptimizationJob).mockResolvedValueOnce({
      job_id: 'opt-cancel-1',
      status: 'cancelled',
      message: 'cancelled by user',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelOptimization(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('opt-cancel-1');
    });

    expect(backtestClient.cancelOptimizationJob).toHaveBeenCalledWith('opt-cancel-1');
    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.job('opt-cancel-1'),
    });
  });

  it('logs error when cancellation fails', async () => {
    vi.mocked(backtestClient.cancelOptimizationJob).mockRejectedValueOnce(new Error('cancel failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCancelOptimization(), { wrapper });
    const { logger } = await import('@/utils/logger');

    await act(async () => {
      await expect(result.current.mutateAsync('opt-cancel-1')).rejects.toThrow('cancel failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to cancel optimization', { error: 'cancel failed' });
  });
});

describe('useOptimizationGridConfigs', () => {
  it('fetches all grid configs', async () => {
    vi.mocked(backtestClient.getOptimizationGridConfigs).mockResolvedValueOnce({ configs: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfigs(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationGridConfigs).toHaveBeenCalledWith();
  });
});

describe('useOptimizationGridConfig', () => {
  it('fetches grid config for strategy', async () => {
    vi.mocked(backtestClient.getOptimizationGridConfig).mockResolvedValueOnce({
      strategy_name: 'Alpha',
      grid: [],
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfig('Alpha Strategy'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationGridConfig).toHaveBeenCalledWith('Alpha Strategy');
  });

  it('does not fetch when strategy is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationGridConfig(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useSaveOptimizationGrid', () => {
  it('saves grid config and invalidates queries', async () => {
    vi.mocked(backtestClient.saveOptimizationGridConfig).mockResolvedValueOnce({
      success: true,
      strategy_name: 'Alpha',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useSaveOptimizationGrid(), { wrapper });

    const request = { content: '' } as OptimizationGridSaveRequest;

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'Alpha', request });
    });

    expect(backtestClient.saveOptimizationGridConfig).toHaveBeenCalledWith('Alpha', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfigs() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfig('Alpha') });
  });
});

describe('useDeleteOptimizationGrid', () => {
  it('deletes grid config and clears cache', async () => {
    vi.mocked(backtestClient.deleteOptimizationGridConfig).mockResolvedValueOnce({
      success: true,
      strategy_name: 'Beta',
    });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteOptimizationGrid(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('Beta');
    });

    expect(backtestClient.deleteOptimizationGridConfig).toHaveBeenCalledWith('Beta');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfigs() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.gridConfig('Beta') });
  });
});

describe('useOptimizationHtmlFiles', () => {
  it('fetches optimization HTML files with strategy', async () => {
    vi.mocked(backtestClient.listOptimizationHtmlFiles).mockResolvedValueOnce({ files: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFiles('myStrat'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listOptimizationHtmlFiles).toHaveBeenCalledWith({ strategy: 'myStrat', limit: 100 });
  });

  it('fetches without strategy filter', async () => {
    vi.mocked(backtestClient.listOptimizationHtmlFiles).mockResolvedValueOnce({ files: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFiles(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listOptimizationHtmlFiles).toHaveBeenCalledWith({ strategy: undefined, limit: 100 });
  });
});

describe('useOptimizationHtmlFileContent', () => {
  it('fetches HTML file content', async () => {
    vi.mocked(backtestClient.getOptimizationHtmlFileContent).mockResolvedValueOnce({ content: '<html>' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFileContent('strat', 'opt.html'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationHtmlFileContent).toHaveBeenCalledWith('strat', 'opt.html');
  });

  it('does not fetch when strategy or filename is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFileContent(null, null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useRenameOptimizationHtmlFile', () => {
  it('renames HTML file and invalidates cache', async () => {
    vi.mocked(backtestClient.renameOptimizationHtmlFile).mockResolvedValueOnce({
      old_filename: 'old.html',
      new_filename: 'new.html',
      strategy_name: 'strat',
    } as never);

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

    expect(backtestClient.renameOptimizationHtmlFile).toHaveBeenCalledWith('strat', 'old.html', {
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
    vi.mocked(backtestClient.deleteOptimizationHtmlFile).mockResolvedValueOnce({
      strategy_name: 'strat',
      filename: 'opt.html',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'strat', filename: 'opt.html' });
    });

    expect(backtestClient.deleteOptimizationHtmlFile).toHaveBeenCalledWith('strat', 'opt.html');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.htmlFilesPrefix() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.htmlFileContent('strat', 'opt.html'),
    });
  });
});
