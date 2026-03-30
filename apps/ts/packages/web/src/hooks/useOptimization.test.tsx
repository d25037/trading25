import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { backtestClient } from '@/lib/backtest-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type {
  OptimizationRequest,
  StrategyOptimizationSaveRequest,
  StrategyOptimizationStateResponse,
} from '@/types/backtest';
import { logger } from '@/utils/logger';
import {
  optimizationKeys,
  useCancelOptimization,
  useDeleteStrategyOptimization,
  useDeleteOptimizationHtmlFile,
  useGenerateStrategyOptimizationDraft,
  useOptimizationHtmlFileContent,
  useOptimizationHtmlFiles,
  useOptimizationJobStatus,
  useRenameOptimizationHtmlFile,
  useRunOptimization,
  useSaveStrategyOptimization,
  useStrategyOptimization,
} from './useOptimization';

vi.mock('@/lib/backtest-client', () => ({
  backtestClient: {
    runOptimization: vi.fn(),
    getOptimizationJobStatus: vi.fn(),
    cancelOptimizationJob: vi.fn(),
    getStrategyOptimization: vi.fn(),
    generateStrategyOptimizationDraft: vi.fn(),
    saveStrategyOptimization: vi.fn(),
    deleteStrategyOptimization: vi.fn(),
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

function createOptimizationState(
  overrides: Partial<StrategyOptimizationStateResponse> = {}
): StrategyOptimizationStateResponse {
  return {
    strategy_name: 'production/demo',
    persisted: true,
    source: 'saved',
    optimization: { parameter_ranges: {} },
    yaml_content: 'description: demo\nparameter_ranges: {}\n',
    valid: true,
    ready_to_run: true,
    param_count: 1,
    combinations: 3,
    errors: [],
    warnings: [],
    drift: [],
    ...overrides,
  };
}

describe('optimizationKeys', () => {
  it('generates correct query keys', () => {
    expect(optimizationKeys.all).toEqual(['optimization']);
    expect(optimizationKeys.job('j1')).toEqual(['optimization', 'job', 'j1']);
    expect(optimizationKeys.strategySpec('s1')).toEqual(['optimization', 'strategy-spec', 's1']);
    expect(optimizationKeys.htmlFilesPrefix()).toEqual(['optimization', 'html-files']);
    expect(optimizationKeys.htmlFiles('s1')).toEqual(['optimization', 'html-files', 's1']);
    expect(optimizationKeys.htmlFileContent('s1', 'f1')).toEqual(['optimization', 'html-file-content', 's1', 'f1']);
  });
});

describe('optimization job hooks', () => {
  it('runs optimization', async () => {
    vi.mocked(backtestClient.runOptimization).mockResolvedValueOnce({ job_id: 'opt-1', status: 'running' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRunOptimization(), { wrapper });

    const request = { strategy_name: 'production/demo' } as OptimizationRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(backtestClient.runOptimization).toHaveBeenCalledWith(request);
  });

  it('logs run optimization errors', async () => {
    vi.mocked(backtestClient.runOptimization).mockRejectedValueOnce(new Error('run failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRunOptimization(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ strategy_name: 'production/demo' } as OptimizationRequest)).rejects.toThrow(
        'run failed'
      );
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to start optimization', {
      error: 'run failed',
    });
  });

  it('fetches job status and polls only while running', async () => {
    vi.mocked(backtestClient.getOptimizationJobStatus).mockResolvedValueOnce({
      job_id: 'opt-1',
      status: 'completed',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationJobStatus('opt-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationJobStatus).toHaveBeenCalledWith('opt-1');

    const query = queryClient.getQueryCache().find({ queryKey: optimizationKeys.job('opt-1') });
    const refetchInterval = (query?.options as { refetchInterval?: unknown } | undefined)?.refetchInterval;
    expect(typeof refetchInterval).toBe('function');
    if (typeof refetchInterval === 'function') {
      expect(refetchInterval({ state: { data: { status: 'running' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'completed' } } } as never)).toBe(false);
    }
  });

  it('throws when optimization job status queryFn runs without a job id', async () => {
    const { queryClient, wrapper } = createWrapper();
    renderHook(() => useOptimizationJobStatus(null), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: optimizationKeys.job('') });
    const queryFn = (query?.options as { queryFn?: () => Promise<unknown> } | undefined)?.queryFn;

    expect(() => queryFn?.()).toThrow('Job ID required');
  });

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

  it('logs cancel optimization errors', async () => {
    vi.mocked(backtestClient.cancelOptimizationJob).mockRejectedValueOnce(new Error('cancel failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCancelOptimization(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync('opt-cancel-1')).rejects.toThrow('cancel failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to cancel optimization', {
      error: 'cancel failed',
    });
  });
});

describe('strategy optimization hooks', () => {
  it('fetches strategy optimization state', async () => {
    vi.mocked(backtestClient.getStrategyOptimization).mockResolvedValueOnce(createOptimizationState() as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategyOptimization('production/demo'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getStrategyOptimization).toHaveBeenCalledWith('production/demo');
  });

  it('does not fetch strategy optimization when strategy is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategyOptimization(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('throws when strategy optimization queryFn runs without a strategy name', async () => {
    const { queryClient, wrapper } = createWrapper();
    renderHook(() => useStrategyOptimization(null), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: optimizationKeys.strategySpec('') });
    const queryFn = (query?.options as { queryFn?: () => Promise<unknown> } | undefined)?.queryFn;

    expect(() => queryFn?.()).toThrow('Strategy name required');
  });

  it('generates a strategy-linked draft', async () => {
    vi.mocked(backtestClient.generateStrategyOptimizationDraft).mockResolvedValueOnce(
      createOptimizationState({ persisted: false, source: 'draft', ready_to_run: false }) as never
    );

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useGenerateStrategyOptimizationDraft(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('production/demo');
    });

    expect(backtestClient.generateStrategyOptimizationDraft).toHaveBeenCalledWith('production/demo');
  });

  it('logs strategy draft generation errors', async () => {
    vi.mocked(backtestClient.generateStrategyOptimizationDraft).mockRejectedValueOnce(new Error('draft failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useGenerateStrategyOptimizationDraft(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync('production/demo')).rejects.toThrow('draft failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to generate strategy optimization draft', {
      error: 'draft failed',
    });
  });

  it('saves strategy optimization and invalidates strategy query', async () => {
    vi.mocked(backtestClient.saveStrategyOptimization).mockResolvedValueOnce(createOptimizationState() as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useSaveStrategyOptimization(), { wrapper });

    const request = { yaml_content: 'description: demo\nparameter_ranges: {}\n' } as StrategyOptimizationSaveRequest;

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'production/demo', request });
    });

    expect(backtestClient.saveStrategyOptimization).toHaveBeenCalledWith('production/demo', request);
    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.strategySpec('production/demo'),
    });
  });

  it('logs save strategy optimization errors', async () => {
    vi.mocked(backtestClient.saveStrategyOptimization).mockRejectedValueOnce(new Error('save failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSaveStrategyOptimization(), { wrapper });

    await act(async () => {
      await expect(
        result.current.mutateAsync({
          strategy: 'production/demo',
          request: { yaml_content: 'description: demo\nparameter_ranges: {}\n' },
        })
      ).rejects.toThrow('save failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to save strategy optimization', {
      error: 'save failed',
    });
  });

  it('deletes strategy optimization and invalidates strategy query', async () => {
    vi.mocked(backtestClient.deleteStrategyOptimization).mockResolvedValueOnce({
      success: true,
      strategy_name: 'production/demo',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeleteStrategyOptimization(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('production/demo');
    });

    expect(backtestClient.deleteStrategyOptimization).toHaveBeenCalledWith('production/demo');
    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.strategySpec('production/demo'),
    });
  });

  it('logs delete strategy optimization errors', async () => {
    vi.mocked(backtestClient.deleteStrategyOptimization).mockRejectedValueOnce(new Error('delete failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDeleteStrategyOptimization(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync('production/demo')).rejects.toThrow('delete failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to delete strategy optimization', {
      error: 'delete failed',
    });
  });
});

describe('optimization html hooks', () => {
  it('fetches optimization HTML files with strategy', async () => {
    vi.mocked(backtestClient.listOptimizationHtmlFiles).mockResolvedValueOnce({ files: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFiles('myStrat'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listOptimizationHtmlFiles).toHaveBeenCalledWith({ strategy: 'myStrat', limit: 100 });
  });

  it('fetches HTML file content', async () => {
    vi.mocked(backtestClient.getOptimizationHtmlFileContent).mockResolvedValueOnce({ content: '<html>' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useOptimizationHtmlFileContent('strat', 'opt.html'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getOptimizationHtmlFileContent).toHaveBeenCalledWith('strat', 'opt.html');
  });

  it('throws when HTML file content queryFn runs without required parameters', async () => {
    const { queryClient, wrapper } = createWrapper();
    renderHook(() => useOptimizationHtmlFileContent(null, null), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: optimizationKeys.htmlFileContent('', '') });
    const queryFn = (query?.options as { queryFn?: () => Promise<unknown> } | undefined)?.queryFn;

    expect(() => queryFn?.()).toThrow('Strategy and filename required');
  });

  it('renames HTML file and clears stale content cache', async () => {
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

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.htmlFilesPrefix() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.htmlFileContent('strat', 'old.html'),
    });
  });

  it('logs rename HTML file errors', async () => {
    vi.mocked(backtestClient.renameOptimizationHtmlFile).mockRejectedValueOnce(new Error('rename failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useRenameOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await expect(
        result.current.mutateAsync({
          strategy: 'strat',
          filename: 'old.html',
          request: { new_filename: 'new.html' },
        })
      ).rejects.toThrow('rename failed');
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to rename optimization HTML file', {
      error: 'rename failed',
    });
  });

  it('deletes HTML file and invalidates file list', async () => {
    vi.mocked(backtestClient.deleteOptimizationHtmlFile).mockResolvedValueOnce({
      success: true,
      strategy_name: 'strat',
      filename: 'old.html',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'strat', filename: 'old.html' });
    });

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: optimizationKeys.htmlFilesPrefix() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: optimizationKeys.htmlFileContent('strat', 'old.html'),
    });
  });

  it('logs delete HTML file errors', async () => {
    vi.mocked(backtestClient.deleteOptimizationHtmlFile).mockRejectedValueOnce(new Error('delete html failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDeleteOptimizationHtmlFile(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ strategy: 'strat', filename: 'old.html' })).rejects.toThrow(
        'delete html failed'
      );
    });

    expect(logger.error).toHaveBeenCalledWith('Failed to delete optimization HTML file', {
      error: 'delete html failed',
    });
  });
});
