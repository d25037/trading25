import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { backtestClient } from '@/lib/backtest-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type {
  BacktestRequest,
  DefaultConfigUpdateRequest,
  SignalAttributionRequest,
  StrategyDuplicateRequest,
  StrategyMoveRequest,
  StrategyRenameRequest,
  StrategyUpdateRequest,
  StrategyValidationRequest,
} from '@/types/backtest';
import {
  backtestKeys,
  useAttributionArtifactContent,
  useAttributionArtifactFiles,
  useBacktestHealth,
  useBacktestResult,
  useCancelBacktest,
  useCancelSignalAttribution,
  useDefaultConfig,
  useDeleteHtmlFile,
  useDeleteStrategy,
  useDuplicateStrategy,
  useHtmlFileContent,
  useHtmlFiles,
  useJobStatus,
  useJobs,
  useMoveStrategy,
  useRenameHtmlFile,
  useRenameStrategy,
  useRunBacktest,
  useRunSignalAttribution,
  useSignalAttributionJobStatus,
  useSignalAttributionResult,
  useSignalReference,
  useStrategies,
  useStrategy,
  useUpdateDefaultConfig,
  useUpdateStrategy,
  useValidateStrategy,
} from './useBacktest';

vi.mock('@/lib/backtest-client', () => ({
  backtestClient: {
    healthCheck: vi.fn(),
    listStrategies: vi.fn(),
    getStrategy: vi.fn(),
    listJobs: vi.fn(),
    getJobStatus: vi.fn(),
    getResult: vi.fn(),
    runBacktest: vi.fn(),
    runSignalAttribution: vi.fn(),
    getSignalAttributionJob: vi.fn(),
    getSignalAttributionResult: vi.fn(),
    listAttributionArtifactFiles: vi.fn(),
    getAttributionArtifactContent: vi.fn(),
    listHtmlFiles: vi.fn(),
    getHtmlFileContent: vi.fn(),
    renameHtmlFile: vi.fn(),
    deleteHtmlFile: vi.fn(),
    updateStrategy: vi.fn(),
    deleteStrategy: vi.fn(),
    duplicateStrategy: vi.fn(),
    renameStrategy: vi.fn(),
    moveStrategy: vi.fn(),
    validateStrategy: vi.fn(),
    getDefaultConfig: vi.fn(),
    updateDefaultConfig: vi.fn(),
    getSignalReference: vi.fn(),
    cancelJob: vi.fn(),
    cancelSignalAttributionJob: vi.fn(),
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

describe('backtestKeys', () => {
  it('generates correct query keys', () => {
    expect(backtestKeys.all).toEqual(['backtest']);
    expect(backtestKeys.health()).toEqual(['backtest', 'health']);
    expect(backtestKeys.strategies()).toEqual(['backtest', 'strategies']);
    expect(backtestKeys.strategy('test.yml')).toEqual(['backtest', 'strategies', 'test.yml']);
    expect(backtestKeys.jobs(10)).toEqual(['backtest', 'jobs', 10]);
    expect(backtestKeys.job('j1')).toEqual(['backtest', 'job', 'j1']);
    expect(backtestKeys.result('j1', true)).toEqual(['backtest', 'result', 'j1', true]);
    expect(backtestKeys.attributionJob('j1')).toEqual(['backtest', 'attribution-job', 'j1']);
    expect(backtestKeys.attributionResult('j1')).toEqual(['backtest', 'attribution-result', 'j1']);
    expect(backtestKeys.attributionArtifactFiles('s1', 100)).toEqual([
      'backtest',
      'attribution-artifact-files',
      's1',
      100,
    ]);
    expect(backtestKeys.attributionArtifactContent('s1', 'f1.json')).toEqual([
      'backtest',
      'attribution-artifact-content',
      's1',
      'f1.json',
    ]);
    expect(backtestKeys.htmlFiles('s1')).toEqual(['backtest', 'html-files', 's1']);
    expect(backtestKeys.htmlFileContent('s1', 'f1')).toEqual(['backtest', 'html-file-content', 's1', 'f1']);
    expect(backtestKeys.defaultConfig()).toEqual(['backtest', 'default-config']);
    expect(backtestKeys.signalReference()).toEqual(['backtest', 'signal-reference']);
  });
});

describe('useBacktestHealth', () => {
  it('fetches health status', async () => {
    vi.mocked(backtestClient.healthCheck).mockResolvedValueOnce({ status: 'ok' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestHealth(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.healthCheck).toHaveBeenCalledWith();
  });
});

describe('useStrategies', () => {
  it('fetches strategies list', async () => {
    vi.mocked(backtestClient.listStrategies).mockResolvedValueOnce({ strategies: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategies(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listStrategies).toHaveBeenCalledWith();
  });
});

describe('useStrategy', () => {
  it('fetches strategy details when name is provided', async () => {
    vi.mocked(backtestClient.getStrategy).mockResolvedValueOnce({ name: 'test.yml', content: '' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategy('test.yml'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getStrategy).toHaveBeenCalledWith('test.yml');
  });

  it('does not fetch when name is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategy(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useJobs', () => {
  it('fetches jobs with default limit', async () => {
    vi.mocked(backtestClient.listJobs).mockResolvedValueOnce([] as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobs(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listJobs).toHaveBeenCalledWith(50);
  });

  it('fetches jobs with custom limit', async () => {
    vi.mocked(backtestClient.listJobs).mockResolvedValueOnce([] as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobs(10), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listJobs).toHaveBeenCalledWith(10);
  });
});

describe('useJobStatus', () => {
  it('fetches job status when jobId is provided', async () => {
    vi.mocked(backtestClient.getJobStatus).mockResolvedValueOnce({ job_id: 'j1', status: 'completed' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobStatus('j1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getJobStatus).toHaveBeenCalledWith('j1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('uses 2-second polling while pending/running and stops on completion', () => {
    vi.mocked(backtestClient.getJobStatus).mockResolvedValueOnce({ job_id: 'job-poll', status: 'pending' } as never);

    const { queryClient, wrapper } = createWrapper();
    renderHook(() => useJobStatus('job-poll'), { wrapper });

    const query = queryClient.getQueryCache().find({ queryKey: backtestKeys.job('job-poll') });
    const refetchInterval = (query?.options as { refetchInterval?: unknown } | undefined)?.refetchInterval;

    expect(typeof refetchInterval).toBe('function');
    if (typeof refetchInterval === 'function') {
      expect(refetchInterval({ state: { data: { status: 'pending' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'running' } } } as never)).toBe(2000);
      expect(refetchInterval({ state: { data: { status: 'completed' } } } as never)).toBe(false);
      expect(refetchInterval({ state: { data: undefined } } as never)).toBe(false);
    }
  });
});

describe('useBacktestResult', () => {
  it('fetches backtest result with HTML', async () => {
    vi.mocked(backtestClient.getResult).mockResolvedValueOnce({ result: {} } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult('job-1', true), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getResult).toHaveBeenCalledWith('job-1', true);
  });

  it('fetches backtest result without HTML', async () => {
    vi.mocked(backtestClient.getResult).mockResolvedValueOnce({ result: {} } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getResult).toHaveBeenCalledWith('job-1', false);
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('signal attribution hooks', () => {
  it('runs signal attribution and invalidates attribution job cache', async () => {
    vi.mocked(backtestClient.runSignalAttribution).mockResolvedValueOnce({
      job_id: 'attr-1',
      status: 'pending',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRunSignalAttribution(), { wrapper });

    const request = { strategy_name: 'test.yml' } as SignalAttributionRequest;
    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(backtestClient.runSignalAttribution).toHaveBeenCalledWith(request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.attributionJob('attr-1') });
  });

  it('fetches attribution job status when jobId is provided', async () => {
    vi.mocked(backtestClient.getSignalAttributionJob).mockResolvedValueOnce({
      job_id: 'attr-1',
      status: 'completed',
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionJobStatus('attr-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getSignalAttributionJob).toHaveBeenCalledWith('attr-1');
  });

  it('does not fetch attribution job status when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('fetches attribution result when jobId is provided', async () => {
    vi.mocked(backtestClient.getSignalAttributionResult).mockResolvedValueOnce({
      job_id: 'attr-1',
      result: {},
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionResult('attr-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getSignalAttributionResult).toHaveBeenCalledWith('attr-1');
  });

  it('cancels attribution job and invalidates job query', async () => {
    vi.mocked(backtestClient.cancelSignalAttributionJob).mockResolvedValueOnce({
      job_id: 'attr-1',
      status: 'cancelled',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelSignalAttribution(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('attr-1');
    });

    expect(backtestClient.cancelSignalAttributionJob).toHaveBeenCalledWith('attr-1');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.attributionJob('attr-1') });
  });
});

describe('useRunBacktest', () => {
  it('runs backtest and invalidates job list', async () => {
    vi.mocked(backtestClient.runBacktest).mockResolvedValueOnce({ job_id: 'job-1', status: 'running' } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRunBacktest(), { wrapper });

    const request = { strategy_name: 'test.yml' } as BacktestRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(backtestClient.runBacktest).toHaveBeenCalledWith(request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.jobs() });
  });
});

describe('useCancelBacktest', () => {
  it('cancels backtest and invalidates queries', async () => {
    vi.mocked(backtestClient.cancelJob).mockResolvedValueOnce({ job_id: 'j1', status: 'cancelled' } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelBacktest(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('j1');
    });

    expect(backtestClient.cancelJob).toHaveBeenCalledWith('j1');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.job('j1') });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.jobs() });
  });

  it('handles 409 conflict error gracefully', async () => {
    const error409 = Object.assign(new Error('Conflict'), { status: 409 });
    vi.mocked(backtestClient.cancelJob).mockRejectedValueOnce(error409);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelBacktest(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync('j1');
      } catch {
        // expected
      }
    });

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.jobs() });
  });
});

describe('useUpdateStrategy', () => {
  it('updates strategy and invalidates caches', async () => {
    vi.mocked(backtestClient.updateStrategy).mockResolvedValueOnce({ success: true } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateStrategy(), { wrapper });

    const request = { config: { content: 'new content' } } as StrategyUpdateRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(backtestClient.updateStrategy).toHaveBeenCalledWith('test.yml', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('test.yml') });
  });
});

describe('useDeleteStrategy', () => {
  it('deletes strategy and clears cache', async () => {
    vi.mocked(backtestClient.deleteStrategy).mockResolvedValueOnce({ success: true } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteStrategy(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('test.yml');
    });

    expect(backtestClient.deleteStrategy).toHaveBeenCalledWith('test.yml');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('test.yml') });
  });
});

describe('useDuplicateStrategy', () => {
  it('duplicates strategy and invalidates list', async () => {
    vi.mocked(backtestClient.duplicateStrategy).mockResolvedValueOnce({ new_strategy_name: 'test-copy.yml' } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDuplicateStrategy(), { wrapper });

    const request = { new_name: 'test-copy.yml' } as StrategyDuplicateRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(backtestClient.duplicateStrategy).toHaveBeenCalledWith('test.yml', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
  });
});

describe('useRenameStrategy', () => {
  it('renames strategy and updates cache', async () => {
    vi.mocked(backtestClient.renameStrategy).mockResolvedValueOnce({ new_name: 'renamed.yml' } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useRenameStrategy(), { wrapper });

    const request = { new_name: 'renamed.yml' } as StrategyRenameRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'old.yml', request });
    });

    expect(backtestClient.renameStrategy).toHaveBeenCalledWith('old.yml', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('old.yml') });
  });
});

describe('useMoveStrategy', () => {
  it('moves strategy and updates cache', async () => {
    vi.mocked(backtestClient.moveStrategy).mockResolvedValueOnce({
      new_strategy_name: 'production/old.yml',
      target_category: 'production',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useMoveStrategy(), { wrapper });

    const request = { target_category: 'production' } as StrategyMoveRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'experimental/old.yml', request });
    });

    expect(backtestClient.moveStrategy).toHaveBeenCalledWith('experimental/old.yml', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('experimental/old.yml') });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('production/old.yml') });
  });
});

describe('useValidateStrategy', () => {
  it('validates strategy config', async () => {
    vi.mocked(backtestClient.validateStrategy).mockResolvedValueOnce({ valid: true, errors: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useValidateStrategy(), { wrapper });

    const request = { config: { content: 'yaml content' } } as StrategyValidationRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(backtestClient.validateStrategy).toHaveBeenCalledWith('test.yml', request);
  });
});

describe('useHtmlFiles', () => {
  it('fetches HTML files list', async () => {
    vi.mocked(backtestClient.listHtmlFiles).mockResolvedValueOnce({ files: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFiles('myStrategy'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listHtmlFiles).toHaveBeenCalledWith({ strategy: 'myStrategy', limit: 100 });
  });
});

describe('useAttributionArtifactFiles', () => {
  it('fetches attribution artifact file list', async () => {
    vi.mocked(backtestClient.listAttributionArtifactFiles).mockResolvedValueOnce({ files: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useAttributionArtifactFiles('experimental/range_break_v18'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.listAttributionArtifactFiles).toHaveBeenCalledWith({
      strategy: 'experimental/range_break_v18',
      limit: 100,
    });
  });
});

describe('useAttributionArtifactContent', () => {
  it('fetches attribution artifact content', async () => {
    vi.mocked(backtestClient.getAttributionArtifactContent).mockResolvedValueOnce({ artifact: {} } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(
      () => useAttributionArtifactContent('experimental/range_break_v18', 'attribution_20260112_120000_job-1.json'),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getAttributionArtifactContent).toHaveBeenCalledWith(
      'experimental/range_break_v18',
      'attribution_20260112_120000_job-1.json'
    );
  });

  it('does not fetch when strategy or filename is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useAttributionArtifactContent(null, null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useHtmlFileContent', () => {
  it('fetches HTML file content', async () => {
    vi.mocked(backtestClient.getHtmlFileContent).mockResolvedValueOnce({ content: '<html>' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFileContent('strat', 'report.html'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getHtmlFileContent).toHaveBeenCalledWith('strat', 'report.html');
  });

  it('does not fetch when strategy or filename is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFileContent(null, null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useRenameHtmlFile', () => {
  it('renames HTML file and invalidates cache', async () => {
    vi.mocked(backtestClient.renameHtmlFile).mockResolvedValueOnce({
      old_filename: 'old.html',
      new_filename: 'new.html',
      strategy_name: 'strat',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useRenameHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        strategy: 'strat',
        filename: 'old.html',
        request: { new_filename: 'new.html' },
      });
    });

    expect(backtestClient.renameHtmlFile).toHaveBeenCalledWith('strat', 'old.html', {
      new_filename: 'new.html',
    });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.htmlFiles() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: backtestKeys.htmlFileContent('strat', 'old.html'),
    });
  });
});

describe('useDeleteHtmlFile', () => {
  it('deletes HTML file and invalidates cache', async () => {
    vi.mocked(backtestClient.deleteHtmlFile).mockResolvedValueOnce({
      strategy_name: 'strat',
      filename: 'report.html',
    } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'strat', filename: 'report.html' });
    });

    expect(backtestClient.deleteHtmlFile).toHaveBeenCalledWith('strat', 'report.html');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.htmlFiles() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: backtestKeys.htmlFileContent('strat', 'report.html'),
    });
  });
});

describe('useDefaultConfig', () => {
  it('fetches default config', async () => {
    vi.mocked(backtestClient.getDefaultConfig).mockResolvedValueOnce({ content: 'yaml: true' } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDefaultConfig(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getDefaultConfig).toHaveBeenCalledWith();
  });

  it('does not fetch when disabled', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDefaultConfig(false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useUpdateDefaultConfig', () => {
  it('updates default config and invalidates cache', async () => {
    vi.mocked(backtestClient.updateDefaultConfig).mockResolvedValueOnce({ success: true } as never);

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateDefaultConfig(), { wrapper });

    const request = { content: 'new: config' } as DefaultConfigUpdateRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(backtestClient.updateDefaultConfig).toHaveBeenCalledWith(request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.defaultConfig() });
  });
});

describe('useSignalReference', () => {
  it('fetches signal reference data', async () => {
    vi.mocked(backtestClient.getSignalReference).mockResolvedValueOnce({ signals: [] } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalReference(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(backtestClient.getSignalReference).toHaveBeenCalledWith();
  });
});
