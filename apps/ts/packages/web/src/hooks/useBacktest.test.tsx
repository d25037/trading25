import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ApiError, apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type {
  BacktestRequest,
  DefaultConfigUpdateRequest,
  SignalAttributionRequest,
  StrategyDuplicateRequest,
  StrategyRenameRequest,
  StrategyUpdateRequest,
  StrategyValidationRequest,
} from '@/types/backtest';
import {
  backtestKeys,
  useBacktestHealth,
  useBacktestResult,
  useCancelBacktest,
  useDefaultConfig,
  useDeleteHtmlFile,
  useDeleteStrategy,
  useDuplicateStrategy,
  useHtmlFileContent,
  useHtmlFiles,
  useSignalAttributionJobStatus,
  useSignalAttributionResult,
  useJobStatus,
  useJobs,
  useRenameHtmlFile,
  useRenameStrategy,
  useRunBacktest,
  useRunSignalAttribution,
  useCancelSignalAttribution,
  useSignalReference,
  useStrategies,
  useStrategy,
  useUpdateDefaultConfig,
  useUpdateStrategy,
  useValidateStrategy,
} from './useBacktest';

vi.mock('@/lib/api-client', () => {
  class MockApiError extends Error {
    status: number;
    constructor(message: string, status: number) {
      super(message);
      this.name = 'ApiError';
      this.status = status;
    }
  }
  return {
    apiGet: vi.fn(),
    apiPost: vi.fn(),
    apiPut: vi.fn(),
    apiDelete: vi.fn(),
    ApiError: MockApiError,
  };
});

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
    expect(backtestKeys.htmlFiles('s1')).toEqual(['backtest', 'html-files', 's1']);
    expect(backtestKeys.htmlFileContent('s1', 'f1')).toEqual(['backtest', 'html-file-content', 's1', 'f1']);
    expect(backtestKeys.defaultConfig()).toEqual(['backtest', 'default-config']);
    expect(backtestKeys.signalReference()).toEqual(['backtest', 'signal-reference']);
  });
});

describe('useBacktestHealth', () => {
  it('fetches health status', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ status: 'ok' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestHealth(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/health');
  });
});

describe('useStrategies', () => {
  it('fetches strategies list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ strategies: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategies(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/strategies');
  });
});

describe('useStrategy', () => {
  it('fetches strategy details when name is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ name: 'test.yml', content: '' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategy('test.yml'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/strategies/test.yml');
  });

  it('does not fetch when name is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useStrategy(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useJobs', () => {
  it('fetches jobs with default limit', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobs(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/jobs?limit=50');
  });

  it('fetches jobs with custom limit', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobs(10), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/jobs?limit=10');
  });
});

describe('useJobStatus', () => {
  it('fetches job status when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ job_id: 'j1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobStatus('j1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/jobs/j1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useBacktestResult', () => {
  it('fetches backtest result with HTML', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ result: {} });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult('job-1', true), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/result/job-1?include_html=true');
  });

  it('fetches backtest result without HTML', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ result: {} });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult('job-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/result/job-1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useBacktestResult(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('signal attribution hooks', () => {
  it('runs signal attribution and invalidates attribution job cache', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'attr-1', status: 'pending' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRunSignalAttribution(), { wrapper });

    const request = { strategy_name: 'test.yml' } as SignalAttributionRequest;
    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/backtest/attribution/run', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.attributionJob('attr-1') });
  });

  it('fetches attribution job status when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ job_id: 'attr-1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionJobStatus('attr-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/attribution/jobs/attr-1');
  });

  it('does not fetch attribution job status when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('fetches attribution result when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ job_id: 'attr-1', result: {} });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalAttributionResult('attr-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/attribution/result/attr-1');
  });

  it('cancels attribution job and invalidates job query', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'attr-1', status: 'cancelled' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelSignalAttribution(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('attr-1');
    });

    expect(apiPost).toHaveBeenCalledWith('/api/backtest/attribution/jobs/attr-1/cancel');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.attributionJob('attr-1') });
  });
});

describe('useRunBacktest', () => {
  it('runs backtest and invalidates job list', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'job-1', status: 'running' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRunBacktest(), { wrapper });

    const request = { strategy_name: 'test.yml' } as BacktestRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/backtest/run', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.jobs() });
  });
});

describe('useCancelBacktest', () => {
  it('cancels backtest and invalidates queries', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'j1', status: 'cancelled' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelBacktest(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('j1');
    });

    expect(apiPost).toHaveBeenCalledWith('/api/backtest/jobs/j1/cancel');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.job('j1') });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.jobs() });
  });

  it('handles 409 conflict error gracefully', async () => {
    const error409 = new ApiError('Conflict', 409);
    vi.mocked(apiPost).mockRejectedValueOnce(error409);

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
    vi.mocked(apiPut).mockResolvedValueOnce({ success: true });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateStrategy(), { wrapper });

    const request = { config: { content: 'new content' } } as StrategyUpdateRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(apiPut).toHaveBeenCalledWith('/api/strategies/test.yml', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('test.yml') });
  });
});

describe('useDeleteStrategy', () => {
  it('deletes strategy and clears cache', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteStrategy(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('test.yml');
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/strategies/test.yml');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('test.yml') });
  });
});

describe('useDuplicateStrategy', () => {
  it('duplicates strategy and invalidates list', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ new_strategy_name: 'test-copy.yml' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDuplicateStrategy(), { wrapper });

    const request = { new_name: 'test-copy.yml' } as StrategyDuplicateRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/strategies/test.yml/duplicate', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
  });
});

describe('useRenameStrategy', () => {
  it('renames strategy and updates cache', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ new_name: 'renamed.yml' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useRenameStrategy(), { wrapper });

    const request = { new_name: 'renamed.yml' } as StrategyRenameRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'old.yml', request });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/strategies/old.yml/rename', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategies() });
    expect(removeSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.strategy('old.yml') });
  });
});

describe('useValidateStrategy', () => {
  it('validates strategy config', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ valid: true, errors: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useValidateStrategy(), { wrapper });

    const request = { config: { content: 'yaml content' } } as StrategyValidationRequest;

    await act(async () => {
      await result.current.mutateAsync({ name: 'test.yml', request });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/strategies/test.yml/validate', request);
  });
});

describe('useHtmlFiles', () => {
  it('fetches HTML files list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ files: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFiles('myStrategy'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/html-files?strategy=myStrategy&limit=100');
  });
});

describe('useHtmlFileContent', () => {
  it('fetches HTML file content', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ content: '<html>' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFileContent('strat', 'report.html'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/backtest/html-files/strat/report.html');
  });

  it('does not fetch when strategy or filename is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHtmlFileContent(null, null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useRenameHtmlFile', () => {
  it('renames HTML file and invalidates cache', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      old_filename: 'old.html',
      new_filename: 'new.html',
      strategy_name: 'strat',
    });

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

    expect(apiPost).toHaveBeenCalledWith('/api/backtest/html-files/strat/old.html/rename', {
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
    vi.mocked(apiDelete).mockResolvedValueOnce({
      strategy_name: 'strat',
      filename: 'report.html',
    });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const removeSpy = vi.spyOn(queryClient, 'removeQueries');
    const { result } = renderHook(() => useDeleteHtmlFile(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ strategy: 'strat', filename: 'report.html' });
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/backtest/html-files/strat/report.html');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.htmlFiles() });
    expect(removeSpy).toHaveBeenCalledWith({
      queryKey: backtestKeys.htmlFileContent('strat', 'report.html'),
    });
  });
});

describe('useDefaultConfig', () => {
  it('fetches default config', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ content: 'yaml: true' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDefaultConfig(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/config/default');
  });

  it('does not fetch when disabled', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDefaultConfig(false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useUpdateDefaultConfig', () => {
  it('updates default config and invalidates cache', async () => {
    vi.mocked(apiPut).mockResolvedValueOnce({ success: true });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateDefaultConfig(), { wrapper });

    const request = { content: 'new: config' } as DefaultConfigUpdateRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPut).toHaveBeenCalledWith('/api/config/default', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: backtestKeys.defaultConfig() });
  });
});

describe('useSignalReference', () => {
  it('fetches signal reference data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ signals: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useSignalReference(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/signals/reference');
  });
});
