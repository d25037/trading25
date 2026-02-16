import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { apiGet, apiPost } from '@/lib/api-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type { LabEvolveRequest, LabGenerateRequest, LabImproveRequest, LabOptimizeRequest } from '@/types/backtest';
import {
  labKeys,
  useCancelLabJob,
  useLabEvolve,
  useLabGenerate,
  useLabImprove,
  useLabJobStatus,
  useLabJobs,
  useLabOptimize,
} from './useLab';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
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

describe('labKeys', () => {
  it('generates correct query keys', () => {
    expect(labKeys.all).toEqual(['lab']);
    expect(labKeys.jobsAll()).toEqual(['lab', 'jobs']);
    expect(labKeys.jobs(25)).toEqual(['lab', 'jobs', 25]);
    expect(labKeys.job('j1')).toEqual(['lab', 'job', 'j1']);
  });
});

describe('useLabGenerate', () => {
  it('calls POST /api/lab/generate', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'gen-1', status: 'pending' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useLabGenerate(), { wrapper });

    const request: LabGenerateRequest = { count: 5, top: 3, direction: 'longonly' };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/lab/generate', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.jobsAll() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('gen-1') });
  });

  it('logs error on failure', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Network error'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useLabGenerate(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync({ count: 1 });
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to start lab generate', { error: 'Network error' });
  });
});

describe('useLabEvolve', () => {
  it('calls POST /api/lab/evolve', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'evo-1', status: 'pending' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useLabEvolve(), { wrapper });

    const request: LabEvolveRequest = { strategy_name: 'my-strategy', generations: 10, population: 20 };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/lab/evolve', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.jobsAll() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('evo-1') });
  });
});

describe('useLabOptimize', () => {
  it('calls POST /api/lab/optimize', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'opt-1', status: 'pending' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useLabOptimize(), { wrapper });

    const request: LabOptimizeRequest = { strategy_name: 'my-strategy', trials: 50, sampler: 'tpe' };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/lab/optimize', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.jobsAll() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('opt-1') });
  });
});

describe('useLabImprove', () => {
  it('calls POST /api/lab/improve', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'imp-1', status: 'pending' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useLabImprove(), { wrapper });

    const request: LabImproveRequest = { strategy_name: 'my-strategy', auto_apply: true };

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/lab/improve', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.jobsAll() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('imp-1') });
  });
});

describe('useLabJobs', () => {
  it('fetches lab jobs list with limit', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce([{ job_id: 'lab-1', status: 'completed' }]);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useLabJobs(20), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/lab/jobs?limit=20');
  });
});

describe('useLabJobStatus', () => {
  it('fetches job status when jobId is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ job_id: 'gen-1', status: 'completed' });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useLabJobStatus('gen-1'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/lab/jobs/gen-1');
  });

  it('does not fetch when jobId is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useLabJobStatus(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('disables polling when SSE is connected', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useLabJobStatus('gen-1', true), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useCancelLabJob', () => {
  it('cancels job and invalidates cache', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ job_id: 'gen-1', status: 'cancelled' });

    const { queryClient, wrapper } = createWrapper();
    const cancelSpy = vi.spyOn(queryClient, 'cancelQueries');
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCancelLabJob(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync('gen-1');
    });

    expect(apiPost).toHaveBeenCalledWith('/api/lab/jobs/gen-1/cancel');
    expect(cancelSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('gen-1') });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.jobsAll() });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: labKeys.job('gen-1') });
  });

  it('logs error on cancel failure', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Cancel failed'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCancelLabJob(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync('gen-1');
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to cancel lab job', { error: 'Cancel failed' });
  });
});
