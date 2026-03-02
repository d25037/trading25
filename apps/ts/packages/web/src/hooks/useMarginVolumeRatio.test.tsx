import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useMarginVolumeRatio } from './useMarginVolumeRatio';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getMarginVolumeRatio: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useMarginVolumeRatio', () => {
  it('fetches margin volume ratio data', async () => {
    vi.mocked(analyticsClient.getMarginVolumeRatio).mockResolvedValueOnce({ data: [] } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginVolumeRatio('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getMarginVolumeRatio).toHaveBeenCalledWith({ symbol: '7203' });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginVolumeRatio(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('retries and returns error on API failure', async () => {
    vi.mocked(analyticsClient.getMarginVolumeRatio).mockRejectedValue(new Error('network'));
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginVolumeRatio('7203'), { wrapper });

    await waitFor(
      () => {
        expect(result.current.isError).toBe(true);
      },
      { timeout: 9000 }
    );

    expect(analyticsClient.getMarginVolumeRatio).toHaveBeenCalledTimes(4);
  }, 15000);
});
