import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useMarginPressureIndicators } from './useMarginPressureIndicators';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getMarginPressureIndicators: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useMarginPressureIndicators', () => {
  it('fetches margin pressure indicators', async () => {
    vi.mocked(analyticsClient.getMarginPressureIndicators).mockResolvedValueOnce({ data: [] } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginPressureIndicators('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getMarginPressureIndicators).toHaveBeenCalledWith({ symbol: '7203', period: 15 });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginPressureIndicators(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('retries and returns error on API failure', async () => {
    vi.mocked(analyticsClient.getMarginPressureIndicators).mockRejectedValue(new Error('network'));
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginPressureIndicators('7203'), { wrapper });

    await waitFor(
      () => {
        expect(result.current.isError).toBe(true);
      },
      { timeout: 9000 }
    );

    expect(analyticsClient.getMarginPressureIndicators).toHaveBeenCalledTimes(4);
  }, 15000);
});
