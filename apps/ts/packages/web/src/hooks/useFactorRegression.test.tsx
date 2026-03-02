import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useFactorRegression } from './useFactorRegression';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getFactorRegression: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useFactorRegression', () => {
  it('fetches factor regression data', async () => {
    vi.mocked(analyticsClient.getFactorRegression).mockResolvedValueOnce({ factors: [] } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFactorRegression).toHaveBeenCalledWith({ symbol: '7203', lookbackDays: 252 });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('is disabled when enabled option is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression('7203', { enabled: false }), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getFactorRegression).not.toHaveBeenCalled();
  });
});
