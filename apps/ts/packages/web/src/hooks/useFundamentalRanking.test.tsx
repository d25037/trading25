import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useFundamentalRanking } from './useFundamentalRanking';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getFundamentalRanking: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useFundamentalRanking', () => {
  it('fetches fundamental ranking data when enabled', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();
    const params = {
      limit: 20,
      markets: 'prime',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 5,
    };
    const { result } = renderHook(() => useFundamentalRanking(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        limit: 20,
        markets: 'prime',
        forecastAboveRecentFyActuals: true,
        forecastLookbackFyCount: 5,
      })
    );
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentalRanking({}, false), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('omits lookback parameter when forecast filter is disabled', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();
    const params = {
      limit: 20,
      markets: 'prime',
      forecastAboveRecentFyActuals: false,
      forecastLookbackFyCount: 10,
    };
    const { result } = renderHook(() => useFundamentalRanking(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        limit: 20,
        markets: 'prime',
        forecastAboveRecentFyActuals: false,
      })
    );
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.not.objectContaining({
        forecastLookbackFyCount: 10,
      })
    );
  });

  it('uses default lookback count when filter is enabled and lookback is missing', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () => useFundamentalRanking({ markets: 'prime', forecastAboveRecentFyActuals: true }, true),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        markets: 'prime',
        forecastAboveRecentFyActuals: true,
        forecastLookbackFyCount: 3,
      })
    );
  });

  it('clamps lookback count to supported range when filter is enabled', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useFundamentalRanking(
          {
            markets: 'prime',
            forecastAboveRecentFyActuals: true,
            forecastLookbackFyCount: 100,
          },
          true
        ),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        markets: 'prime',
        forecastAboveRecentFyActuals: true,
        forecastLookbackFyCount: 20,
      })
    );
  });

  it('supports legacy forecastAboveAllActuals flag when new flag is absent', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useFundamentalRanking(
          {
            markets: 'prime',
            forecastAboveAllActuals: true,
            forecastLookbackFyCount: 2,
          },
          true
        ),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith(
      expect.objectContaining({
        markets: 'prime',
        forecastAboveRecentFyActuals: true,
        forecastLookbackFyCount: 2,
      })
    );
  });
});
