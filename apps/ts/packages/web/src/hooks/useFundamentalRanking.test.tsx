import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useFundamentalRanking } from './useFundamentalRanking';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: { getFundamentalRanking: vi.fn() },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => vi.clearAllMocks());

describe('useFundamentalRanking', () => {
  it('normalizes and forwards the enabled forecast filter', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useFundamentalRanking(
          {
            limit: 20,
            markets: 'prime',
            metricKey: 'eps_forecast_to_actual',
            forecastAboveRecentFyActuals: true,
            forecastLookbackFyCount: 100,
          },
          true
        ),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith({
      limit: 20,
      markets: 'prime',
      metricKey: 'eps_forecast_to_actual',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 20,
    });
  });

  it('omits lookback when the forecast filter is disabled', async () => {
    vi.mocked(analyticsClient.getFundamentalRanking).mockResolvedValueOnce({ rankings: {} } as never);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () => useFundamentalRanking({ forecastAboveRecentFyActuals: false, forecastLookbackFyCount: 10 }, true),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentalRanking).toHaveBeenCalledWith({
      limit: undefined,
      markets: undefined,
      metricKey: undefined,
      forecastAboveRecentFyActuals: false,
      forecastLookbackFyCount: undefined,
    });
  });

  it('does not fetch while disabled', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentalRanking({}, false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getFundamentalRanking).not.toHaveBeenCalled();
  });
});
