import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useFundamentalRanking } from './useFundamentalRanking';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
  apiDelete: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useFundamentalRanking', () => {
  it('fetches fundamental ranking data when enabled', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ rankings: {} });
    const { wrapper } = createTestWrapper();
    const params = {
      limit: 20,
      markets: 'prime',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 5,
    };
    const { result } = renderHook(() => useFundamentalRanking(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith(
      '/api/analytics/fundamental-ranking',
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
});
