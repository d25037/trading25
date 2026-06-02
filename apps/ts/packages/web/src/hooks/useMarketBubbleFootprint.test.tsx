import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useMarketBubbleFootprint } from './useMarketBubbleFootprint';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useMarketBubbleFootprint', () => {
  it('fetches latest footprint and labels near blowoff horizons', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      date: '2026-05-29',
      markets: ['prime', 'standard', 'growth'],
      overallRegime: 'blowoff_watch',
      overallScore: 4,
      nearBlowoff: true,
      researchExperimentId: 'market-behavior/market-bubble-footprint',
      reratingExperimentId: 'market-behavior/rerating-bubble-regime-forward-response',
      horizons: [
        {
          horizon: 60,
          score: 3,
          regime: 'crowded',
          nearBlowoff: true,
          returnDispersionPercentile: 0.8974,
          activeFlags: ['breadth_narrowing'],
        },
      ],
    });
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useMarketBubbleFootprint({ markets: 'prime,standard,growth' }), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(apiGet).toHaveBeenCalledWith('/api/analytics/market-bubble-footprint/latest', {
      markets: 'prime,standard,growth',
      date: undefined,
    });
    if (!result.current.data) throw new Error('expected latest bubble footprint data');
    expect(result.current.data.horizons[0]?.intensityLabel).toBe('Near blowoff');
    expect(result.current.data.horizons[0]?.activeFlags).toEqual(['breadth_narrowing']);
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarketBubbleFootprint({}, false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('surfaces monitor load errors instead of replacing them with a setup command', async () => {
    vi.mocked(apiGet).mockRejectedValueOnce({
      status: 404,
      message: 'No market price data was available for the requested bubble footprint date.',
    });
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useMarketBubbleFootprint({ markets: 'prime,standard,growth' }), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.data).toBeUndefined();
    expect(result.current.error).toEqual(expect.objectContaining({ status: 404 }));
  });
});
