import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useTopix100Ranking } from './useTopix100Ranking';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getTopix100Ranking: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useTopix100Ranking', () => {
  it('fetches TOPIX100 ranking data with the selected metric', async () => {
    vi.mocked(analyticsClient.getTopix100Ranking).mockResolvedValueOnce({
      date: '2026-03-30',
      rankingMetric: 'price_sma_20_80',
      itemCount: 0,
      items: [],
      lastUpdated: '2026-03-30T00:00:00Z',
    } as never);

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopix100Ranking('2026-03-30', 'price_sma_20_80', true), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getTopix100Ranking).toHaveBeenCalledWith({
      date: '2026-03-30',
      metric: 'price_sma_20_80',
    });
  });

  it('stays idle when disabled', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopix100Ranking('2026-03-30', 'price_vs_sma20_gap', false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('refetches when the metric changes', async () => {
    vi.mocked(analyticsClient.getTopix100Ranking)
      .mockResolvedValueOnce({
        date: '2026-03-30',
        rankingMetric: 'price_vs_sma20_gap',
        itemCount: 0,
        items: [],
        lastUpdated: '2026-03-30T00:00:00Z',
      } as never)
      .mockResolvedValueOnce({
        date: '2026-03-30',
        rankingMetric: 'price_sma_20_80',
        itemCount: 0,
        items: [],
        lastUpdated: '2026-03-30T00:00:00Z',
      } as never);

    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(
      ({ metric }) => useTopix100Ranking('2026-03-30', metric, true),
      {
        initialProps: { metric: 'price_vs_sma20_gap' as const },
        wrapper,
      }
    );

    await waitFor(() => {
      expect(analyticsClient.getTopix100Ranking).toHaveBeenNthCalledWith(1, {
        date: '2026-03-30',
        metric: 'price_vs_sma20_gap',
      });
    });

    rerender({ metric: 'price_sma_20_80' });

    await waitFor(() => {
      expect(analyticsClient.getTopix100Ranking).toHaveBeenNthCalledWith(2, {
        date: '2026-03-30',
        metric: 'price_sma_20_80',
      });
    });
  });
});
