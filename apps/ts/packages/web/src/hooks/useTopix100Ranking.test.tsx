import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import type { Topix100RankingMetric } from '@/types/ranking';
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
      smaWindow: 50,
      shortWindowStreaks: 3,
      longWindowStreaks: 53,
      longScoreHorizonDays: 5,
      shortScoreHorizonDays: 1,
      intradayScoreTarget: 'next_session_open_close',
      scoreModelType: 'daily_refit',
      scoreTrainWindowDays: 756,
      scoreTestWindowDays: 1,
      scoreStepDays: 1,
      scoreSplitTrainStart: '2023-01-04',
      scoreSplitTrainEnd: '2025-12-30',
      scoreSplitTestStart: null,
      scoreSplitTestEnd: null,
      scoreSplitPartialTail: false,
      scoreSourceRunId: '20260406_180623_c0eb7f87',
      itemCount: 0,
      items: [],
      lastUpdated: '2026-03-30T00:00:00Z',
    } as never);

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopix100Ranking('2026-03-30', 'price_sma_20_80', 50, true), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getTopix100Ranking).toHaveBeenCalledWith({
      date: '2026-03-30',
      metric: 'price_sma_20_80',
      smaWindow: 50,
    });
  });

  it('stays idle when disabled', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopix100Ranking('2026-03-30', 'price_vs_sma_gap', 50, false), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('refetches when the metric or sma window changes', async () => {
    vi.mocked(analyticsClient.getTopix100Ranking)
      .mockResolvedValueOnce({
        date: '2026-03-30',
        rankingMetric: 'price_vs_sma_gap',
        smaWindow: 50,
        shortWindowStreaks: 3,
        longWindowStreaks: 53,
        longScoreHorizonDays: 5,
        shortScoreHorizonDays: 1,
        intradayScoreTarget: 'next_session_open_close',
        scoreModelType: 'daily_refit',
        scoreTrainWindowDays: 756,
        scoreTestWindowDays: 1,
        scoreStepDays: 1,
        scoreSplitTrainStart: '2023-01-04',
        scoreSplitTrainEnd: '2025-12-30',
        scoreSplitTestStart: null,
        scoreSplitTestEnd: null,
        scoreSplitPartialTail: false,
        scoreSourceRunId: '20260406_180623_c0eb7f87',
        itemCount: 0,
        items: [],
        lastUpdated: '2026-03-30T00:00:00Z',
      } as never)
      .mockResolvedValueOnce({
        date: '2026-03-30',
        rankingMetric: 'price_vs_sma_gap',
        smaWindow: 100,
        shortWindowStreaks: 3,
        longWindowStreaks: 53,
        longScoreHorizonDays: 5,
        shortScoreHorizonDays: 1,
        intradayScoreTarget: 'next_session_open_close',
        scoreModelType: 'daily_refit',
        scoreTrainWindowDays: 756,
        scoreTestWindowDays: 1,
        scoreStepDays: 1,
        scoreSplitTrainStart: '2023-01-04',
        scoreSplitTrainEnd: '2025-12-30',
        scoreSplitTestStart: null,
        scoreSplitTestEnd: null,
        scoreSplitPartialTail: false,
        scoreSourceRunId: '20260406_180623_c0eb7f87',
        itemCount: 0,
        items: [],
        lastUpdated: '2026-03-30T00:00:00Z',
      } as never);

    const { wrapper } = createTestWrapper();
    const { rerender } = renderHook(
      ({ metric, smaWindow }: { metric: Topix100RankingMetric; smaWindow: 20 | 50 | 100 }) =>
        useTopix100Ranking('2026-03-30', metric, smaWindow, true),
      {
        initialProps: { metric: 'price_vs_sma_gap' as Topix100RankingMetric, smaWindow: 50 as 20 | 50 | 100 },
        wrapper,
      }
    );

    await waitFor(() => {
      expect(analyticsClient.getTopix100Ranking).toHaveBeenNthCalledWith(1, {
        date: '2026-03-30',
        metric: 'price_vs_sma_gap',
        smaWindow: 50,
      });
    });

    rerender({ metric: 'price_vs_sma_gap', smaWindow: 100 });

    await waitFor(() => {
      expect(analyticsClient.getTopix100Ranking).toHaveBeenNthCalledWith(2, {
        date: '2026-03-30',
        metric: 'price_vs_sma_gap',
        smaWindow: 100,
      });
    });
  });
});
