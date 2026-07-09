import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { normalizeRankingSymbol, useRankingSymbolSnapshot } from './useRankingSymbolSnapshot';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getMarketRankingSymbol: vi.fn(),
  },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('normalizeRankingSymbol', () => {
  it('normalizes whitespace, case, and legacy five-character codes', () => {
    expect(normalizeRankingSymbol(' 72030 ')).toBe('7203');
    expect(normalizeRankingSymbol('285A0')).toBe('285A');
    expect(normalizeRankingSymbol(null)).toBeNull();
  });
});

describe('useRankingSymbolSnapshot', () => {
  it('clears previous symbol data while the next symbol is pending', async () => {
    let resolveSecond: ((value: { date: string; item: null; lastUpdated: string }) => void) | undefined;
    const second = new Promise<{ date: string; item: null; lastUpdated: string }>((resolve) => {
      resolveSecond = resolve;
    });
    vi.mocked(analyticsClient.getMarketRankingSymbol)
      .mockResolvedValueOnce({ date: '2026-07-09', item: null, lastUpdated: 'first' })
      .mockReturnValueOnce(second as never);
    const { wrapper } = createTestWrapper();
    const { result, rerender } = renderHook(
      ({ symbol }: { symbol: string | null }) => useRankingSymbolSnapshot(symbol),
      { initialProps: { symbol: '7203' }, wrapper }
    );

    await waitFor(() => expect(result.current.data?.lastUpdated).toBe('first'));
    rerender({ symbol: '6758' });

    expect(result.current.data).toBeUndefined();
    expect(analyticsClient.getMarketRankingSymbol).toHaveBeenLastCalledWith('6758');

    await act(async () => {
      resolveSecond?.({ date: '2026-07-10', item: null, lastUpdated: 'second' });
      await second;
    });
    await waitFor(() => expect(result.current.data?.lastUpdated).toBe('second'));
  });

  it('does not fetch when the normalized symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useRankingSymbolSnapshot(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getMarketRankingSymbol).not.toHaveBeenCalled();
  });
});
