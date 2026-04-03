import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useCostStructureAnalysis } from './useCostStructureAnalysis';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getCostStructureAnalysis: vi.fn(),
  },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useCostStructureAnalysis', () => {
  it('fetches cost structure analysis', async () => {
    vi.mocked(analyticsClient.getCostStructureAnalysis).mockResolvedValueOnce({ symbol: '7203' } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCostStructureAnalysis('7203'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getCostStructureAnalysis).toHaveBeenCalledWith({
      symbol: '7203',
      view: 'recent',
      windowQuarters: 12,
    });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCostStructureAnalysis(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useCostStructureAnalysis('7203', { enabled: false }), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getCostStructureAnalysis).not.toHaveBeenCalled();
  });

  it('forwards custom analysis view options', async () => {
    vi.mocked(analyticsClient.getCostStructureAnalysis).mockResolvedValueOnce({ symbol: '7203' } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(
      () => useCostStructureAnalysis('7203', { view: 'same_quarter', windowQuarters: 20 }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getCostStructureAnalysis).toHaveBeenCalledWith({
      symbol: '7203',
      view: 'same_quarter',
      windowQuarters: 20,
    });
  });
});
