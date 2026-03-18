import { renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { apiGet, ApiError } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useN225Options } from './useN225Options';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  ApiError: class extends Error {
    constructor(
      message: string,
      public readonly status: number
    ) {
      super(message);
      this.name = 'ApiError';
    }

    isClientError() {
      return this.status >= 400 && this.status < 500;
    }
  },
}));

describe('useN225Options', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches options explorer payload with date filter', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({
      requestedDate: '2026-03-18',
      resolvedDate: '2026-03-18',
      lastUpdated: '2026-03-18T00:00:00Z',
      sourceCallCount: 2,
      availableContractMonths: ['2026-04'],
      items: [],
      summary: {
        totalCount: 0,
        putCount: 0,
        callCount: 0,
        totalVolume: 0,
        totalOpenInterest: 0,
        strikePriceRange: { min: null, max: null },
        underlyingPriceRange: { min: null, max: null },
        settlementPriceRange: { min: null, max: null },
      },
    });
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useN225Options({ date: '2026-03-18' }), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/jquants/options/225', { date: '2026-03-18' });
  });

  it('does not retry client errors', async () => {
    vi.mocked(apiGet).mockRejectedValueOnce(new ApiError('Not found', 404));
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useN225Options({ date: '2026-03-18' }), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(apiGet).toHaveBeenCalledTimes(1);
  });
});
