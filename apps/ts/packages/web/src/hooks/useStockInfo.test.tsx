import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useStockInfo } from './useStockInfo';

vi.mock('@/lib/api-client', () => ({
  ApiError: class MockApiError extends Error {
    constructor(
      message: string,
      public readonly status: number,
      public readonly details?: unknown
    ) {
      super(message);
      this.name = 'ApiError';
    }

    isClientError() {
      return this.status >= 400 && this.status < 500;
    }
  },
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

describe('useStockInfo', () => {
  it('fetches stock info from the market endpoint', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ code: '7203', companyName: 'Toyota Motor' });
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useStockInfo('7203'), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/market/stocks/7203');
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useStockInfo(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('does not retry client errors', async () => {
    vi.mocked(apiGet).mockRejectedValue(new ApiError('Not found', 404));
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useStockInfo('7203'), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(apiGet).toHaveBeenCalledTimes(1);
  });

  it('retries server errors twice before failing', async () => {
    vi.mocked(apiGet).mockRejectedValue(new ApiError('Server error', 500));
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(() => useStockInfo('7203'), { wrapper });

    await waitFor(() => expect(apiGet).toHaveBeenCalledTimes(3), { timeout: 5000 });
    expect(result.current.isError).toBe(true);
    expect(apiGet).toHaveBeenCalledTimes(3);
  });
});
