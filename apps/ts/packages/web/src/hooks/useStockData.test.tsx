import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useStockData, useStockDataMultiple } from './useStockData';

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

describe('useStockData', () => {
  it('fetches stock data with mapped timeframe', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ candlestickData: [], indicators: {} });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockData('7203', '1D'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/stocks/7203', { timeframe: 'daily' });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockData(null, '1D'), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('passes through unmapped timeframe values', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ candlestickData: [], indicators: {} });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockData('7203', 'daily'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/stocks/7203', { timeframe: 'daily' });
  });
});

describe('useStockDataMultiple', () => {
  it('fetches multiple symbols', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ candlestickData: [] }).mockResolvedValueOnce({ candlestickData: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockDataMultiple(['7203', '6758'], '1D'), {
      wrapper,
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });

  it('is disabled when symbols array is empty', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockDataMultiple([], '1D'), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
