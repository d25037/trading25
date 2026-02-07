import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiPost } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { btOHLCVKeys, useBtOHLCV, useMultiTimeframeBtOHLCV } from './useBtOHLCV';

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

const mockOHLCVResponse = {
  stock_code: '7203',
  timeframe: 'daily',
  meta: { source_bars: 100, resampled_bars: 100 },
  data: [
    { date: '2025-01-02', open: 1000, high: 1100, low: 950, close: 1050, volume: 100000 },
    { date: '2025-01-03', open: 1050, high: 1150, low: 1000, close: 1100, volume: 120000 },
  ],
};

describe('useBtOHLCV', () => {
  it('fetches OHLCV data via apps/bt/ API', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce(mockOHLCVResponse);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useBtOHLCV({
          stockCode: '7203',
          timeframe: 'daily',
          relativeMode: false,
        }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.data).not.toBeNull());

    expect(apiPost).toHaveBeenCalledWith('/api/ohlcv/resample', {
      stock_code: '7203',
      source: 'market',
      timeframe: 'daily',
    });

    expect(result.current.data).toHaveLength(2);
    expect(result.current.data?.[0]).toEqual({
      time: '2025-01-02',
      open: 1000,
      high: 1100,
      low: 950,
      close: 1050,
      volume: 100000,
    });
  });

  it('includes benchmark_code for relativeMode', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({
      ...mockOHLCVResponse,
      benchmark_code: 'topix',
    });
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useBtOHLCV({
          stockCode: '7203',
          timeframe: 'weekly',
          relativeMode: true,
        }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.data).not.toBeNull());

    expect(apiPost).toHaveBeenCalledWith('/api/ohlcv/resample', {
      stock_code: '7203',
      source: 'market',
      timeframe: 'weekly',
      benchmark_code: 'topix',
      relative_options: {
        handle_zero_division: 'skip',
      },
    });
  });

  it('is disabled when stockCode is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(
      () =>
        useBtOHLCV({
          stockCode: null,
          timeframe: 'daily',
        }),
      { wrapper }
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
    expect(apiPost).not.toHaveBeenCalled();
  });

  it('is disabled when enabled=false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(
      () =>
        useBtOHLCV({
          stockCode: '7203',
          timeframe: 'daily',
          enabled: false,
        }),
      { wrapper }
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
    expect(apiPost).not.toHaveBeenCalled();
  });

  it('returns error on API failure', async () => {
    const error = new Error('API Error');
    vi.mocked(apiPost).mockRejectedValue(error);
    const { wrapper } = createTestWrapper();

    const { result } = renderHook(
      () =>
        useBtOHLCV({
          stockCode: '7203',
          timeframe: 'daily',
        }),
      { wrapper }
    );

    // retry: 2設定のため、エラーになるまで時間がかかる
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 5000 });
    expect(result.current.error?.message).toBe('API Error');
  });
});

describe('useMultiTimeframeBtOHLCV', () => {
  it('fetches daily, weekly, and monthly data', async () => {
    const dailyResponse = { ...mockOHLCVResponse, timeframe: 'daily' };
    const weeklyResponse = {
      ...mockOHLCVResponse,
      timeframe: 'weekly',
      data: [{ date: '2025-01-06', open: 1000, high: 1200, low: 900, close: 1150, volume: 500000 }],
    };
    const monthlyResponse = {
      ...mockOHLCVResponse,
      timeframe: 'monthly',
      data: [{ date: '2025-01-31', open: 1000, high: 1300, low: 800, close: 1200, volume: 2000000 }],
    };

    vi.mocked(apiPost)
      .mockResolvedValueOnce(dailyResponse)
      .mockResolvedValueOnce(weeklyResponse)
      .mockResolvedValueOnce(monthlyResponse);

    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMultiTimeframeBtOHLCV('7203', false), { wrapper });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.daily.data).toHaveLength(2);
    expect(result.current.weekly.data).toHaveLength(1);
    expect(result.current.monthly.data).toHaveLength(1);
  });

  it('is disabled when stockCode is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMultiTimeframeBtOHLCV(null, false), { wrapper });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.daily.data).toBeNull();
    expect(result.current.weekly.data).toBeNull();
    expect(result.current.monthly.data).toBeNull();
    expect(apiPost).not.toHaveBeenCalled();
  });
});

describe('btOHLCVKeys', () => {
  it('generates correct query keys', () => {
    expect(btOHLCVKeys.all).toEqual(['bt-ohlcv']);
    expect(btOHLCVKeys.resample('7203', 'daily', false)).toEqual(['bt-ohlcv', 'resample', '7203', 'daily', false]);
    expect(btOHLCVKeys.resample('6758', 'weekly', true)).toEqual(['bt-ohlcv', 'resample', '6758', 'weekly', true]);
  });
});
