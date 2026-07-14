import { renderHook, waitFor } from '@testing-library/react';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { shouldRetryFundamentals, useFundamentals } from './useFundamentals';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getFundamentals: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useFundamentals', () => {
  it.each([404, 409, 422])('does not retry HTTP %i failures', (status) => {
    const error = new HttpRequestError('request rejected', 'http', { status });

    expect(shouldRetryFundamentals(0, error)).toBe(false);
  });

  it.each([
    new HttpRequestError('network unavailable', 'network'),
    new HttpRequestError('request timed out', 'timeout'),
    new HttpRequestError('server unavailable', 'http', { status: 503 }),
  ])('retries transient failures only while failureCount is below two', (error) => {
    expect(shouldRetryFundamentals(0, error)).toBe(true);
    expect(shouldRetryFundamentals(1, error)).toBe(true);
    expect(shouldRetryFundamentals(2, error)).toBe(false);
  });

  it('fetches fundamentals data', async () => {
    vi.mocked(analyticsClient.getFundamentals).mockResolvedValueOnce({ asOfDate: '2024-06-28', roe: 10, per: 15 } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentals).toHaveBeenCalledWith({
      symbol: '7203',
      tradingValuePeriod: 15,
      forecastEpsLookbackFyCount: 3,
    });
  });

  it('uses custom trading value period', async () => {
    vi.mocked(analyticsClient.getFundamentals).mockResolvedValueOnce({ asOfDate: '2024-06-28', roe: 10, per: 15 } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(
      () => useFundamentals('7203', { tradingValuePeriod: 30, forecastEpsLookbackFyCount: 5 }),
      { wrapper }
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentals).toHaveBeenCalledWith({
      symbol: '7203',
      tradingValuePeriod: 30,
      forecastEpsLookbackFyCount: 5,
    });
  });

  it('normalizes invalid trading value period to default', async () => {
    vi.mocked(analyticsClient.getFundamentals).mockResolvedValueOnce({ asOfDate: '2024-06-28', roe: 10, per: 15 } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { tradingValuePeriod: Number.NaN }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentals).toHaveBeenCalledWith({
      symbol: '7203',
      tradingValuePeriod: 15,
      forecastEpsLookbackFyCount: 3,
    });
  });

  it('normalizes fractional trading value period to a minimum integer', async () => {
    vi.mocked(analyticsClient.getFundamentals).mockResolvedValueOnce({ asOfDate: '2024-06-28', roe: 10, per: 15 } as never);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { tradingValuePeriod: 0.6 }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getFundamentals).toHaveBeenCalledWith({
      symbol: '7203',
      tradingValuePeriod: 1,
      forecastEpsLookbackFyCount: 3,
    });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('is disabled when enabled option is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { enabled: false }), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getFundamentals).not.toHaveBeenCalled();
  });
});
