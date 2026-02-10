import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useFundamentals } from './useFundamentals';

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

describe('useFundamentals', () => {
  it('fetches fundamentals data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ roe: 10, per: 15 });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/fundamentals/7203', {
      tradingValuePeriod: 15,
    });
  });

  it('uses custom trading value period', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ roe: 10, per: 15 });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { tradingValuePeriod: 30 }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/fundamentals/7203', {
      tradingValuePeriod: 30,
    });
  });

  it('normalizes invalid trading value period to default', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ roe: 10, per: 15 });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { tradingValuePeriod: Number.NaN }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/fundamentals/7203', {
      tradingValuePeriod: 15,
    });
  });

  it('normalizes fractional trading value period to a minimum integer', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ roe: 10, per: 15 });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFundamentals('7203', { tradingValuePeriod: 0.6 }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/fundamentals/7203', {
      tradingValuePeriod: 1,
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
    expect(apiGet).not.toHaveBeenCalled();
  });
});
