import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { usePortfolioPerformance } from './usePortfolioPerformance';

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

describe('usePortfolioPerformance', () => {
  it('fetches portfolio performance data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ summary: {}, holdings: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => usePortfolioPerformance(1), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/portfolio/1/performance', expect.any(Object));
  });

  it('is disabled when portfolioId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => usePortfolioPerformance(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('passes custom options', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ summary: {}, holdings: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => usePortfolioPerformance(1, { benchmarkCode: 'topix', lookbackDays: 60 }), {
      wrapper,
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith(
      '/api/portfolio/1/performance',
      expect.objectContaining({ benchmarkCode: 'topix', lookbackDays: 60 })
    );
  });
});
