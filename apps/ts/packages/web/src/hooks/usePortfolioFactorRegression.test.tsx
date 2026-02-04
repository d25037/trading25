import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { usePortfolioFactorRegression } from './usePortfolioFactorRegression';

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

describe('usePortfolioFactorRegression', () => {
  it('fetches portfolio factor regression data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ factors: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => usePortfolioFactorRegression(1), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/portfolio-factor-regression/1', { lookbackDays: 252 });
  });

  it('is disabled when portfolioId is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => usePortfolioFactorRegression(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
