import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useFactorRegression } from './useFactorRegression';

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

describe('useFactorRegression', () => {
  it('fetches factor regression data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ factors: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/factor-regression/7203', { lookbackDays: 252 });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
