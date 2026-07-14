import { renderHook, waitFor } from '@testing-library/react';
import type { FactorRegressionResponse } from '@trading25/contracts/types/api-response-types';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useFactorRegression } from './useFactorRegression';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getFactorRegression: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useFactorRegression', () => {
  it('fetches factor regression data', async () => {
    const response: FactorRegressionResponse = {
      analysisDate: '2026-07-14',
      dataPoints: 252,
      dateRange: { from: '2025-07-14', to: '2026-07-14' },
      marketBeta: 1.05,
      marketRSquared: 0.7,
      sector17Matches: [],
      sector33Matches: [],
      stockCode: '7203',
      topixStyleMatches: [],
    };
    vi.mocked(analyticsClient.getFactorRegression).mockResolvedValueOnce(response);
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.companyName).toBeUndefined();
    expect(analyticsClient.getFactorRegression).toHaveBeenCalledWith({ symbol: '7203', lookbackDays: 252 });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('is disabled when enabled option is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useFactorRegression('7203', { enabled: false }), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
    expect(analyticsClient.getFactorRegression).not.toHaveBeenCalled();
  });
});
