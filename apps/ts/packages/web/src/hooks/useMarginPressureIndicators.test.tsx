import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useMarginPressureIndicators } from './useMarginPressureIndicators';

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

describe('useMarginPressureIndicators', () => {
  it('fetches margin pressure indicators', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginPressureIndicators('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/stocks/7203/margin-pressure', { period: 15 });
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginPressureIndicators(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
