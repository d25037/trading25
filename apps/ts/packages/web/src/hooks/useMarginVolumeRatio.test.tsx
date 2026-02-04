import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useMarginVolumeRatio } from './useMarginVolumeRatio';

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

describe('useMarginVolumeRatio', () => {
  it('fetches margin volume ratio data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginVolumeRatio('7203'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/stocks/7203/margin-ratio');
  });

  it('is disabled when symbol is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useMarginVolumeRatio(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
