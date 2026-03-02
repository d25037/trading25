import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { analyticsClient } from '@/lib/analytics-client';
import { createTestWrapper } from '@/test-utils';
import { useSectorStocks } from './useSectorStocks';

vi.mock('@/lib/analytics-client', () => ({
  analyticsClient: {
    getSectorStocks: vi.fn(),
  },
}));

vi.mock('@/utils/logger', () => ({
  logger: { debug: vi.fn(), error: vi.fn() },
}));

afterEach(() => {
  vi.clearAllMocks();
});

describe('useSectorStocks', () => {
  it('fetches sector stocks when enabled', async () => {
    vi.mocked(analyticsClient.getSectorStocks).mockResolvedValueOnce({ items: [] } as never);
    const { wrapper } = createTestWrapper();
    const params = { sector33Name: '輸送用機器' };
    const { result } = renderHook(() => useSectorStocks(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(analyticsClient.getSectorStocks).toHaveBeenCalledWith(expect.objectContaining({ sector33Name: '輸送用機器' }));
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSectorStocks({}, false), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
