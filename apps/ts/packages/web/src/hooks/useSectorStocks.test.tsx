import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useSectorStocks } from './useSectorStocks';

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

describe('useSectorStocks', () => {
  it('fetches sector stocks when enabled', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ items: [] });
    const { wrapper } = createTestWrapper();
    const params = { sector33Name: '輸送用機器' };
    const { result } = renderHook(() => useSectorStocks(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith(
      '/api/analytics/sector-stocks',
      expect.objectContaining({ sector33Name: '輸送用機器' })
    );
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useSectorStocks({}, false), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
