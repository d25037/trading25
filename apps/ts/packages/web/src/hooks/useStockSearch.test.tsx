import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useStockSearch } from './useStockSearch';

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

describe('useStockSearch', () => {
  it('fetches search results for valid query', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ results: [{ code: '7203', name: 'Toyota' }] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockSearch('toyota'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/stocks/search', expect.objectContaining({ q: 'toyota' }));
  });

  it('is disabled for empty query', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockSearch(''), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('is disabled for whitespace-only query', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockSearch('   '), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('respects enabled option', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useStockSearch('test', { enabled: false }), {
      wrapper,
    });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
