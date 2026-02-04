import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useIndexData, useIndicesList } from './useIndices';

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

describe('useIndices hooks', () => {
  it('useIndicesList fetches indices list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ indices: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useIndicesList(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/indices');
  });

  it('useIndexData fetches index data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useIndexData('topix'), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/indices/topix');
  });

  it('useIndexData is disabled when code is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useIndexData(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
