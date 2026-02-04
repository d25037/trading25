import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useAllTopixData, useTopixData, useTopixDateRange } from './useTopixData';

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

describe('useTopixData', () => {
  it('fetches topix data with options', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopixData({ from: '2024-01-01', to: '2024-12-31' }), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith(
      '/api/chart/indices/topix',
      expect.objectContaining({ from: '2024-01-01', to: '2024-12-31' })
    );
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopixData({ enabled: false }), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});

describe('useAllTopixData', () => {
  it('fetches all topix data', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useAllTopixData(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/chart/indices/topix', expect.any(Object));
  });
});

describe('useTopixDateRange', () => {
  it('fetches topix data for date range', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ data: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useTopixDateRange('2024-01-01', '2024-06-30'), {
      wrapper,
    });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith(
      '/api/chart/indices/topix',
      expect.objectContaining({ from: '2024-01-01', to: '2024-06-30' })
    );
  });
});
