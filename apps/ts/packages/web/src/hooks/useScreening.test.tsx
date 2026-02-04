import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiGet } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import { useScreening } from './useScreening';

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

describe('useScreening', () => {
  it('fetches screening data when enabled', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ items: [] });
    const { wrapper } = createTestWrapper();
    const params = { limit: 50 };
    const { result } = renderHook(() => useScreening(params, true), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/analytics/screening', expect.objectContaining({ limit: 50 }));
  });

  it('is disabled when enabled is false', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useScreening({}, false), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
