import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import { createQueryWrapper, createTestQueryClient } from '@/test-utils';
import type { CreatePortfolioItemRequest, CreatePortfolioRequest, UpdatePortfolioRequest } from '@/types/portfolio';
import {
  useAddPortfolioItem,
  useCreatePortfolio,
  useDeletePortfolio,
  useDeletePortfolioItem,
  usePortfolios,
  usePortfolioWithItems,
  useUpdatePortfolio,
  useUpdatePortfolioItem,
} from './usePortfolio';

vi.mock('@/lib/api-client', () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  apiPut: vi.fn(),
  apiDelete: vi.fn(),
}));

vi.mock('@/utils/logger', () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

const createWrapper = () => {
  const queryClient = createTestQueryClient();
  return {
    queryClient,
    wrapper: createQueryWrapper(queryClient),
  };
};

describe('usePortfolio hooks', () => {
  it('fetches portfolio list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ portfolios: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePortfolios(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/portfolio');
  });

  it('fetches portfolio details when id is provided', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ id: 1, name: 'Main', items: [] });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePortfolioWithItems(1), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/portfolio/1');
  });

  it('does not fetch portfolio details when id is null', () => {
    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePortfolioWithItems(null), { wrapper });

    expect(result.current.fetchStatus).toBe('idle');
  });

  it('creates portfolio and invalidates list', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ id: 1, name: 'Growth' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCreatePortfolio(), { wrapper });

    const request = { name: 'Growth' } as CreatePortfolioRequest;

    await act(async () => {
      await result.current.mutateAsync(request);
    });

    expect(apiPost).toHaveBeenCalledWith('/api/portfolio', request);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
  });

  it('updates portfolio and invalidates both list and detail', async () => {
    vi.mocked(apiPut).mockResolvedValueOnce({ id: 1, name: 'Updated' });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdatePortfolio(), { wrapper });

    const data: UpdatePortfolioRequest = { name: 'Updated' };

    await act(async () => {
      await result.current.mutateAsync({ id: 1, data });
    });

    expect(apiPut).toHaveBeenCalledWith('/api/portfolio/1', data);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolio', 1] });
  });

  it('deletes portfolio and invalidates list', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeletePortfolio(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(1);
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/portfolio/1');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
  });

  it('adds stock to portfolio and invalidates caches', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ id: 10, code: '7203', quantity: 100 });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useAddPortfolioItem(), { wrapper });

    const data: CreatePortfolioItemRequest = {
      code: '7203',
      quantity: 100,
      purchasePrice: 2500,
      purchaseDate: '2025-01-30',
    };

    await act(async () => {
      await result.current.mutateAsync({ portfolioId: 1, data });
    });

    expect(apiPost).toHaveBeenCalledWith('/api/portfolio/1/items', data);
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolio', 1] });
  });

  it('updates portfolio item and invalidates caches', async () => {
    vi.mocked(apiPut).mockResolvedValueOnce({ id: 10, code: '7203', quantity: 200 });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdatePortfolioItem(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ portfolioId: 1, itemId: 10, data: { quantity: 200 } });
    });

    expect(apiPut).toHaveBeenCalledWith('/api/portfolio/1/items/10', { quantity: 200 });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolio', 1] });
  });

  it('deletes portfolio item and invalidates caches', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true });

    const { queryClient, wrapper } = createWrapper();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeletePortfolioItem(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync({ portfolioId: 1, itemId: 10 });
    });

    expect(apiDelete).toHaveBeenCalledWith('/api/portfolio/1/items/10');
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolios'] });
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['portfolio', 1] });
  });

  it('logs error on create failure', async () => {
    vi.mocked(apiPost).mockRejectedValueOnce(new Error('Server error'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useCreatePortfolio(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync({ name: 'Fail' });
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to create portfolio', { error: 'Server error' });
  });

  it('logs error on delete failure', async () => {
    vi.mocked(apiDelete).mockRejectedValueOnce(new Error('Not found'));

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useDeletePortfolio(), { wrapper });

    await act(async () => {
      try {
        await result.current.mutateAsync(999);
      } catch {
        // expected
      }
    });

    const { logger } = await import('@/utils/logger');
    expect(logger.error).toHaveBeenCalledWith('Failed to delete portfolio', { error: 'Not found' });
  });
});
