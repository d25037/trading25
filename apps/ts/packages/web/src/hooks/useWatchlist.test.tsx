import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import { createTestWrapper } from '@/test-utils';
import {
  useAddWatchlistItem,
  useCreateWatchlist,
  useDeleteWatchlist,
  useRemoveWatchlistItem,
  useUpdateWatchlist,
  useWatchlistPrices,
  useWatchlists,
  useWatchlistWithItems,
} from './useWatchlist';

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

describe('useWatchlist hooks', () => {
  it('useWatchlists fetches list', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ watchlists: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useWatchlists(), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/watchlist');
  });

  it('useWatchlistWithItems fetches details', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ id: 1, name: 'Test', items: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useWatchlistWithItems(1), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/watchlist/1');
  });

  it('useWatchlistWithItems is disabled when id is null', () => {
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useWatchlistWithItems(null), { wrapper });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('useWatchlistPrices fetches prices', async () => {
    vi.mocked(apiGet).mockResolvedValueOnce({ prices: [] });
    const { wrapper } = createTestWrapper();
    const { result } = renderHook(() => useWatchlistPrices(1), { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(apiGet).toHaveBeenCalledWith('/api/watchlist/1/prices');
  });

  it('useCreateWatchlist creates and invalidates', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ id: 1, name: 'New' });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useCreateWatchlist(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync({ name: 'New' });
    });
    expect(apiPost).toHaveBeenCalledWith('/api/watchlist', { name: 'New' });
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlists'] });
  });

  it('useUpdateWatchlist updates and invalidates', async () => {
    vi.mocked(apiPut).mockResolvedValueOnce({ id: 1, name: 'Updated' });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateWatchlist(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: 1, data: { name: 'Updated' } });
    });
    expect(apiPut).toHaveBeenCalledWith('/api/watchlist/1', { name: 'Updated' });
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlists'] });
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlist', 1] });
  });

  it('useDeleteWatchlist deletes and invalidates', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useDeleteWatchlist(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync(1);
    });
    expect(apiDelete).toHaveBeenCalledWith('/api/watchlist/1');
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlists'] });
  });

  it('useAddWatchlistItem adds item and invalidates', async () => {
    vi.mocked(apiPost).mockResolvedValueOnce({ id: 1 });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useAddWatchlistItem(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync({ watchlistId: 1, data: { code: '7203' } });
    });
    expect(apiPost).toHaveBeenCalledWith('/api/watchlist/1/items', { code: '7203' });
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlist', 1] });
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlist-prices', 1] });
  });

  it('useRemoveWatchlistItem removes item and invalidates', async () => {
    vi.mocked(apiDelete).mockResolvedValueOnce({ success: true });
    const { queryClient, wrapper } = createTestWrapper();
    const spy = vi.spyOn(queryClient, 'invalidateQueries');
    const { result } = renderHook(() => useRemoveWatchlistItem(), { wrapper });
    await act(async () => {
      await result.current.mutateAsync({ watchlistId: 1, itemId: 5 });
    });
    expect(apiDelete).toHaveBeenCalledWith('/api/watchlist/1/items/5');
    expect(spy).toHaveBeenCalledWith({ queryKey: ['watchlist', 1] });
  });
});
