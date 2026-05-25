import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  ListWatchlistsResponse,
  WatchlistCreateRequest,
  WatchlistDeleteResponse,
  WatchlistItemCreateRequest,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistSummaryResponse,
  WatchlistUpdateRequest,
  WatchlistWithItemsResponse,
} from '@trading25/contracts/types/api-response-types';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import { logger } from '@/utils/logger';

// Fetch functions
const fetchWatchlists = () => apiGet<ListWatchlistsResponse>('/api/watchlist');

const fetchWatchlistWithItems = (id: number) => apiGet<WatchlistWithItemsResponse>(`/api/watchlist/${id}`);

const fetchWatchlistPrices = (id: number) => apiGet<WatchlistPricesResponse>(`/api/watchlist/${id}/prices`);

// Mutation functions
const createWatchlist = (data: WatchlistCreateRequest) => apiPost<WatchlistSummaryResponse>('/api/watchlist', data);

const updateWatchlist = (id: number, data: WatchlistUpdateRequest) =>
  apiPut<WatchlistSummaryResponse>(`/api/watchlist/${id}`, data);

const deleteWatchlist = (id: number) => apiDelete<WatchlistDeleteResponse>(`/api/watchlist/${id}`);

const addWatchlistItem = (watchlistId: number, data: WatchlistItemCreateRequest) =>
  apiPost<WatchlistItemResponse>(`/api/watchlist/${watchlistId}/items`, data);

const removeWatchlistItem = (watchlistId: number, itemId: number) =>
  apiDelete<WatchlistDeleteResponse>(`/api/watchlist/${watchlistId}/items/${itemId}`);

// Query hooks
export function useWatchlists() {
  return useQuery({
    queryKey: ['watchlists'],
    queryFn: () => {
      logger.debug('Fetching watchlists list');
      return fetchWatchlists();
    },
    staleTime: 30 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}

export function useWatchlistWithItems(id: number | null) {
  return useQuery({
    queryKey: ['watchlist', id],
    queryFn: () => {
      if (id === null) {
        throw new Error('Watchlist ID is required');
      }
      logger.debug('Fetching watchlist details', { id });
      return fetchWatchlistWithItems(id);
    },
    enabled: id !== null,
    staleTime: 30 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}

export function useWatchlistPrices(id: number | null) {
  return useQuery({
    queryKey: ['watchlist-prices', id],
    queryFn: () => {
      if (id === null) {
        throw new Error('Watchlist ID is required');
      }
      logger.debug('Fetching watchlist prices', { id });
      return fetchWatchlistPrices(id);
    },
    enabled: id !== null,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000,
  });
}

// Mutation hooks
export function useCreateWatchlist() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: createWatchlist,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlists'] });
      logger.debug('Watchlist created successfully');
    },
    onError: (error) => {
      logger.error('Failed to create watchlist', { error: error.message });
    },
  });
}

export function useUpdateWatchlist() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: number; data: WatchlistUpdateRequest }) => updateWatchlist(id, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['watchlists'] });
      queryClient.invalidateQueries({ queryKey: ['watchlist', variables.id] });
      logger.debug('Watchlist updated successfully');
    },
    onError: (error) => {
      logger.error('Failed to update watchlist', { error: error.message });
    },
  });
}

export function useDeleteWatchlist() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: deleteWatchlist,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['watchlists'] });
      logger.debug('Watchlist deleted successfully');
    },
    onError: (error) => {
      logger.error('Failed to delete watchlist', { error: error.message });
    },
  });
}

export function useAddWatchlistItem() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ watchlistId, data }: { watchlistId: number; data: WatchlistItemCreateRequest }) =>
      addWatchlistItem(watchlistId, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['watchlists'] });
      queryClient.invalidateQueries({ queryKey: ['watchlist', variables.watchlistId] });
      queryClient.invalidateQueries({ queryKey: ['watchlist-prices', variables.watchlistId] });
      logger.debug('Stock added to watchlist successfully');
    },
    onError: (error) => {
      logger.error('Failed to add stock to watchlist', { error: error.message });
    },
  });
}

export function useRemoveWatchlistItem() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ watchlistId, itemId }: { watchlistId: number; itemId: number }) =>
      removeWatchlistItem(watchlistId, itemId),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['watchlists'] });
      queryClient.invalidateQueries({ queryKey: ['watchlist', variables.watchlistId] });
      queryClient.invalidateQueries({ queryKey: ['watchlist-prices', variables.watchlistId] });
      logger.debug('Stock removed from watchlist successfully');
    },
    onError: (error) => {
      logger.error('Failed to remove stock from watchlist', { error: error.message });
    },
  });
}
