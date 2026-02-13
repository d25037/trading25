import { BaseApiClient } from './base-client.js';
import type {
  ListWatchlistsResponse,
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  WatchlistResponse,
  WatchlistWithItemsResponse,
} from './types.js';

export class WatchlistClient extends BaseApiClient {
  /**
   * List all watchlists with summary statistics
   */
  async listWatchlists(): Promise<ListWatchlistsResponse> {
    return this.request<ListWatchlistsResponse>('/api/watchlist');
  }

  /**
   * Create a new watchlist
   */
  async createWatchlist(data: { name: string; description?: string }): Promise<WatchlistResponse> {
    return this.request<WatchlistResponse>('/api/watchlist', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Get watchlist with all items
   */
  async getWatchlist(id: number): Promise<WatchlistWithItemsResponse> {
    return this.request<WatchlistWithItemsResponse>(`/api/watchlist/${id}`);
  }

  /**
   * Delete watchlist
   */
  async deleteWatchlist(id: number): Promise<WatchlistDeleteResponse> {
    return this.request<WatchlistDeleteResponse>(`/api/watchlist/${id}`, {
      method: 'DELETE',
    });
  }

  /**
   * Add item to watchlist
   */
  async addWatchlistItem(watchlistId: number, data: { code: string; memo?: string }): Promise<WatchlistItemResponse> {
    return this.request<WatchlistItemResponse>(`/api/watchlist/${watchlistId}/items`, {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete item from watchlist
   */
  async deleteWatchlistItem(watchlistId: number, itemId: number): Promise<WatchlistDeleteResponse> {
    return this.request<WatchlistDeleteResponse>(`/api/watchlist/${watchlistId}/items/${itemId}`, {
      method: 'DELETE',
    });
  }
}
