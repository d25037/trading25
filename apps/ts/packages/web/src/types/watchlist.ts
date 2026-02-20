/**
 * Watchlist types for web frontend
 * Re-exports from @trading25/shared with aliases
 */

export type {
  ListWatchlistsResponse,
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  // Aliases for convenience
  WatchlistItemResponse as WatchlistItem,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummaryResponse,
  WatchlistSummaryResponse as WatchlistSummary,
  WatchlistWithItemsResponse,
  WatchlistWithItemsResponse as WatchlistWithItems,
} from '@trading25/shared/watchlist';

// Frontend-specific request types
export interface CreateWatchlistRequest {
  name: string;
  description?: string;
}

export interface UpdateWatchlistRequest {
  name?: string;
  description?: string;
}

export interface CreateWatchlistItemRequest {
  code: string;
  companyName: string;
  memo?: string;
}
