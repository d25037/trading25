/**
 * Watchlist Management Module
 * Shared types and errors for watchlist API contracts.
 */

export type {
  CreateWatchlistInput,
  CreateWatchlistItemInput,
  ListWatchlistsResponse,
  UpdateWatchlistInput,
  Watchlist,
  WatchlistDeleteResponse,
  WatchlistItem,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummary,
  WatchlistSummaryResponse,
  WatchlistWithItems,
  WatchlistWithItemsResponse,
} from './types';
export {
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  WatchlistError,
  WatchlistItemNotFoundError,
  WatchlistNameNotFoundError,
  WatchlistNotFoundError,
} from './types';
