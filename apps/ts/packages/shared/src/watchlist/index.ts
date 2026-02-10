/**
 * Watchlist Management Module
 * Lightweight stock monitoring lists with SQLite storage
 *
 * Uses Drizzle ORM for type-safe database operations
 */

// Re-export Drizzle implementation as WatchlistDatabase
export { DrizzleWatchlistDatabase as WatchlistDatabase } from '../db/drizzle-watchlist-database';

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
