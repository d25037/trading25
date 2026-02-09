/**
 * Watchlist Types and Error Classes
 * Core types for lightweight stock watchlist management
 */

import { BadRequestError } from '../errors';

/**
 * Watchlist - A named collection of watched stocks
 */
export interface Watchlist {
  id: number;
  name: string;
  description?: string;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Watchlist Item - Individual stock entry within a watchlist
 */
export interface WatchlistItem {
  id: number;
  watchlistId: number;
  code: string;
  companyName: string;
  memo?: string;
  createdAt: Date;
}

/**
 * Watchlist with all its items
 */
export interface WatchlistWithItems extends Watchlist {
  items: WatchlistItem[];
}

/**
 * Watchlist summary statistics
 */
export interface WatchlistSummary {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Input for creating a new watchlist
 */
export interface CreateWatchlistInput {
  name: string;
  description?: string;
}

/**
 * Input for creating a new watchlist item
 */
export interface CreateWatchlistItemInput {
  watchlistId: number;
  code: string;
  companyName: string;
  memo?: string;
}

/**
 * Input for updating an existing watchlist
 */
export interface UpdateWatchlistInput {
  name?: string;
  description?: string;
}

/**
 * Base error class for watchlist operations
 */
export class WatchlistError extends BadRequestError {
  override readonly code: string = 'WATCHLIST_ERROR';

  constructor(message: string, code?: string) {
    super(message);
    if (code) {
      this.code = code;
    }
    this.name = 'WatchlistError';
  }
}

/**
 * Error thrown when a watchlist is not found
 */
export class WatchlistNotFoundError extends WatchlistError {
  constructor(id: number) {
    super(`Watchlist with ID ${id} not found`, 'WATCHLIST_NOT_FOUND');
  }
}

/**
 * Error thrown when a watchlist name is not found
 */
export class WatchlistNameNotFoundError extends WatchlistError {
  constructor(name: string) {
    super(`Watchlist with name "${name}" not found`, 'WATCHLIST_NAME_NOT_FOUND');
  }
}

/**
 * Error thrown when a watchlist item is not found
 */
export class WatchlistItemNotFoundError extends WatchlistError {
  constructor(id: number) {
    super(`Watchlist item with ID ${id} not found`, 'ITEM_NOT_FOUND');
  }
}

/**
 * Error thrown when a stock code is not found in a specific watchlist
 */
export class StockNotFoundInWatchlistError extends WatchlistError {
  constructor(code: string, watchlistId: number) {
    super(`Stock ${code} not found in watchlist ${watchlistId}`, 'STOCK_NOT_FOUND_IN_WATCHLIST');
  }
}

/**
 * Error thrown when trying to add a duplicate stock to a watchlist
 */
export class DuplicateWatchlistStockError extends WatchlistError {
  constructor(code: string, watchlistId: number) {
    super(`Stock ${code} already exists in watchlist ${watchlistId}`, 'DUPLICATE_STOCK');
  }
}

/**
 * Error thrown when a duplicate watchlist name is used
 */
export class DuplicateWatchlistNameError extends WatchlistError {
  constructor(name: string) {
    super(`Watchlist with name "${name}" already exists`, 'DUPLICATE_NAME');
  }
}

// ============================================================
// API Response Types (Date fields serialized as ISO 8601 strings)
// ============================================================

/**
 * Watchlist API response - dates as strings
 */
export interface WatchlistResponse {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Watchlist Item API response - dates as strings
 */
export interface WatchlistItemResponse {
  id: number;
  watchlistId: number;
  code: string;
  companyName: string;
  memo?: string;
  createdAt: string;
}

/**
 * Watchlist with items API response
 */
export interface WatchlistWithItemsResponse extends WatchlistResponse {
  items: WatchlistItemResponse[];
}

/**
 * Watchlist summary API response
 */
export interface WatchlistSummaryResponse {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  createdAt: string;
  updatedAt: string;
}

/**
 * List watchlists API response
 */
export interface ListWatchlistsResponse {
  watchlists: WatchlistSummaryResponse[];
}

/**
 * Stock price info for watchlist display
 */
export interface WatchlistStockPrice {
  code: string;
  close: number;
  prevClose: number | null;
  changePercent: number | null;
  volume: number;
  date: string;
}

/**
 * Watchlist prices API response
 */
export interface WatchlistPricesResponse {
  prices: WatchlistStockPrice[];
}

/**
 * Delete operation API response
 */
export interface WatchlistDeleteResponse {
  success: boolean;
  message: string;
}
