import {
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  type Watchlist,
  WatchlistError,
  type WatchlistItem,
  WatchlistItemNotFoundError,
  WatchlistNameNotFoundError,
  WatchlistNotFoundError,
  type WatchlistSummary,
} from '@trading25/shared/watchlist';
import type { Context } from 'hono';
import { type ErrorResponseResult, type ErrorStatusCode, handleDomainError, type KnownErrorConfig } from '../../utils';

/**
 * Classify a watchlist error into an HTTP error type and status code.
 * Returns null for unknown errors (which fall through to 500).
 */
function classifyWatchlistError(error: unknown): KnownErrorConfig | null {
  if (
    error instanceof WatchlistNotFoundError ||
    error instanceof WatchlistItemNotFoundError ||
    error instanceof WatchlistNameNotFoundError ||
    error instanceof StockNotFoundInWatchlistError
  ) {
    return { type: 'Not Found', status: 404 };
  }

  if (error instanceof WatchlistError && error.code === 'INVALID_STOCK_CODE') {
    return { type: 'Bad Request', status: 400 };
  }

  if (error instanceof DuplicateWatchlistNameError || error instanceof DuplicateWatchlistStockError) {
    return { type: 'Conflict', status: 409 };
  }

  return null;
}

export function handleWatchlistError<Code extends ErrorStatusCode = ErrorStatusCode>(
  c: Context,
  error: unknown,
  correlationId: string,
  operationName: string,
  logContext?: Record<string, unknown>,
  allowedStatusCodes?: readonly Code[]
): ErrorResponseResult<Code> {
  return handleDomainError(
    c,
    error,
    correlationId,
    operationName,
    classifyWatchlistError,
    logContext,
    allowedStatusCodes
  );
}

export function serializeWatchlist(watchlist: Watchlist): {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
} {
  return {
    id: watchlist.id,
    name: watchlist.name,
    description: watchlist.description ?? undefined,
    createdAt: watchlist.createdAt.toISOString(),
    updatedAt: watchlist.updatedAt.toISOString(),
  };
}

export function serializeWatchlistSummary(summary: WatchlistSummary): {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  createdAt: string;
  updatedAt: string;
} {
  return {
    id: summary.id,
    name: summary.name,
    description: summary.description ?? undefined,
    stockCount: summary.stockCount,
    createdAt: summary.createdAt.toISOString(),
    updatedAt: summary.updatedAt.toISOString(),
  };
}

export function serializeWatchlistItem(item: WatchlistItem): {
  id: number;
  watchlistId: number;
  code: string;
  companyName: string;
  memo?: string;
  createdAt: string;
} {
  return {
    id: item.id,
    watchlistId: item.watchlistId,
    code: item.code,
    companyName: item.companyName,
    memo: item.memo ?? undefined,
    createdAt: item.createdAt.toISOString(),
  };
}
