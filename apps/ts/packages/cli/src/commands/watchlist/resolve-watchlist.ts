import type { WatchlistWithItemsResponse } from '@trading25/portfolio-db-ts/watchlist';
import type { ApiClient } from '../../utils/api-client.js';
import { CLINotFoundError } from '../../utils/error-handling.js';

/**
 * Resolve a watchlist by name or ID string.
 * Throws CLINotFoundError if the watchlist is not found by name.
 */
export async function resolveWatchlist(apiClient: ApiClient, nameOrId: string): Promise<WatchlistWithItemsResponse> {
  const watchlistId = Number.parseInt(nameOrId, 10);

  if (Number.isNaN(watchlistId)) {
    const response = await apiClient.listWatchlists();
    const found = response.watchlists.find((w) => w.name === nameOrId);
    if (!found) {
      throw new CLINotFoundError(`Watchlist not found: ${nameOrId}. List all watchlists with: watchlist list`);
    }
    return apiClient.getWatchlist(found.id);
  }
  return apiClient.getWatchlist(watchlistId);
}

/**
 * Resolve a watchlist ID by name or ID string.
 * Throws CLINotFoundError if the watchlist is not found by name.
 */
export async function resolveWatchlistId(apiClient: ApiClient, nameOrId: string): Promise<number> {
  const watchlistId = Number.parseInt(nameOrId, 10);

  if (!Number.isNaN(watchlistId)) {
    return watchlistId;
  }

  const response = await apiClient.listWatchlists();
  const found = response.watchlists.find((w) => w.name === nameOrId);
  if (!found) {
    throw new CLINotFoundError(`Watchlist not found: ${nameOrId}. List all watchlists with: watchlist list`);
  }
  return found.id;
}
