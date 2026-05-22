export type {
  ListWatchlistsResponse,
  WatchlistDeleteResponse,
  WatchlistItemResponse,
  WatchlistPricesResponse,
  WatchlistResponse,
  WatchlistStockPrice,
  WatchlistSummaryResponse,
  WatchlistWithItemsResponse,
} from '@trading25/contracts/types/api-response-types';

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
