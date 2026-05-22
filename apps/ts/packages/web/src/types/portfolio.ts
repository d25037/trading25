/**
 * Portfolio types for web frontend
 * Re-exports from @trading25/contracts.
 */

export type {
  DeleteResponse,
  ListPortfoliosResponse,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioSummaryResponse,
  PortfolioWithItemsResponse,
} from '@trading25/contracts/types/api-response-types';

// Frontend-specific request types
export interface CreatePortfolioRequest {
  name: string;
  description?: string;
}

export interface UpdatePortfolioRequest {
  name?: string;
  description?: string;
}

export interface CreatePortfolioItemRequest {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  notes?: string;
}

export interface UpdatePortfolioItemRequest {
  quantity?: number;
  purchasePrice?: number;
  purchaseDate?: string;
  account?: string;
  notes?: string;
}
