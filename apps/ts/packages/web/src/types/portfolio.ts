/**
 * Portfolio types for web frontend
 * Re-exports from @trading25/shared with backward-compatible aliases
 */

export type {
  DeleteResponse,
  // Backward-compatible aliases used by web components
  DeleteResponse as DeleteSuccessResponse,
  ListPortfoliosResponse,
  PortfolioItemResponse,
  PortfolioItemResponse as PortfolioItem,
  PortfolioResponse,
  PortfolioSummaryResponse,
  PortfolioSummaryResponse as PortfolioSummary,
  PortfolioWithItemsResponse,
  PortfolioWithItemsResponse as PortfolioWithItems,
} from '@trading25/portfolio-db-ts/portfolio';

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
  companyName?: string;
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
