/**
 * Portfolio Types and Error Classes
 * Core types for portfolio management system
 */

import { BadRequestError } from '../errors';

/**
 * Portfolio - A named collection of stock holdings
 */
export interface Portfolio {
  id: number;
  name: string;
  description?: string;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Portfolio Item - Individual stock holding within a portfolio
 */
export interface PortfolioItem {
  id: number;
  portfolioId: number;
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: Date;
  account?: string;
  notes?: string;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Portfolio with all its items
 */
export interface PortfolioWithItems extends Portfolio {
  items: PortfolioItem[];
}

/**
 * Portfolio summary statistics
 */
export interface PortfolioSummary {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  totalShares: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Input for creating a new portfolio
 */
export interface CreatePortfolioInput {
  name: string;
  description?: string;
}

/**
 * Input for creating a new portfolio item
 */
export interface CreatePortfolioItemInput {
  portfolioId: number;
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: Date;
  account?: string;
  notes?: string;
}

/**
 * Input for updating an existing portfolio
 */
export interface UpdatePortfolioInput {
  name?: string;
  description?: string;
}

/**
 * Input for updating an existing portfolio item
 */
export interface UpdatePortfolioItemInput {
  quantity?: number;
  purchasePrice?: number;
  purchaseDate?: Date;
  account?: string;
  notes?: string;
}

/**
 * Base error class for portfolio operations
 */
export class PortfolioError extends BadRequestError {
  override readonly code: string = 'PORTFOLIO_ERROR';

  constructor(message: string, code?: string) {
    super(message);
    if (code) {
      this.code = code;
    }
    this.name = 'PortfolioError';
  }
}

/**
 * Error thrown when a portfolio is not found
 */
export class PortfolioNotFoundError extends PortfolioError {
  constructor(id: number) {
    super(`Portfolio with ID ${id} not found`, 'PORTFOLIO_NOT_FOUND');
  }
}

/**
 * Error thrown when a portfolio name is not found
 */
export class PortfolioNameNotFoundError extends PortfolioError {
  constructor(name: string) {
    super(`Portfolio with name "${name}" not found`, 'PORTFOLIO_NAME_NOT_FOUND');
  }
}

/**
 * Error thrown when a portfolio item is not found
 */
export class PortfolioItemNotFoundError extends PortfolioError {
  constructor(id: number) {
    super(`Portfolio item with ID ${id} not found`, 'ITEM_NOT_FOUND');
  }
}

/**
 * Error thrown when a stock code is not found in a specific portfolio
 */
export class StockNotFoundInPortfolioError extends PortfolioError {
  constructor(code: string, portfolioId: number) {
    super(`Stock ${code} not found in portfolio ${portfolioId}`, 'STOCK_NOT_FOUND_IN_PORTFOLIO');
  }
}

/**
 * Error thrown when trying to add a duplicate stock to a portfolio
 */
export class DuplicateStockError extends PortfolioError {
  constructor(code: string, portfolioId: number) {
    super(`Stock ${code} already exists in portfolio ${portfolioId}`, 'DUPLICATE_STOCK');
  }
}

/**
 * Error thrown when a stock code is invalid
 */
export class InvalidStockCodeError extends PortfolioError {
  constructor(code: string) {
    super(`Invalid stock code: ${code}. Must be 4 characters (e.g., 7203 or 285A).`, 'INVALID_STOCK_CODE');
  }
}

/**
 * Error thrown when a duplicate portfolio name is used
 */
export class DuplicatePortfolioNameError extends PortfolioError {
  constructor(name: string) {
    super(`Portfolio with name "${name}" already exists`, 'DUPLICATE_NAME');
  }
}

/**
 * Error thrown when validation fails
 */
export class ValidationError extends PortfolioError {
  constructor(message: string) {
    super(message, 'VALIDATION_ERROR');
  }
}

// ============================================================
// API Response Types (Date fields serialized as ISO 8601 strings)
// ============================================================

/**
 * Portfolio API response - dates as strings
 */
export interface PortfolioResponse {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Portfolio Item API response - dates as strings
 */
export interface PortfolioItemResponse {
  id: number;
  portfolioId: number;
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  purchaseDate: string;
  account?: string;
  notes?: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Portfolio with items API response
 */
export interface PortfolioWithItemsResponse extends PortfolioResponse {
  items: PortfolioItemResponse[];
}

/**
 * Portfolio summary API response
 */
export interface PortfolioSummaryResponse {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  totalShares: number;
  createdAt: string;
  updatedAt: string;
}

/**
 * List portfolios API response
 */
export interface ListPortfoliosResponse {
  portfolios: PortfolioSummaryResponse[];
}

/**
 * Delete operation API response
 */
export interface DeleteResponse {
  success: boolean;
  message: string;
}
