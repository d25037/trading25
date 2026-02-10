/**
 * Portfolio Management Module
 * XDG-compliant portfolio tracking with SQLite storage
 *
 * Uses Drizzle ORM for type-safe database operations
 */

// Re-export Drizzle implementation as PortfolioDatabase for backward compatibility
export { DrizzlePortfolioDatabase as PortfolioDatabase } from '../db/drizzle-portfolio-database';

export type {
  CreatePortfolioInput,
  CreatePortfolioItemInput,
  DeleteResponse,
  ListPortfoliosResponse,
  Portfolio,
  PortfolioItem,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioSummary,
  PortfolioSummaryResponse,
  PortfolioWithItems,
  PortfolioWithItemsResponse,
  UpdatePortfolioInput,
  UpdatePortfolioItemInput,
} from './types';
export {
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  PortfolioError,
  PortfolioItemNotFoundError,
  PortfolioNameNotFoundError,
  PortfolioNotFoundError,
  StockNotFoundInPortfolioError,
  ValidationError,
} from './types';
