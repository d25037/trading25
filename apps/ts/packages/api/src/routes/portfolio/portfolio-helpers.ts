import {
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  type Portfolio,
  type PortfolioItem,
  PortfolioItemNotFoundError,
  PortfolioNameNotFoundError,
  PortfolioNotFoundError,
  type PortfolioSummary,
  StockNotFoundInPortfolioError,
  ValidationError,
} from '@trading25/shared/portfolio';
import type { Context } from 'hono';
import { type ErrorResponseResult, type ErrorStatusCode, handleDomainError, type KnownErrorConfig } from '../../utils';

/**
 * Classify a portfolio error into an HTTP error type and status code.
 * Returns null for unknown errors (which fall through to 500).
 */
function classifyPortfolioError(error: unknown): KnownErrorConfig | null {
  if (
    error instanceof PortfolioNotFoundError ||
    error instanceof PortfolioItemNotFoundError ||
    error instanceof PortfolioNameNotFoundError ||
    error instanceof StockNotFoundInPortfolioError
  ) {
    return { type: 'Not Found', status: 404 };
  }

  if (error instanceof InvalidStockCodeError || error instanceof ValidationError) {
    return { type: 'Bad Request', status: 400 };
  }

  if (error instanceof DuplicatePortfolioNameError || error instanceof DuplicateStockError) {
    return { type: 'Conflict', status: 409 };
  }

  return null;
}

/**
 * Handle portfolio route errors with standardized response
 */
export function handlePortfolioError<Code extends ErrorStatusCode = ErrorStatusCode>(
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
    classifyPortfolioError,
    logContext,
    allowedStatusCodes
  );
}

/**
 * Serialize portfolio dates to ISO strings for API response
 */
export function serializePortfolio(portfolio: Portfolio): {
  id: number;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
} {
  return {
    id: portfolio.id,
    name: portfolio.name,
    description: portfolio.description ?? undefined,
    createdAt: portfolio.createdAt.toISOString(),
    updatedAt: portfolio.updatedAt.toISOString(),
  };
}

/**
 * Serialize portfolio summary for API response (includes stockCount and totalShares)
 */
export function serializePortfolioSummary(summary: PortfolioSummary): {
  id: number;
  name: string;
  description?: string;
  stockCount: number;
  totalShares: number;
  createdAt: string;
  updatedAt: string;
} {
  return {
    id: summary.id,
    name: summary.name,
    description: summary.description ?? undefined,
    stockCount: summary.stockCount,
    totalShares: summary.totalShares,
    createdAt: summary.createdAt.toISOString(),
    updatedAt: summary.updatedAt.toISOString(),
  };
}

/**
 * Serialize portfolio item dates for API response
 */
export function serializeItem(item: PortfolioItem): {
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
} {
  return {
    id: item.id,
    portfolioId: item.portfolioId,
    code: item.code,
    companyName: item.companyName,
    quantity: item.quantity,
    purchasePrice: item.purchasePrice,
    purchaseDate: item.purchaseDate.toISOString().split('T')[0] ?? '',
    account: item.account ?? undefined,
    notes: item.notes ?? undefined,
    createdAt: item.createdAt.toISOString(),
    updatedAt: item.updatedAt.toISOString(),
  };
}
