import { logger } from '@trading25/shared/utils/logger';
import type { Context } from 'hono';
import { createErrorResponse, type ErrorType } from './error-responses';

/**
 * Database error detection result
 */
export interface DatabaseErrorResult {
  isDatabaseError: boolean;
  errorType: 'not_initialized' | 'no_data' | 'table_missing' | 'sqlite_error' | null;
}

/**
 * Error patterns that indicate database-related issues
 */
const DATABASE_ERROR_PATTERNS = {
  not_initialized: ['Database not initialized', 'database not ready'],
  no_data: ['No trading data', 'No data available', 'Insufficient data'],
  table_missing: ['no such table', 'table not found'],
  sqlite_error: ['SQLITE_ERROR', 'SQLITE_CONSTRAINT', 'SQLITE_BUSY'],
} as const;

/**
 * Detect if an error message indicates a database-related error
 *
 * Checks the error message against known database error patterns to determine
 * if the error is due to database issues (uninitialized, missing data, etc.)
 *
 * @param errorMessage - The error message to check
 * @returns Detection result with error type if it's a database error
 *
 * @example
 * ```typescript
 * const result = detectDatabaseError('No trading data for the specified date');
 * if (result.isDatabaseError) {
 *   // Handle as 422 Unprocessable Entity
 * }
 * ```
 */
export function detectDatabaseError(errorMessage: string): DatabaseErrorResult {
  const lowerMessage = errorMessage.toLowerCase();

  for (const [errorType, patterns] of Object.entries(DATABASE_ERROR_PATTERNS)) {
    for (const pattern of patterns) {
      if (lowerMessage.includes(pattern.toLowerCase())) {
        return {
          isDatabaseError: true,
          errorType: errorType as DatabaseErrorResult['errorType'],
        };
      }
    }
  }

  return { isDatabaseError: false, errorType: null };
}

/**
 * Configuration for database error handling
 */
export interface DatabaseErrorConfig {
  /** Log context for the error (e.g., { date: '2024-01-01', markets: ['prime'] }) */
  logContext?: Record<string, unknown>;
  /** Custom message for database not ready errors */
  notReadyMessage?: string;
}

/**
 * Default message when database is not ready
 */
const DEFAULT_NOT_READY_MESSAGE = 'Market database not initialized. Please run "bun cli db sync" first.';

/**
 * Handle database-related errors with standardized response
 *
 * If the error is database-related, logs appropriately and returns a 422 response.
 * Returns null if the error is not database-related.
 *
 * @param c - Hono context
 * @param error - The error that occurred
 * @param correlationId - Request correlation ID
 * @param config - Optional configuration
 * @returns JSON response for database errors, or null if not a database error
 *
 * @example
 * ```typescript
 * try {
 *   const data = await service.getData(params);
 *   return c.json(data, 200);
 * } catch (error) {
 *   const errorMessage = error instanceof Error ? error.message : 'Unknown error';
 *
 *   // Check for database errors (returns 422)
 *   const dbResponse = handleDatabaseError(c, errorMessage, correlationId, {
 *     logContext: { date: params.date },
 *   });
 *   if (dbResponse) return dbResponse;
 *
 *   // Fall through to generic error handling...
 * }
 * ```
 */
export function handleDatabaseError(
  c: Context,
  errorMessage: string,
  correlationId: string,
  config?: DatabaseErrorConfig
): Response | null {
  const detection = detectDatabaseError(errorMessage);

  if (!detection.isDatabaseError) {
    return null;
  }

  logger.warn('Database not ready', {
    correlationId,
    errorType: detection.errorType,
    error: errorMessage,
    ...config?.logContext,
  });

  return c.json(
    createErrorResponse({
      error: 'Unprocessable Entity',
      message: config?.notReadyMessage ?? DEFAULT_NOT_READY_MESSAGE,
      correlationId,
    }),
    422
  );
}

/**
 * Error mapping configuration for route handlers
 */
export interface ErrorMapping {
  /** Pattern to match in error message (case-insensitive) */
  pattern: string;
  /** HTTP error type to return */
  errorType: ErrorType;
  /** HTTP status code */
  statusCode: number;
  /** Optional custom message (uses error message if not provided) */
  message?: string;
}

/**
 * Common error mappings used across routes
 */
export const COMMON_ERROR_MAPPINGS: ErrorMapping[] = [
  { pattern: 'not found', errorType: 'Not Found', statusCode: 404 },
  { pattern: 'Insufficient data', errorType: 'Unprocessable Entity', statusCode: 422 },
  { pattern: 'already exists', errorType: 'Conflict', statusCode: 409 },
  { pattern: 'already running', errorType: 'Conflict', statusCode: 409 },
];

/**
 * Find matching error mapping for an error message
 *
 * @param errorMessage - The error message to check
 * @param mappings - Array of error mappings to check against
 * @returns The matching error mapping, or undefined if no match
 */
export function findErrorMapping(errorMessage: string, mappings: ErrorMapping[]): ErrorMapping | undefined {
  const lowerMessage = errorMessage.toLowerCase();
  return mappings.find((mapping) => lowerMessage.includes(mapping.pattern.toLowerCase()));
}
