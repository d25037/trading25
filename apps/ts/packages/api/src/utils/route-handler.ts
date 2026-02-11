import { getErrorMessage, getErrorStack } from '@trading25/shared/utils/error-helpers';
import { logger } from '@trading25/shared/utils/logger';
import type { Context } from 'hono';
import {
  COMMON_ERROR_MAPPINGS,
  type DatabaseErrorConfig,
  type ErrorMapping,
  findErrorMapping,
  handleDatabaseError,
} from './database-error-handler';
import {
  createErrorResponse,
  type ErrorResponseResult,
  type ErrorStatusCode,
  type ErrorType,
  isErrorStatusCode,
  resolveAllowedStatus,
} from './error-responses';

/**
 * Known error classification result from domain-specific error detection
 */
export interface KnownErrorConfig {
  type: ErrorType;
  status: ErrorStatusCode;
}

/**
 * Handle domain-specific route errors with standardized response
 *
 * Shared logic for domain error handlers (portfolio, watchlist, etc.) that:
 * 1. Checks for known domain errors via the provided classifier
 * 2. Falls through to generic 500 Internal Server Error for unknown errors
 *
 * @param c - Hono context
 * @param error - The error that occurred
 * @param correlationId - Request correlation ID
 * @param operationName - Operation name for logging
 * @param classifyError - Domain-specific error classifier function
 * @param logContext - Optional log context
 * @param allowedStatusCodes - Optional allowed status codes for type narrowing
 * @returns JSON error response
 */
export function handleDomainError<Code extends ErrorStatusCode = ErrorStatusCode>(
  c: Context,
  error: unknown,
  correlationId: string,
  operationName: string,
  classifyError: (error: unknown) => KnownErrorConfig | null,
  logContext?: Record<string, unknown>,
  allowedStatusCodes?: readonly Code[]
): ErrorResponseResult<Code> {
  const errorMessage = getErrorMessage(error);
  const errorStack = getErrorStack(error);
  const errorConfig = classifyError(error);

  if (errorConfig) {
    const statusCode = resolveAllowedStatus(errorConfig.status, allowedStatusCodes);
    return c.json(
      createErrorResponse({
        error: errorConfig.type,
        message: errorMessage,
        correlationId,
      }),
      statusCode
    ) as ErrorResponseResult<Code>;
  }

  logger.error(`Failed to ${operationName}`, {
    correlationId,
    error: errorMessage,
    stack: errorStack,
    ...logContext,
  });

  const statusCode = resolveAllowedStatus(500, allowedStatusCodes);
  return c.json(
    createErrorResponse({
      error: 'Internal Server Error',
      message: errorMessage,
      correlationId,
    }),
    statusCode
  ) as ErrorResponseResult<Code>;
}

/**
 * Configuration for route error handling
 */
export interface RouteErrorConfig {
  /** Operation name for logging (e.g., 'get market rankings') */
  operationName: string;
  /** Log context to include in error logs */
  logContext?: Record<string, unknown>;
  /** Whether to check for database errors (default: false) */
  checkDatabaseErrors?: boolean;
  /** Custom message for database not ready errors */
  databaseNotReadyMessage?: string;
  /** Additional error mappings to check before falling through to 500 */
  errorMappings?: ErrorMapping[];
  /** Default error type for unhandled errors (default: 'Internal Server Error') */
  defaultErrorType?: ErrorType;
}

interface ErrorWithStatusCode {
  statusCode?: unknown;
}

function statusToErrorType(statusCode: ErrorStatusCode): ErrorType {
  switch (statusCode) {
    case 400:
      return 'Bad Request';
    case 404:
      return 'Not Found';
    case 409:
      return 'Conflict';
    case 422:
      return 'Unprocessable Entity';
    default:
      return 'Internal Server Error';
  }
}

function getStatusCodeFromError(error: unknown): ErrorStatusCode | null {
  if (typeof error !== 'object' || error === null) return null;
  const statusCode = (error as ErrorWithStatusCode).statusCode;
  if (typeof statusCode !== 'number') return null;
  return isErrorStatusCode(statusCode) ? statusCode : null;
}

function logByStatusCode(
  operationName: string,
  correlationId: string,
  statusCode: ErrorStatusCode,
  errorMessage: string,
  errorStack: string | undefined,
  logContext?: Record<string, unknown>,
  reason?: string
): void {
  if (statusCode >= 500) {
    logger.error(`Failed to ${operationName}`, {
      correlationId,
      statusCode,
      error: errorMessage,
      stack: errorStack,
      ...logContext,
    });
    return;
  }

  logger.warn(`${operationName} failed`, {
    correlationId,
    statusCode,
    reason,
    error: errorMessage,
    ...logContext,
  });
}

function createStatusCodeResponse<Code extends ErrorStatusCode = ErrorStatusCode>(
  c: Context,
  statusCodeFromError: ErrorStatusCode,
  errorMessage: string,
  correlationId: string,
  operationName: string,
  errorStack: string | undefined,
  config: RouteErrorConfig & { allowedStatusCodes?: readonly Code[] }
): ErrorResponseResult<Code> {
  const statusCode = resolveAllowedStatus(statusCodeFromError, config.allowedStatusCodes);
  const errorType = statusToErrorType(statusCode);

  logByStatusCode(operationName, correlationId, statusCode, errorMessage, errorStack, config.logContext);

  return c.json(
    createErrorResponse({
      error: errorType,
      message: errorMessage,
      correlationId,
    }),
    statusCode
  ) as ErrorResponseResult<Code>;
}

function createMappedResponse<Code extends ErrorStatusCode = ErrorStatusCode>(
  c: Context,
  mapping: ErrorMapping,
  errorMessage: string,
  correlationId: string,
  operationName: string,
  errorStack: string | undefined,
  config: RouteErrorConfig & { allowedStatusCodes?: readonly Code[] }
): ErrorResponseResult<Code> {
  logByStatusCode(
    operationName,
    correlationId,
    mapping.statusCode as ErrorStatusCode,
    errorMessage,
    errorStack,
    config.logContext,
    mapping.errorType
  );

  const statusCode = resolveAllowedStatus(mapping.statusCode as ErrorStatusCode, config.allowedStatusCodes);
  return c.json(
    createErrorResponse({
      error: mapping.errorType,
      message: mapping.message ?? errorMessage,
      correlationId,
    }),
    statusCode
  ) as ErrorResponseResult<Code>;
}

/**
 * Handle route errors with standardized response
 *
 * Provides a centralized error handling function that:
 * 1. Optionally checks for database-related errors (returns 422)
 * 2. Checks custom error mappings for specific error types
 * 3. Falls through to generic 500 Internal Server Error
 *
 * @param c - Hono context
 * @param error - The error that occurred
 * @param correlationId - Request correlation ID
 * @param config - Error handling configuration
 * @returns JSON error response
 *
 * @example
 * ```typescript
 * try {
 *   const data = await service.getData(params);
 *   return c.json(data, 200);
 * } catch (error) {
 *   return handleRouteError(c, error, correlationId, {
 *     operationName: 'get market rankings',
 *     logContext: { date: params.date },
 *     checkDatabaseErrors: true,
 *     errorMappings: [
 *       { pattern: 'not found', errorType: 'Not Found', statusCode: 404 },
 *     ],
 *   });
 * }
 * ```
 */
export function handleRouteError<Code extends ErrorStatusCode = ErrorStatusCode>(
  c: Context,
  error: unknown,
  correlationId: string,
  config: RouteErrorConfig & { allowedStatusCodes?: readonly Code[] }
): ErrorResponseResult<Code> {
  const errorMessage = getErrorMessage(error);
  const errorStack = getErrorStack(error);

  // Validate allowedStatusCodes includes 422 when checkDatabaseErrors is enabled
  if (config.checkDatabaseErrors && config.allowedStatusCodes && !config.allowedStatusCodes.includes(422 as Code)) {
    logger.warn('allowedStatusCodes should include 422 when checkDatabaseErrors is enabled', {
      operationName: config.operationName,
      allowedStatusCodes: config.allowedStatusCodes,
    });
  }

  // Check for database errors first if enabled
  if (config.checkDatabaseErrors) {
    const dbConfig: DatabaseErrorConfig = {
      logContext: config.logContext,
      notReadyMessage: config.databaseNotReadyMessage,
    };
    const dbResponse = handleDatabaseError(c, errorMessage, correlationId, dbConfig);
    if (dbResponse) return dbResponse as ErrorResponseResult<Code>;
  }

  const statusCodeFromError = getStatusCodeFromError(error);
  if (statusCodeFromError !== null) {
    return createStatusCodeResponse(c, statusCodeFromError, errorMessage, correlationId, config.operationName, errorStack, config);
  }

  // Check custom error mappings
  const allMappings = [...(config.errorMappings ?? []), ...COMMON_ERROR_MAPPINGS];
  const mapping = findErrorMapping(errorMessage, allMappings);

  if (mapping) {
    return createMappedResponse(c, mapping, errorMessage, correlationId, config.operationName, errorStack, config);
  }

  // Default to 500 Internal Server Error
  logger.error(`Failed to ${config.operationName}`, {
    correlationId,
    error: errorMessage,
    stack: errorStack,
    ...config.logContext,
  });

  const statusCode = resolveAllowedStatus(500, config.allowedStatusCodes);
  return c.json(
    createErrorResponse({
      error: config.defaultErrorType ?? 'Internal Server Error',
      message: errorMessage,
      correlationId,
    }),
    statusCode
  ) as ErrorResponseResult<Code>;
}

/**
 * Handler function type for route handlers
 */
export type RouteHandler<T> = (c: Context) => Promise<T>;

/**
 * Configuration for the withErrorHandling wrapper
 */
export interface WithErrorHandlingConfig extends Omit<RouteErrorConfig, 'logContext'> {
  /** Function to extract log context from the Hono context */
  getLogContext?: (c: Context) => Record<string, unknown>;
}

/**
 * Wrap a route handler with standardized error handling
 *
 * Creates a higher-order function that wraps the handler with try-catch
 * and standardized error response handling.
 *
 * @param handler - The route handler function to wrap
 * @param config - Error handling configuration
 * @returns Wrapped handler with error handling
 *
 * @example
 * ```typescript
 * const getDataHandler = withErrorHandling(
 *   async (c) => {
 *     const params = c.req.valid('query');
 *     const data = await service.getData(params);
 *     return c.json(data, 200);
 *   },
 *   {
 *     operationName: 'get data',
 *     checkDatabaseErrors: true,
 *     getLogContext: (c) => ({ query: c.req.valid('query') }),
 *   }
 * );
 *
 * app.openapi(route, getDataHandler);
 * ```
 */
export function withErrorHandling<T>(
  handler: RouteHandler<T>,
  config: WithErrorHandlingConfig
): RouteHandler<T | ErrorResponseResult> {
  return async (c: Context) => {
    const correlationId = c.get('correlationId') || c.req.header('x-correlation-id') || crypto.randomUUID();

    try {
      return await handler(c);
    } catch (error) {
      const logContext = config.getLogContext?.(c) ?? {};
      return handleRouteError(c, error, correlationId, {
        ...config,
        logContext,
      });
    }
  };
}
