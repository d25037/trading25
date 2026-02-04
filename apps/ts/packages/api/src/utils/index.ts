/**
 * API Utilities
 *
 * This module exports shared utilities for API route handlers including:
 * - Error response creation
 * - Validation hooks for OpenAPI
 * - Database error handling
 * - Route error handling
 * - Service lifecycle management
 */

// Database error handling
export {
  COMMON_ERROR_MAPPINGS,
  type DatabaseErrorConfig,
  type DatabaseErrorResult,
  detectDatabaseError,
  type ErrorMapping,
  findErrorMapping,
  handleDatabaseError,
} from './database-error-handler';
// Error response utilities
export {
  createErrorResponse,
  type ErrorResponse,
  type ErrorResponseParams,
  type ErrorResponseResult,
  type ErrorStatusCode,
  type ErrorType,
  resolveAllowedStatus,
} from './error-responses';
// Route error handling
export {
  handleDomainError,
  handleRouteError,
  type KnownErrorConfig,
  type RouteErrorConfig,
  type RouteHandler,
  type WithErrorHandlingConfig,
  withErrorHandling,
} from './route-handler';
// Safe parsing utilities
export { safeDecodeURIComponent, safeParseInt } from './safe-parsers';
// Service lifecycle management
export {
  type CleanupableService,
  clearServiceRegistry,
  createManagedService,
  getRegisteredServiceCount,
  type ManagedServiceConfig,
  registerServiceForCleanup,
  unregisterService,
} from './service-lifecycle';
// Validation hook and OpenAPI app factory
export { createOpenAPIApp, validationHook } from './validation-hook';
