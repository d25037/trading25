/**
 * Unified Error Hierarchy for Trading25
 *
 * Base error classes that provide:
 * - Consistent error codes across the application
 * - HTTP status code mapping for API responses
 * - Proper error classification for handling
 */

/**
 * Abstract base class for all Trading25 errors.
 * All domain-specific errors should extend this class.
 */
export abstract class Trading25Error extends Error {
  /** Error code for programmatic error handling */
  abstract readonly code: string;
  /** HTTP status code for API responses */
  abstract readonly httpStatus: number;

  constructor(
    message: string,
    public override readonly cause?: Error
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

/**
 * 400 Bad Request - Invalid input or request format
 */
export class BadRequestError extends Trading25Error {
  readonly code: string = 'BAD_REQUEST';
  readonly httpStatus = 400 as const;
}

/**
 * 404 Not Found - Requested resource does not exist
 */
export class NotFoundError extends Trading25Error {
  readonly code: string = 'NOT_FOUND';
  readonly httpStatus = 404 as const;
}

/**
 * 409 Conflict - Resource already exists or state conflict
 */
export class ConflictError extends Trading25Error {
  readonly code: string = 'CONFLICT';
  readonly httpStatus = 409 as const;
}

/**
 * 500 Internal Server Error - Unexpected server-side error
 */
export class InternalError extends Trading25Error {
  readonly code: string = 'INTERNAL_ERROR';
  readonly httpStatus = 500 as const;
}

/**
 * Type guard to check if an error is a Trading25Error
 */
export function isTrading25Error(error: unknown): error is Trading25Error {
  return error instanceof Trading25Error;
}

/**
 * Get a safe error message from an unknown error
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
