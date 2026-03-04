/**
 * Error handling utilities for API requests and retries
 * These helpers reduce complexity in error handling logic
 */

/**
 * Extract error message from unknown error type
 *
 * Handles both Error instances and unknown values safely.
 * This utility eliminates the repeated `error instanceof Error ? error.message : String(error)` pattern.
 *
 * @param error - The error to extract message from
 * @returns The error message string
 *
 * @example
 * ```typescript
 * try {
 *   await someOperation();
 * } catch (error) {
 *   logger.error('Operation failed', { error: getErrorMessage(error) });
 * }
 * ```
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

/**
 * Extract stack trace from unknown error type
 *
 * Returns the stack trace if the error is an Error instance, undefined otherwise.
 * This utility eliminates the repeated `error instanceof Error ? error.stack : undefined` pattern.
 *
 * @param error - The error to extract stack from
 * @returns The stack trace string or undefined
 */
export function getErrorStack(error: unknown): string | undefined {
  if (error instanceof Error) {
    return error.stack;
  }
  return undefined;
}

export interface RetryableErrorResult {
  isRetryable: boolean;
  retryAfterMs?: number;
}

/**
 * Check if an error is a network connectivity issue
 */
export function isNetworkError(error: Error): boolean {
  const message = error.message.toLowerCase();
  return (
    message.includes('network') ||
    message.includes('connection') ||
    message.includes('timeout') ||
    message.includes('enotfound') ||
    message.includes('econnrefused') ||
    message.includes('econnreset')
  );
}

/**
 * Check if an error is a rate limiting error
 */
export function isRateLimitError(error: Error): RetryableErrorResult {
  const message = error.message.toLowerCase();

  if (message.includes('rate limit') || message.includes('too many requests')) {
    // Try to extract retry-after time from common formats
    const retryAfterMatch = message.match(/retry[_\s-]?after[:\s]*(\d+)/i);
    if (retryAfterMatch?.[1]) {
      return {
        isRetryable: true,
        retryAfterMs: Number.parseInt(retryAfterMatch[1], 10) * 1000,
      };
    }

    // Default rate limit retry delay
    return { isRetryable: true, retryAfterMs: 5000 };
  }

  return { isRetryable: false };
}

/**
 * Check if an error is a temporary server error
 */
export function isTemporaryServerError(error: Error): boolean {
  const message = error.message.toLowerCase();
  return (
    message.includes('500') ||
    message.includes('502') ||
    message.includes('503') ||
    message.includes('504') ||
    message.includes('internal server error') ||
    message.includes('bad gateway') ||
    message.includes('service unavailable') ||
    message.includes('gateway timeout')
  );
}

/**
 * Check if an error is a timeout error
 */
export function isTimeoutError(error: Error): boolean {
  const message = error.message.toLowerCase();
  return message.includes('timeout') || message.includes('etimedout') || message.includes('request timeout');
}

/**
 * Check if an HTTP status code indicates a retryable error
 */
export function isRetryableHttpStatus(status: number): boolean {
  return (
    status === 408 || // Request Timeout
    status === 429 || // Too Many Requests
    status === 500 || // Internal Server Error
    status === 502 || // Bad Gateway
    status === 503 || // Service Unavailable
    status === 504 // Gateway Timeout
  );
}

/**
 * Main function to determine if an error is retryable
 * This consolidates all the error checking logic
 */
export function determineRetryability(error: unknown): RetryableErrorResult {
  if (!(error instanceof Error)) {
    return { isRetryable: false };
  }

  // Check for rate limiting first (has specific retry delay)
  const rateLimitResult = isRateLimitError(error);
  if (rateLimitResult.isRetryable) {
    return rateLimitResult;
  }

  // Check for network errors
  if (isNetworkError(error)) {
    return { isRetryable: true, retryAfterMs: 1000 };
  }

  // Check for timeout errors
  if (isTimeoutError(error)) {
    return { isRetryable: true, retryAfterMs: 2000 };
  }

  // Check for temporary server errors
  if (isTemporaryServerError(error)) {
    return { isRetryable: true, retryAfterMs: 3000 };
  }

  // Try to extract HTTP status from JQuantsApiError
  const apiError = error as { status?: number; response?: { status?: number } };
  const status = apiError.status ?? apiError.response?.status;
  if (status && isRetryableHttpStatus(status)) {
    const delay = status === 429 ? 5000 : 2000;
    return { isRetryable: true, retryAfterMs: delay };
  }

  return { isRetryable: false };
}
