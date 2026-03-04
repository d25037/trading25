/**
 * Timeout utilities for handling operation timeouts gracefully
 */

/**
 * Error thrown when an operation times out
 */
export class TimeoutError extends Error {
  constructor(
    public readonly operationName: string,
    public readonly timeoutMs: number
  ) {
    super(`Operation "${operationName}" timed out after ${timeoutMs}ms`);
    this.name = 'TimeoutError';
  }
}

/**
 * Options for withTimeout function
 */
export interface TimeoutOptions {
  /** Timeout in milliseconds */
  timeoutMs: number;
  /** Name of the operation (for error messages) */
  operationName: string;
  /** Optional AbortSignal for external cancellation */
  signal?: AbortSignal;
}

/**
 * Execute an operation with a timeout
 *
 * @param operation - The async operation to execute
 * @param options - Timeout configuration
 * @returns The result of the operation
 * @throws TimeoutError if the operation times out
 * @throws Error if the operation is aborted via signal
 *
 * @example
 * ```typescript
 * const result = await withTimeout(
 *   () => fetchData(),
 *   { timeoutMs: 5000, operationName: 'fetchData' }
 * );
 * ```
 */
export async function withTimeout<T>(operation: () => Promise<T>, options: TimeoutOptions): Promise<T> {
  const { timeoutMs, operationName, signal } = options;

  return new Promise((resolve, reject) => {
    let settled = false;

    // Setup timeout
    const timeoutId = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(new TimeoutError(operationName, timeoutMs));
      }
    }, timeoutMs);

    // Handle abort signal
    const abortHandler = () => {
      if (!settled) {
        settled = true;
        clearTimeout(timeoutId);
        reject(new Error(`Operation "${operationName}" aborted`));
      }
    };

    if (signal) {
      if (signal.aborted) {
        clearTimeout(timeoutId);
        reject(new Error(`Operation "${operationName}" aborted`));
        return;
      }
      signal.addEventListener('abort', abortHandler, { once: true });
    }

    // Execute operation
    operation()
      .then((result) => {
        if (!settled) {
          settled = true;
          clearTimeout(timeoutId);
          signal?.removeEventListener('abort', abortHandler);
          resolve(result);
        }
      })
      .catch((error) => {
        if (!settled) {
          settled = true;
          clearTimeout(timeoutId);
          signal?.removeEventListener('abort', abortHandler);
          reject(error);
        }
      });
  });
}

/**
 * Create a timeout promise that rejects after the specified time
 *
 * @param timeoutMs - Timeout in milliseconds
 * @param operationName - Name for error message
 * @returns A promise that rejects with TimeoutError after timeoutMs
 */
export function createTimeoutPromise(timeoutMs: number, operationName: string): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      reject(new TimeoutError(operationName, timeoutMs));
    }, timeoutMs);
  });
}

/**
 * Check if an error is a TimeoutError
 */
export function isTimeoutError(error: unknown): error is TimeoutError {
  return error instanceof TimeoutError;
}
