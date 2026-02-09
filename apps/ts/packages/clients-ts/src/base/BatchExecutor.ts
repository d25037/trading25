/**
 * BatchExecutor - Handles batch execution with retry logic
 * Rate limiting is handled by BaseJQuantsClient, not here
 */

export interface BatchExecutorConfig {
  maxRetries: number;
  retryDelayMs: number;
  maxRetryDelayMs: number;
}

interface ConcurrentExecutionState<T> {
  results: (T | undefined)[];
  failedCount: number;
  completedCount: number;
  currentIndex: number;
  cancelled: boolean;
}

const DEFAULT_CONFIG: BatchExecutorConfig = {
  maxRetries: 3,
  retryDelayMs: 1000,
  maxRetryDelayMs: 10000,
};

/**
 * Categorize error type based on error message patterns
 */
export function categorizeErrorType(error: unknown): string {
  const errorStr = String(error).toLowerCase();

  if (errorStr.includes('timeout')) return 'TIMEOUT_ERROR';
  if (errorStr.includes('network') || errorStr.includes('econnreset') || errorStr.includes('econnrefused'))
    return 'NETWORK_ERROR';
  if (errorStr.includes('rate limit') || errorStr.includes('429')) return 'RATE_LIMIT_ERROR';
  if (errorStr.includes('401') || errorStr.includes('403') || errorStr.includes('unauthorized')) return 'AUTH_ERROR';
  if (errorStr.includes('404') || errorStr.includes('not found')) return 'NOT_FOUND_ERROR';
  if (errorStr.includes('500') || errorStr.includes('502') || errorStr.includes('503') || errorStr.includes('504'))
    return 'SERVER_ERROR';
  if (errorStr.includes('abort') || errorStr.includes('cancel')) return 'ABORT_ERROR';

  return 'UNKNOWN_ERROR';
}

/**
 * Simple batch executor for running operations with retry logic
 */
export class BatchExecutor {
  private readonly config: BatchExecutorConfig;

  constructor(config?: Partial<BatchExecutorConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Execute a single operation with retry logic
   */
  async execute<T>(operation: () => Promise<T>): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        if (attempt < this.config.maxRetries) {
          await this.waitWithBackoff(attempt);
        }
      }
    }

    throw new Error(`Operation failed after ${this.config.maxRetries + 1} attempts: ${lastError?.message}`);
  }

  /**
   * Execute multiple operations with concurrency control
   */
  async executeAll<T>(
    operations: (() => Promise<T>)[],
    options?: {
      concurrency?: number;
      signal?: AbortSignal;
      onProgress?: (completed: number, total: number) => void;
    }
  ): Promise<T[]> {
    const concurrency = options?.concurrency ?? 1;
    const signal = options?.signal;
    const onProgress = options?.onProgress;

    if (signal?.aborted) {
      throw new Error('Operation cancelled');
    }

    if (concurrency <= 1) {
      return this.executeSequential(operations, onProgress, signal);
    }

    return this.executeConcurrent(operations, concurrency, onProgress, signal);
  }

  private async executeSequential<T>(
    operations: (() => Promise<T>)[],
    onProgress?: (completed: number, total: number) => void,
    signal?: AbortSignal
  ): Promise<T[]> {
    const results: T[] = [];

    for (let i = 0; i < operations.length; i++) {
      if (signal?.aborted) {
        throw new Error('Operation cancelled');
      }

      const operation = operations[i];
      if (!operation) {
        throw new Error(`Operation at index ${i} is undefined`);
      }

      const result = await this.executeWithRetry(operation, signal);
      results.push(result);
      onProgress?.(i + 1, operations.length);
    }

    return results;
  }

  private async executeConcurrent<T>(
    operations: (() => Promise<T>)[],
    concurrency: number,
    onProgress?: (completed: number, total: number) => void,
    signal?: AbortSignal
  ): Promise<T[]> {
    const state = this.createConcurrentState<T>(operations.length);

    const executeNext = async (): Promise<void> => {
      while (state.currentIndex < operations.length && !state.cancelled) {
        if (signal?.aborted) {
          state.cancelled = true;
          break;
        }

        const index = state.currentIndex++;
        const operation = operations[index];
        if (!operation) continue;

        const shouldStop = await this.executeOperation(operation, index, state, signal);
        if (shouldStop) break;

        state.completedCount++;
        onProgress?.(state.completedCount, operations.length);
      }
    };

    const workerCount = Math.min(concurrency, operations.length);
    const workers = Array.from({ length: workerCount }, () => executeNext());
    await Promise.all(workers);

    return this.finalizeConcurrentResults(state, operations.length, signal);
  }

  private createConcurrentState<T>(length: number): ConcurrentExecutionState<T> {
    return {
      results: new Array(length),
      failedCount: 0,
      completedCount: 0,
      currentIndex: 0,
      cancelled: false,
    };
  }

  private async executeOperation<T>(
    operation: () => Promise<T>,
    index: number,
    state: ConcurrentExecutionState<T>,
    signal?: AbortSignal
  ): Promise<boolean> {
    try {
      const result = await this.executeWithRetry(operation, signal);
      state.results[index] = result;
      return false;
    } catch (error) {
      return this.handleOperationError(error, state, signal);
    }
  }

  private handleOperationError<T>(error: unknown, state: ConcurrentExecutionState<T>, signal?: AbortSignal): boolean {
    const errorObj = error instanceof Error ? error : new Error(String(error));
    if (errorObj.message === 'Operation cancelled' || signal?.aborted) {
      state.cancelled = true;
      return true;
    }
    state.failedCount++;
    return false;
  }

  private finalizeConcurrentResults<T>(
    state: ConcurrentExecutionState<T>,
    totalOperations: number,
    signal?: AbortSignal
  ): T[] {
    if (state.cancelled || signal?.aborted) {
      throw new Error('Operation cancelled');
    }

    if (state.failedCount === totalOperations) {
      throw new Error(`All ${totalOperations} operations failed`);
    }

    // Filter out undefined values from failed operations
    // This maintains type safety: Promise<T[]> returns only successful results
    return state.results.filter((r): r is T => r !== undefined);
  }

  private async executeWithRetry<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      if (signal?.aborted) {
        throw new Error('Operation cancelled');
      }

      try {
        return await operation();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        if (attempt < this.config.maxRetries) {
          await this.waitWithBackoff(attempt);
        }
      }
    }

    throw new Error(`Operation failed after ${this.config.maxRetries + 1} attempts: ${lastError?.message}`);
  }

  private async waitWithBackoff(attempt: number): Promise<void> {
    const baseDelay = this.config.retryDelayMs * 2 ** attempt;
    const cappedDelay = Math.min(baseDelay, this.config.maxRetryDelayMs);
    const jitter = Math.random() * cappedDelay;
    await new Promise((resolve) => setTimeout(resolve, jitter));
  }

  /**
   * Get executor statistics (minimal implementation for compatibility)
   */
  getStats(): { config: BatchExecutorConfig } {
    return { config: this.config };
  }

  /**
   * Get detailed performance report (minimal implementation for compatibility)
   */
  getDetailedReport(): string {
    return `[BATCH EXECUTOR] Config: maxRetries=${this.config.maxRetries}, retryDelayMs=${this.config.retryDelayMs}`;
  }

  /**
   * Reset executor state (no-op for this simple implementation)
   */
  reset(): void {
    // No state to reset in this simple implementation
  }
}

/**
 * Create a default batch executor
 */
export function createBatchExecutor(config?: Partial<BatchExecutorConfig>): BatchExecutor {
  return new BatchExecutor(config);
}
