import { describe, expect, test } from 'bun:test';
import { BatchExecutor, categorizeErrorType, createBatchExecutor } from './BatchExecutor';

describe('categorizeErrorType', () => {
  test('categorizes timeout errors', () => {
    expect(categorizeErrorType(new Error('Request timeout'))).toBe('TIMEOUT_ERROR');
  });

  test('categorizes network errors', () => {
    expect(categorizeErrorType(new Error('network failure'))).toBe('NETWORK_ERROR');
    expect(categorizeErrorType(new Error('ECONNRESET'))).toBe('NETWORK_ERROR');
    expect(categorizeErrorType(new Error('ECONNREFUSED'))).toBe('NETWORK_ERROR');
  });

  test('categorizes rate limit errors', () => {
    expect(categorizeErrorType(new Error('Rate limit exceeded'))).toBe('RATE_LIMIT_ERROR');
    expect(categorizeErrorType(new Error('HTTP 429'))).toBe('RATE_LIMIT_ERROR');
  });

  test('categorizes auth errors', () => {
    expect(categorizeErrorType(new Error('401 Unauthorized'))).toBe('AUTH_ERROR');
    expect(categorizeErrorType(new Error('403 Forbidden'))).toBe('AUTH_ERROR');
    expect(categorizeErrorType(new Error('unauthorized access'))).toBe('AUTH_ERROR');
  });

  test('categorizes not found errors', () => {
    expect(categorizeErrorType(new Error('404 Not Found'))).toBe('NOT_FOUND_ERROR');
    expect(categorizeErrorType(new Error('resource not found'))).toBe('NOT_FOUND_ERROR');
  });

  test('categorizes server errors', () => {
    expect(categorizeErrorType(new Error('500 Internal'))).toBe('SERVER_ERROR');
    expect(categorizeErrorType(new Error('502 Bad Gateway'))).toBe('SERVER_ERROR');
    expect(categorizeErrorType(new Error('503 Unavailable'))).toBe('SERVER_ERROR');
    expect(categorizeErrorType(new Error('504 status'))).toBe('SERVER_ERROR');
  });

  test('categorizes abort errors', () => {
    expect(categorizeErrorType(new Error('AbortError'))).toBe('ABORT_ERROR');
    expect(categorizeErrorType(new Error('Operation cancelled'))).toBe('ABORT_ERROR');
  });

  test('returns UNKNOWN_ERROR for unrecognized errors', () => {
    expect(categorizeErrorType(new Error('something unexpected'))).toBe('UNKNOWN_ERROR');
    expect(categorizeErrorType('string error')).toBe('UNKNOWN_ERROR');
  });
});

describe('BatchExecutor', () => {
  test('constructor uses default config', () => {
    const executor = new BatchExecutor();
    const stats = executor.getStats();
    expect(stats.config.maxRetries).toBe(3);
    expect(stats.config.retryDelayMs).toBe(1000);
    expect(stats.config.maxRetryDelayMs).toBe(10000);
  });

  test('constructor accepts custom config', () => {
    const executor = new BatchExecutor({ maxRetries: 1, retryDelayMs: 100 });
    expect(executor.getStats().config.maxRetries).toBe(1);
    expect(executor.getStats().config.retryDelayMs).toBe(100);
  });

  test('execute runs operation successfully', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const result = await executor.execute(() => Promise.resolve(42));
    expect(result).toBe(42);
  });

  test('execute retries on failure', async () => {
    const executor = new BatchExecutor({ maxRetries: 2, retryDelayMs: 1, maxRetryDelayMs: 1 });
    let attempt = 0;

    const result = await executor.execute(async () => {
      attempt++;
      if (attempt < 3) throw new Error('fail');
      return 'success';
    });

    expect(result).toBe('success');
    expect(attempt).toBe(3);
  });

  test('execute throws after all retries exhausted', async () => {
    const executor = new BatchExecutor({ maxRetries: 1, retryDelayMs: 1, maxRetryDelayMs: 1 });

    await expect(executor.execute(() => Promise.reject(new Error('always fails')))).rejects.toThrow(
      'Operation failed after 2 attempts'
    );
  });

  test('executeAll runs operations sequentially (concurrency=1)', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const order: number[] = [];

    const ops = [1, 2, 3].map((n) => async () => {
      order.push(n);
      return n;
    });

    const results = await executor.executeAll(ops);
    expect(results).toEqual([1, 2, 3]);
    expect(order).toEqual([1, 2, 3]);
  });

  test('executeAll runs operations concurrently', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });

    const ops = [10, 20, 30].map((n) => async () => n);
    const results = await executor.executeAll(ops, { concurrency: 3 });

    expect(results.sort()).toEqual([10, 20, 30]);
  });

  test('executeAll reports progress', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const progressCalls: [number, number][] = [];

    await executor.executeAll([() => Promise.resolve(1), () => Promise.resolve(2)], {
      onProgress: (completed, total) => progressCalls.push([completed, total]),
    });

    expect(progressCalls).toEqual([
      [1, 2],
      [2, 2],
    ]);
  });

  test('executeAll respects AbortSignal (pre-aborted)', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const controller = new AbortController();
    controller.abort();

    await expect(executor.executeAll([() => Promise.resolve(1)], { signal: controller.signal })).rejects.toThrow(
      'Operation cancelled'
    );
  });

  test('executeAll handles abort during sequential execution', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const controller = new AbortController();

    const ops = [
      async () => {
        controller.abort();
        return 1;
      },
      async () => 2,
    ];

    await expect(executor.executeAll(ops, { signal: controller.signal })).rejects.toThrow('Operation cancelled');
  });

  test('executeAll handles abort during concurrent execution', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });
    const controller = new AbortController();

    const ops = [
      async () => {
        controller.abort();
        return 1;
      },
      async () => {
        await new Promise((r) => setTimeout(r, 50));
        return 2;
      },
    ];

    await expect(executor.executeAll(ops, { concurrency: 2, signal: controller.signal })).rejects.toThrow(
      'Operation cancelled'
    );
  });

  test('executeAll concurrent handles all failures', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });

    const ops = [() => Promise.reject(new Error('fail1')), () => Promise.reject(new Error('fail2'))];

    await expect(executor.executeAll(ops, { concurrency: 2 })).rejects.toThrow('All 2 operations failed');
  });

  test('executeAll concurrent returns partial results on some failures', async () => {
    const executor = new BatchExecutor({ maxRetries: 0 });

    const ops = [() => Promise.resolve(1), () => Promise.reject(new Error('fail')), () => Promise.resolve(3)];

    const results = await executor.executeAll(ops, { concurrency: 3 });
    expect(results).toContain(1);
    expect(results).toContain(3);
    expect(results.length).toBe(2);
  });

  test('getDetailedReport returns string', () => {
    const executor = new BatchExecutor();
    const report = executor.getDetailedReport();
    expect(report).toContain('BATCH EXECUTOR');
    expect(report).toContain('maxRetries=3');
  });

  test('reset does not throw', () => {
    const executor = new BatchExecutor();
    expect(() => executor.reset()).not.toThrow();
  });
});

describe('createBatchExecutor', () => {
  test('creates executor with default config', () => {
    const executor = createBatchExecutor();
    expect(executor).toBeInstanceOf(BatchExecutor);
  });

  test('creates executor with custom config', () => {
    const executor = createBatchExecutor({ maxRetries: 5 });
    expect(executor.getStats().config.maxRetries).toBe(5);
  });
});
