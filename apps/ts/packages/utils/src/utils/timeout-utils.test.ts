import { describe, expect, it } from 'bun:test';
import { createTimeoutPromise, isTimeoutError, TimeoutError, withTimeout } from './timeout-utils';

describe('TimeoutError', () => {
  it('has correct name, operationName, and timeoutMs', () => {
    const err = new TimeoutError('fetchData', 5000);
    expect(err.name).toBe('TimeoutError');
    expect(err.operationName).toBe('fetchData');
    expect(err.timeoutMs).toBe(5000);
  });

  it('has correct message format', () => {
    const err = new TimeoutError('sync', 3000);
    expect(err.message).toBe('Operation "sync" timed out after 3000ms');
  });

  it('is an instance of Error', () => {
    const err = new TimeoutError('op', 100);
    expect(err).toBeInstanceOf(Error);
  });
});

describe('withTimeout', () => {
  it('resolves when operation completes before timeout', async () => {
    const result = await withTimeout(() => Promise.resolve(42), {
      timeoutMs: 1000,
      operationName: 'test',
    });
    expect(result).toBe(42);
  });

  it('rejects with TimeoutError when operation exceeds timeout', async () => {
    const slowOp = () => new Promise<number>((resolve) => setTimeout(() => resolve(42), 200));
    await expect(withTimeout(slowOp, { timeoutMs: 10, operationName: 'slowOp' })).rejects.toThrow(TimeoutError);
  });

  it('propagates operation rejection', async () => {
    const failOp = () => Promise.reject(new Error('operation failed'));
    await expect(withTimeout(failOp, { timeoutMs: 1000, operationName: 'failOp' })).rejects.toThrow('operation failed');
  });

  it('rejects immediately when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(
      withTimeout(() => Promise.resolve(42), {
        timeoutMs: 1000,
        operationName: 'aborted',
        signal: controller.signal,
      })
    ).rejects.toThrow('aborted');
  });

  it('rejects when signal is aborted during operation', async () => {
    const controller = new AbortController();
    const slowOp = () => new Promise<number>((resolve) => setTimeout(() => resolve(42), 500));
    setTimeout(() => controller.abort(), 10);
    await expect(
      withTimeout(slowOp, {
        timeoutMs: 5000,
        operationName: 'midAbort',
        signal: controller.signal,
      })
    ).rejects.toThrow('aborted');
  });
});

describe('createTimeoutPromise', () => {
  it('rejects with TimeoutError after specified ms', async () => {
    await expect(createTimeoutPromise(10, 'testOp')).rejects.toThrow(TimeoutError);
  });
});

describe('isTimeoutError', () => {
  it('returns true for TimeoutError instances', () => {
    expect(isTimeoutError(new TimeoutError('op', 100))).toBe(true);
  });

  it('returns false for regular Error', () => {
    expect(isTimeoutError(new Error('oops'))).toBe(false);
  });

  it('returns false for non-Error values', () => {
    expect(isTimeoutError('timeout')).toBe(false);
    expect(isTimeoutError(null)).toBe(false);
  });
});
