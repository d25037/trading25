import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import { describe, expect, it } from 'vitest';
import { ApiError } from '@/lib/api-client';
import { shouldRetry } from './QueryProvider';

describe('shouldRetry', () => {
  it.each([new ApiError('bad request', 400), new HttpRequestError('bad request', 'http', { status: 400 })])(
    'does not retry non-transient client failures',
    (error) => {
      expect(shouldRetry(0, error)).toBe(false);
    }
  );

  it.each([
    new ApiError('request timeout', 408),
    new ApiError('rate limited', 429),
    new ApiError('server error', 500),
    new HttpRequestError('request timeout', 'http', { status: 408 }),
    new HttpRequestError('rate limited', 'http', { status: 429 }),
    new HttpRequestError('server error', 'http', { status: 500 }),
    new HttpRequestError('network unavailable', 'network'),
    new HttpRequestError('request timed out', 'timeout'),
  ])('retries transient failures below the attempt cap', (error) => {
    expect(shouldRetry(0, error)).toBe(true);
    expect(shouldRetry(2, error)).toBe(true);
    expect(shouldRetry(3, error)).toBe(false);
  });
});
