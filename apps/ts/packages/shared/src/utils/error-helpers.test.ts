import { describe, expect, it } from 'bun:test';
import {
  determineRetryability,
  getErrorMessage,
  getErrorStack,
  isNetworkError,
  isRateLimitError,
  isRetryableHttpStatus,
  isTemporaryServerError,
  isTimeoutError,
} from './error-helpers';

describe('getErrorMessage', () => {
  it('extracts message from Error', () => {
    expect(getErrorMessage(new Error('test'))).toBe('test');
  });

  it('converts string', () => {
    expect(getErrorMessage('some string')).toBe('some string');
  });

  it('converts number', () => {
    expect(getErrorMessage(42)).toBe('42');
  });

  it('converts object', () => {
    expect(getErrorMessage({ key: 'val' })).toBe('[object Object]');
  });
});

describe('getErrorStack', () => {
  it('returns stack for Error', () => {
    const err = new Error('test');
    expect(getErrorStack(err)).toBeDefined();
    expect(getErrorStack(err)).toContain('test');
  });

  it('returns undefined for non-Error', () => {
    expect(getErrorStack('string')).toBeUndefined();
    expect(getErrorStack(42)).toBeUndefined();
  });
});

describe('isNetworkError', () => {
  it('detects network errors', () => {
    expect(isNetworkError(new Error('network failure'))).toBe(true);
    expect(isNetworkError(new Error('ECONNREFUSED'))).toBe(true);
    expect(isNetworkError(new Error('connection reset'))).toBe(true);
    expect(isNetworkError(new Error('ENOTFOUND'))).toBe(true);
    expect(isNetworkError(new Error('ECONNRESET'))).toBe(true);
  });

  it('returns false for non-network errors', () => {
    expect(isNetworkError(new Error('validation failed'))).toBe(false);
  });
});

describe('isRateLimitError', () => {
  it('detects rate limit errors', () => {
    const result = isRateLimitError(new Error('rate limit exceeded'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(5000);
  });

  it('detects too many requests', () => {
    const result = isRateLimitError(new Error('too many requests'));
    expect(result.isRetryable).toBe(true);
  });

  it('extracts retry-after value', () => {
    const result = isRateLimitError(new Error('rate limit exceeded, retry_after: 10'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(10000);
  });

  it('returns not retryable for other errors', () => {
    const result = isRateLimitError(new Error('some other error'));
    expect(result.isRetryable).toBe(false);
  });
});

describe('isTemporaryServerError', () => {
  it('detects server errors', () => {
    expect(isTemporaryServerError(new Error('500 Internal Server Error'))).toBe(true);
    expect(isTemporaryServerError(new Error('502 Bad Gateway'))).toBe(true);
    expect(isTemporaryServerError(new Error('service unavailable'))).toBe(true);
    expect(isTemporaryServerError(new Error('gateway timeout'))).toBe(true);
  });

  it('returns false for other errors', () => {
    expect(isTemporaryServerError(new Error('404'))).toBe(false);
  });
});

describe('isTimeoutError', () => {
  it('detects timeout errors', () => {
    expect(isTimeoutError(new Error('request timeout'))).toBe(true);
    expect(isTimeoutError(new Error('ETIMEDOUT'))).toBe(true);
  });

  it('returns false for non-timeout errors', () => {
    expect(isTimeoutError(new Error('not found'))).toBe(false);
  });
});

describe('isRetryableHttpStatus', () => {
  it('identifies retryable statuses', () => {
    expect(isRetryableHttpStatus(408)).toBe(true);
    expect(isRetryableHttpStatus(429)).toBe(true);
    expect(isRetryableHttpStatus(500)).toBe(true);
    expect(isRetryableHttpStatus(502)).toBe(true);
    expect(isRetryableHttpStatus(503)).toBe(true);
    expect(isRetryableHttpStatus(504)).toBe(true);
  });

  it('rejects non-retryable statuses', () => {
    expect(isRetryableHttpStatus(200)).toBe(false);
    expect(isRetryableHttpStatus(400)).toBe(false);
    expect(isRetryableHttpStatus(404)).toBe(false);
  });
});

describe('determineRetryability', () => {
  it('returns not retryable for non-Error', () => {
    expect(determineRetryability('string error').isRetryable).toBe(false);
  });

  it('detects rate limit errors', () => {
    const result = determineRetryability(new Error('rate limit exceeded'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(5000);
  });

  it('detects network errors', () => {
    const result = determineRetryability(new Error('ECONNREFUSED'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(1000);
  });

  it('detects timeout errors', () => {
    const result = determineRetryability(new Error('ETIMEDOUT'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(2000);
  });

  it('detects server errors', () => {
    const result = determineRetryability(new Error('internal server error'));
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(3000);
  });

  it('detects retryable API status codes', () => {
    const apiError = Object.assign(new Error('API error'), { status: 429 });
    const result = determineRetryability(apiError);
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(5000);
  });

  it('detects retryable API response status', () => {
    const apiError = Object.assign(new Error('API error'), { response: { status: 503 } });
    const result = determineRetryability(apiError);
    expect(result.isRetryable).toBe(true);
    expect(result.retryAfterMs).toBe(2000);
  });

  it('returns not retryable for normal errors', () => {
    expect(determineRetryability(new Error('validation failed')).isRetryable).toBe(false);
  });
});
