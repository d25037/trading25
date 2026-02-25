import { beforeEach, describe, expect, it, mock } from 'bun:test';

const requestJsonMock = mock();

type MockHttpRequestErrorKind = 'http' | 'network' | 'timeout' | 'invalid-json';

class MockHttpRequestError extends Error {
  readonly kind: MockHttpRequestErrorKind;
  readonly status?: number;
  readonly statusText?: string;
  readonly body?: unknown;

  constructor(
    message: string,
    kind: MockHttpRequestErrorKind,
    options: { status?: number; statusText?: string; body?: unknown; cause?: unknown } = {}
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = 'HttpRequestError';
    this.kind = kind;
    this.status = options.status;
    this.statusText = options.statusText;
    this.body = options.body;
  }
}

mock.module('@trading25/api-clients/base/http-client', () => ({
  HttpRequestError: MockHttpRequestError,
  requestJson: requestJsonMock,
}));

import { BaseApiClient, toQueryString } from './base-client.js';

class TestApiClient extends BaseApiClient {
  callRequest<T>(endpoint: string, options?: RequestInit): Promise<T> {
    return this.request<T>(endpoint, options);
  }
}

describe('BaseApiClient', () => {
  beforeEach(() => {
    requestJsonMock.mockClear();
  });

  it('builds query strings while skipping undefined values', () => {
    const query = toQueryString({
      symbol: '7203',
      limit: 10,
      includeDetails: false,
      empty: undefined,
    });

    expect(query).toBe('symbol=7203&limit=10&includeDetails=false');
  });

  it('forwards requests with merged headers and baseUrl', async () => {
    requestJsonMock.mockResolvedValueOnce({ ok: true });
    const client = new TestApiClient('http://localhost:3002');

    const result = await client.callRequest('/api/test', {
      method: 'POST',
      headers: {
        Authorization: 'Bearer token',
      },
      body: JSON.stringify({ ok: true }),
    });

    expect(result).toEqual({ ok: true });
    expect(requestJsonMock).toHaveBeenCalledTimes(1);
    expect(requestJsonMock).toHaveBeenCalledWith('/api/test', {
      baseUrl: 'http://localhost:3002',
      method: 'POST',
      body: JSON.stringify({ ok: true }),
      headers: {
        'Content-Type': 'application/json',
        Authorization: 'Bearer token',
      },
    });
  });

  it('maps HTTP errors with explicit message body', async () => {
    requestJsonMock.mockRejectedValueOnce(
      new MockHttpRequestError('Bad Request', 'http', {
        status: 400,
        body: { message: 'Validation failed' },
      })
    );
    const client = new TestApiClient('http://localhost:3002');

    await expect(client.callRequest('/api/test')).rejects.toThrow('Validation failed');
  });

  it('maps HTTP errors without message body to status fallback', async () => {
    requestJsonMock.mockRejectedValueOnce(
      new MockHttpRequestError('Bad Request', 'http', {
        status: 503,
        body: { detail: 'service unavailable' },
      })
    );
    const client = new TestApiClient('http://localhost:3002');

    await expect(client.callRequest('/api/test')).rejects.toThrow('HTTP error! status: 503');
  });

  it('maps connection errors to actionable guidance', async () => {
    requestJsonMock.mockRejectedValueOnce(new MockHttpRequestError('fetch failed', 'network'));
    const client = new TestApiClient('http://localhost:3002');

    await expect(client.callRequest('/api/test')).rejects.toThrow(
      'Cannot connect to API server. Please ensure bt FastAPI is running with "uv run bt server --port 3002"'
    );
  });

  it('preserves generic error messages', async () => {
    requestJsonMock.mockRejectedValueOnce(new Error('Something unexpected happened'));
    const client = new TestApiClient('http://localhost:3002');

    await expect(client.callRequest('/api/test')).rejects.toThrow('Something unexpected happened');
  });

  it('returns unknown error fallback for non-error values', async () => {
    requestJsonMock.mockRejectedValueOnce(1234);
    const client = new TestApiClient('http://localhost:3002');

    await expect(client.callRequest('/api/test')).rejects.toThrow('Unknown error occurred');
  });
});
