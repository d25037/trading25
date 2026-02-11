import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import { BtApiProxyError, btGet } from './bt-api-proxy';

const originalFetch = globalThis.fetch;
const originalBtApiUrl = process.env.BT_API_URL;
const originalBtApiTimeout = process.env.BT_API_TIMEOUT;

function restoreEnv(): void {
  if (originalBtApiUrl === undefined) {
    delete process.env.BT_API_URL;
  } else {
    process.env.BT_API_URL = originalBtApiUrl;
  }

  if (originalBtApiTimeout === undefined) {
    delete process.env.BT_API_TIMEOUT;
  } else {
    process.env.BT_API_TIMEOUT = originalBtApiTimeout;
  }
}

async function expectBtApiProxyError<T>(promise: Promise<T>): Promise<BtApiProxyError> {
  try {
    await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(BtApiProxyError);
    return error as BtApiProxyError;
  }

  throw new Error('Expected promise to reject with BtApiProxyError');
}

describe('btGet', () => {
  beforeEach(() => {
    process.env.BT_API_URL = 'http://bt.local';
    delete process.env.BT_API_TIMEOUT;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    restoreEnv();
  });

  it('builds query string and parses JSON response', async () => {
    const fetchMock = mock(async () => new Response(JSON.stringify({ ok: true }), { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const result = await btGet<{ ok: boolean }>('/api/analytics/ranking', {
      date: '2025-01-15',
      limit: 5,
      ignored: undefined,
      markets: ['prime', 'standard'],
    });

    expect(result).toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    const [url] = fetchMock.mock.calls[0] ?? [];
    expect(String(url)).toBe(
      'http://bt.local/api/analytics/ranking?date=2025-01-15&limit=5&markets=prime&markets=standard'
    );
  });

  it('throws BtApiProxyError with upstream status for non-OK responses', async () => {
    const fetchMock = mock(
      async () =>
        new Response(JSON.stringify({ message: 'Symbol must be a valid 4-character stock code' }), { status: 400 })
    );
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const error = await expectBtApiProxyError(btGet('/api/analytics/factor-regression/@@@@'));
    expect(error.statusCode).toBe(400);
    expect(error.message).toBe('Symbol must be a valid 4-character stock code');
  });

  it('falls back to raw response text when upstream error body is not JSON', async () => {
    const fetchMock = mock(async () => new Response('database not ready', { status: 422 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const error = await expectBtApiProxyError(btGet('/api/analytics/ranking'));
    expect(error.statusCode).toBe(422);
    expect(error.message).toBe('database not ready');
  });

  it('throws on empty successful response body', async () => {
    const fetchMock = mock(async () => new Response('', { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const error = await expectBtApiProxyError(btGet('/api/analytics/ranking'));
    expect(error.statusCode).toBe(500);
    expect(error.message).toContain('Empty response body');
  });

  it('throws on invalid JSON successful response body', async () => {
    const fetchMock = mock(async () => new Response('not json', { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const error = await expectBtApiProxyError(btGet('/api/analytics/ranking'));
    expect(error.statusCode).toBe(500);
    expect(error.message).toContain('Invalid JSON response');
  });

  it('times out using configured timeout', async () => {
    process.env.BT_API_TIMEOUT = '1';

    const fetchMock = mock(
      (_url: string | URL | Request, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal;
          signal?.addEventListener('abort', () => reject(new Error('aborted')));
        })
    );
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const error = await expectBtApiProxyError(btGet('/api/analytics/ranking'));
    expect(error.statusCode).toBe(500);
    expect(error.message).toContain('timed out after 1ms');
  });

  it('uses default timeout when BT_API_TIMEOUT is invalid', async () => {
    process.env.BT_API_TIMEOUT = 'invalid';

    const fetchMock = mock(
      (_url: string | URL | Request, init?: RequestInit) =>
        new Promise<Response>((resolve, reject) => {
          const signal = init?.signal;
          signal?.addEventListener('abort', () => reject(new Error('aborted')));
          setTimeout(() => resolve(new Response(JSON.stringify({ ok: true }), { status: 200 })), 5);
        })
    );
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const result = await btGet<{ ok: boolean }>('/api/analytics/ranking');
    expect(result.ok).toBe(true);
  });
});
