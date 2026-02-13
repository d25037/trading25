import type { Mock } from 'bun:test';
import { afterEach, beforeEach, describe, expect, spyOn, test } from 'bun:test';
import { buildQueryString, buildUrl, extractErrorMessage, HttpRequestError, requestJson } from './http-client.js';

describe('http-client', () => {
  describe('buildQueryString', () => {
    test('filters out null and undefined values', () => {
      const result = buildQueryString({ a: '1', b: undefined, c: null, d: true });
      expect(result).toBe('a=1&d=true');
    });
  });

  describe('buildUrl', () => {
    test('appends query to relative path', () => {
      expect(buildUrl('/api/test', { limit: 10 })).toBe('/api/test?limit=10');
    });

    test('appends query to absolute URL with baseUrl', () => {
      expect(buildUrl('/api/test', { limit: 10 }, 'http://localhost:3002')).toBe('http://localhost:3002/api/test?limit=10');
    });

    test('appends query with ampersand when path already has query', () => {
      expect(buildUrl('/api/test?offset=10', { limit: 5 })).toBe('/api/test?offset=10&limit=5');
    });

    test('appends query to absolute URL path', () => {
      expect(buildUrl('https://example.test/path', { q: 'x' })).toBe('https://example.test/path?q=x');
    });
  });

  describe('extractErrorMessage', () => {
    test('extracts message from known payload fields and strings', () => {
      expect(extractErrorMessage({ message: 'msg' })).toBe('msg');
      expect(extractErrorMessage({ error: 'err' })).toBe('err');
      expect(extractErrorMessage({ detail: 'detail' })).toBe('detail');
      expect(extractErrorMessage('raw message')).toBe('raw message');
    });

    test('returns undefined for unsupported values', () => {
      expect(extractErrorMessage({})).toBeUndefined();
      expect(extractErrorMessage(42)).toBeUndefined();
      expect(extractErrorMessage('')).toBeUndefined();
    });
  });

  describe('requestJson', () => {
    let fetchSpy: Mock<typeof fetch>;
    let originalNodeEnv: string | undefined;

    beforeEach(() => {
      originalNodeEnv = process.env.NODE_ENV;
      fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
        Promise.resolve(new Response(JSON.stringify({ ok: true })))) as unknown as typeof fetch);
    });

    afterEach(() => {
      fetchSpy.mockRestore();
      process.env.NODE_ENV = originalNodeEnv;
    });

    test('calls fetch without init when no request options are provided', async () => {
      await requestJson('/api/health');
      const call = fetchSpy.mock.calls.at(-1);
      expect(call?.[0]).toBe('/api/health');
      expect(call?.length).toBe(1);
    });

    test('builds URL from baseUrl and query', async () => {
      await requestJson('/api/health', {
        baseUrl: 'http://localhost:3002',
        query: { limit: 5, skip: undefined },
      });
      expect(fetchSpy.mock.calls.at(-1)?.[0]).toBe('http://localhost:3002/api/health?limit=5');
    });

    test('combines timeout signal with caller signal and cleans listeners', async () => {
      const controller = new AbortController();
      const addListenerSpy = spyOn(controller.signal, 'addEventListener');
      const removeListenerSpy = spyOn(controller.signal, 'removeEventListener');

      await requestJson('/api/health', {
        signal: controller.signal,
        timeoutMs: 100,
      });

      const init = fetchSpy.mock.calls.at(-1)?.[1] as RequestInit | undefined;
      expect(init).toBeDefined();
      expect(init?.signal).toBeDefined();
      expect(addListenerSpy.mock.calls.length).toBeGreaterThan(0);
      expect(removeListenerSpy.mock.calls.length).toBeGreaterThan(0);

      addListenerSpy.mockRestore();
      removeListenerSpy.mockRestore();
    });

    test('passes an already-aborted signal through timeout wrapper', async () => {
      const controller = new AbortController();
      controller.abort();

      await requestJson('/api/health', {
        signal: controller.signal,
        timeoutMs: 100,
      });

      const init = fetchSpy.mock.calls.at(-1)?.[1] as RequestInit | undefined;
      expect((init?.signal as AbortSignal).aborted).toBe(true);
    });

    test('throws HttpRequestError for HTTP status errors', async () => {
      fetchSpy.mockResolvedValueOnce(
        new Response(JSON.stringify({ message: 'Not Found' }), { status: 404, statusText: 'Not Found' })
      );

      const error = await requestJson('/api/missing').catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('http');
      expect(requestError.status).toBe(404);
      expect(requestError.message).toBe('Not Found');
    });

    test('handles non-JSON HTTP error bodies and emits observations outside test mode', async () => {
      process.env.NODE_ENV = 'development';
      const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
      fetchSpy.mockResolvedValueOnce(new Response('Service down', { status: 503, statusText: 'Service Unavailable' }));

      const error = await requestJson('/api/error').catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('http');
      expect(requestError.status).toBe(503);
      expect(requestError.body).toBe('Service down');
      expect(requestError.message).toBe('Service down');
      expect(
        warnSpy.mock.calls.some(([message]) => String(message).includes('[http-client] Received non-JSON error response body'))
      ).toBe(true);

      warnSpy.mockRestore();
    });

    test('falls back to status text when non-JSON body cannot be read', async () => {
      process.env.NODE_ENV = 'development';
      const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
      fetchSpy.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Server Error',
        json: async (): Promise<never> => {
          throw new Error('json parse failed');
        },
        clone: () => ({
          text: async (): Promise<never> => {
            throw new Error('text read failed');
          },
        }),
      } as unknown as Response);

      const error = await requestJson('/api/error').catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('http');
      expect(requestError.status).toBe(500);
      expect(requestError.message).toBe('Server Error');
      expect(requestError.body).toBeUndefined();
      expect(
        warnSpy.mock.calls.some(([message]) =>
          String(message).includes('[http-client] Failed to read non-JSON error response body')
        )
      ).toBe(true);

      warnSpy.mockRestore();
    });

    test('throws HttpRequestError for invalid JSON response', async () => {
      fetchSpy.mockResolvedValueOnce(new Response('plain text', { status: 200, statusText: 'OK' }));

      const error = await requestJson('/api/plain-text').catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('invalid-json');
    });

    test('throws HttpRequestError for network failures', async () => {
      fetchSpy.mockRejectedValueOnce(new Error('connection reset'));

      const error = await requestJson('/api/network').catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('network');
      expect(requestError.message).toContain('connection reset');
    });

    test('throws timeout errors when fetch is aborted by timeout signal', async () => {
      fetchSpy.mockImplementationOnce(((_url: string | URL | Request, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal as AbortSignal | undefined;
          if (!signal) {
            reject(new Error('missing signal'));
            return;
          }

          if (signal.aborted) {
            reject(new DOMException('Aborted', 'AbortError'));
            return;
          }

          signal.addEventListener(
            'abort',
            () => {
              reject(new DOMException('Aborted', 'AbortError'));
            },
            { once: true }
          );
        })) as unknown as typeof fetch);

      const error = await requestJson('/api/slow', { timeoutMs: 10 }).catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(HttpRequestError);
      const requestError = error as HttpRequestError;
      expect(requestError.kind).toBe('timeout');
      expect(requestError.message).toContain('10ms');
    });
  });
});
