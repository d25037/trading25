import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, apiDelete, apiGet, apiPost, apiPut, buildQueryString, buildUrl } from './api-client';

describe('ApiError', () => {
  it('creates error with status and details', () => {
    const error = new ApiError('Not Found', 404, { reason: 'missing' });
    expect(error.message).toBe('Not Found');
    expect(error.status).toBe(404);
    expect(error.details).toEqual({ reason: 'missing' });
    expect(error.name).toBe('ApiError');
  });

  it('isClientError returns true for 4xx', () => {
    expect(new ApiError('', 400).isClientError()).toBe(true);
    expect(new ApiError('', 404).isClientError()).toBe(true);
    expect(new ApiError('', 499).isClientError()).toBe(true);
  });

  it('isClientError returns false for non-4xx', () => {
    expect(new ApiError('', 200).isClientError()).toBe(false);
    expect(new ApiError('', 500).isClientError()).toBe(false);
  });

  it('isServerError returns true for 5xx', () => {
    expect(new ApiError('', 500).isServerError()).toBe(true);
    expect(new ApiError('', 503).isServerError()).toBe(true);
  });

  it('isServerError returns false for non-5xx', () => {
    expect(new ApiError('', 404).isServerError()).toBe(false);
    expect(new ApiError('', 200).isServerError()).toBe(false);
  });
});

describe('buildQueryString', () => {
  it('builds query string from params', () => {
    const result = buildQueryString({ a: '1', b: 2, c: true });
    expect(result).toContain('a=1');
    expect(result).toContain('b=2');
    expect(result).toContain('c=true');
  });

  it('filters out undefined values', () => {
    const result = buildQueryString({ a: '1', b: undefined });
    expect(result).toBe('a=1');
  });

  it('returns empty string for empty params', () => {
    expect(buildQueryString({})).toBe('');
  });
});

describe('buildUrl', () => {
  it('returns path without params', () => {
    expect(buildUrl('/api/test')).toBe('/api/test');
  });

  it('appends query string when params provided', () => {
    expect(buildUrl('/api/test', { limit: 10 })).toBe('/api/test?limit=10');
  });

  it('returns path when all params undefined', () => {
    expect(buildUrl('/api/test', { a: undefined })).toBe('/api/test');
  });
});

describe('API methods', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  function mockFetch(status: number, body: unknown, ok?: boolean) {
    const response = {
      ok: ok ?? (status >= 200 && status < 300),
      status,
      statusText: status === 200 ? 'OK' : 'Error',
      json: vi.fn().mockResolvedValue(body),
    } as unknown as Response;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    return response;
  }

  describe('apiGet', () => {
    it('returns data on success', async () => {
      mockFetch(200, { data: 'test' });
      const result = await apiGet('/api/test');
      expect(result).toEqual({ data: 'test' });
      expect(fetch).toHaveBeenCalledWith('/api/test');
    });

    it('appends query params', async () => {
      mockFetch(200, {});
      await apiGet('/api/test', { limit: 5 });
      expect(fetch).toHaveBeenCalledWith('/api/test?limit=5');
    });

    it('throws ApiError on HTTP error', async () => {
      mockFetch(404, { message: 'Not found' }, false);
      await expect(apiGet('/api/test')).rejects.toThrow(ApiError);
      try {
        await apiGet('/api/test');
      } catch (e) {
        expect((e as ApiError).status).toBe(404);
      }
    });

    it('uses statusText when response body has no message', async () => {
      const response = {
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        json: vi.fn().mockRejectedValue(new Error('parse error')),
      } as unknown as Response;
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
      await expect(apiGet('/api/test')).rejects.toThrow('Request failed: Internal Server Error');
    });
  });

  describe('apiPost', () => {
    it('sends POST with JSON body', async () => {
      mockFetch(200, { id: 1 });
      const result = await apiPost('/api/test', { name: 'foo' });
      expect(result).toEqual({ id: 1 });
      expect(fetch).toHaveBeenCalledWith('/api/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'foo' }),
      });
    });

    it('sends POST without body', async () => {
      mockFetch(200, {});
      await apiPost('/api/test');
      expect(fetch).toHaveBeenCalledWith('/api/test', {
        method: 'POST',
        headers: undefined,
        body: undefined,
      });
    });

    it('throws ApiError on HTTP error', async () => {
      mockFetch(400, { message: 'Bad request' }, false);
      await expect(apiPost('/api/test', {})).rejects.toThrow(ApiError);
    });
  });

  describe('apiPut', () => {
    it('sends PUT with JSON body', async () => {
      mockFetch(200, { updated: true });
      const result = await apiPut('/api/test/1', { name: 'bar' });
      expect(result).toEqual({ updated: true });
      expect(fetch).toHaveBeenCalledWith('/api/test/1', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'bar' }),
      });
    });

    it('throws ApiError on HTTP error', async () => {
      mockFetch(500, { message: 'Server error' }, false);
      await expect(apiPut('/api/test/1', {})).rejects.toThrow(ApiError);
    });
  });

  describe('apiDelete', () => {
    it('sends DELETE request', async () => {
      mockFetch(200, { deleted: true });
      const result = await apiDelete('/api/test/1');
      expect(result).toEqual({ deleted: true });
      expect(fetch).toHaveBeenCalledWith('/api/test/1', { method: 'DELETE' });
    });

    it('throws ApiError on HTTP error', async () => {
      mockFetch(403, { message: 'Forbidden' }, false);
      await expect(apiDelete('/api/test/1')).rejects.toThrow(ApiError);
    });
  });
});
