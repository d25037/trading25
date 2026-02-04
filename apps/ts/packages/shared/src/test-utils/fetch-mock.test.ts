import { describe, expect, it } from 'bun:test';
import { createMockErrorResponse, createMockResponse, createNetworkError, createTimeoutAbortError } from './fetch-mock';

describe('createMockResponse', () => {
  it('creates a 200 response with JSON data', async () => {
    const resp = createMockResponse({ key: 'value' });
    expect(resp.status).toBe(200);
    const data = await resp.json();
    expect(data).toEqual({ key: 'value' });
  });

  it('creates a custom status response', async () => {
    const resp = createMockResponse({ error: true }, 404);
    expect(resp.status).toBe(404);
  });
});

describe('createMockErrorResponse', () => {
  it('creates an error response', async () => {
    const resp = createMockErrorResponse('Not Found', 404);
    expect(resp.status).toBe(404);
    const data = (await resp.json()) as { message: string };
    expect(data.message).toBe('Not Found');
  });
});

describe('createNetworkError', () => {
  it('creates a TypeError', () => {
    const err = createNetworkError();
    expect(err).toBeInstanceOf(TypeError);
    expect(err.message).toBe('Failed to fetch');
  });
});

describe('createTimeoutAbortError', () => {
  it('creates an AbortError', () => {
    const err = createTimeoutAbortError();
    expect(err.name).toBe('AbortError');
  });
});
