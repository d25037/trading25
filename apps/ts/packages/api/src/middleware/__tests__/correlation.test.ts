import { describe, expect, test } from 'bun:test';
import { Hono } from 'hono';
import { CORRELATION_ID_HEADER, correlationMiddleware, createCorrelationId, getCorrelationId } from '../correlation';

describe('correlationMiddleware', () => {
  test('generates a correlation ID when none provided', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.get('/test', (c) => {
      const id = getCorrelationId(c);
      return c.json({ correlationId: id });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(200);

    const header = res.headers.get(CORRELATION_ID_HEADER);
    expect(header).toBeDefined();
    expect(typeof header).toBe('string');
    expect(header?.length).toBeGreaterThan(0);

    const body = (await res.json()) as Record<string, unknown>;
    expect(body.correlationId).toBe(header);
  });

  test('uses provided correlation ID from header', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.get('/test', (c) => {
      return c.json({ correlationId: getCorrelationId(c) });
    });

    const res = await app.request('/test', {
      headers: { [CORRELATION_ID_HEADER]: 'my-custom-id' },
    });

    const body = (await res.json()) as Record<string, unknown>;
    expect(body.correlationId).toBe('my-custom-id');
    expect(res.headers.get(CORRELATION_ID_HEADER)).toBe('my-custom-id');
  });
});

describe('createCorrelationId', () => {
  test('returns a string', () => {
    const id = createCorrelationId();
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
  });

  test('returns unique IDs', () => {
    const ids = new Set(Array.from({ length: 10 }, () => createCorrelationId()));
    expect(ids.size).toBe(10);
  });
});

describe('CORRELATION_ID_HEADER', () => {
  test('is x-correlation-id', () => {
    expect(CORRELATION_ID_HEADER).toBe('x-correlation-id');
  });
});
