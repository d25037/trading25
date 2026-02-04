import { describe, expect, test } from 'bun:test';
import { BadRequestError, ConflictError, NotFoundError } from '@trading25/shared';
import { Hono } from 'hono';
import { correlationMiddleware } from '../correlation';
import { errorHandler, httpLogger, requestLogger } from '../http-logger';

describe('httpLogger', () => {
  test('logs and passes through successful requests', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.use('*', httpLogger());
    app.get('/test', (c) => c.json({ ok: true }));

    const res = await app.request('/test');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.ok).toBe(true);
  });

  test('re-throws errors from downstream', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.use('*', httpLogger());
    app.get('/test', () => {
      throw new Error('downstream error');
    });

    // Hono catches errors internally, returning 500
    const res = await app.request('/test');
    expect(res.status).toBe(500);
  });
});

describe('requestLogger', () => {
  test('returns an array of middleware handlers', () => {
    const middleware = requestLogger();
    expect(Array.isArray(middleware)).toBe(true);
    expect(middleware.length).toBe(2);
  });
});

describe('errorHandler', () => {
  test('returns 500 for generic errors with correlation ID', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.use('*', errorHandler());
    app.get('/test', async () => {
      throw new Error('unexpected error');
    });

    const res = await app.request('/test');
    // The error handler middleware catches the error and returns JSON 500
    expect(res.status).toBe(500);
  });

  test('handles non-Error thrown values', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.use('*', errorHandler());
    app.get('/test', async () => {
      throw 'string error';
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
  });

  test('passes through successful requests', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.use('*', errorHandler());
    app.get('/test', (c) => c.json({ ok: true }));

    const res = await app.request('/test');
    expect(res.status).toBe(200);
  });

  test('returns 400 for BadRequestError via app.onError', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.onError((err, c) => {
      const handler = errorHandler();
      // Simulate middleware catch block by invoking handler with a next that throws
      return handler(c, () => {
        throw err;
      }) as unknown as Response;
    });
    app.get('/test', async () => {
      throw new BadRequestError('Invalid input');
    });

    const res = await app.request('/test');
    expect(res.status).toBe(400);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Bad Request');
    expect(body.message).toBe('Invalid input');
  });

  test('returns 404 for NotFoundError via app.onError', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.onError((err, c) => {
      const handler = errorHandler();
      return handler(c, () => {
        throw err;
      }) as unknown as Response;
    });
    app.get('/test', async () => {
      throw new NotFoundError('Resource not found');
    });

    const res = await app.request('/test');
    expect(res.status).toBe(404);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Not Found');
  });

  test('returns 409 for ConflictError via app.onError', async () => {
    const app = new Hono();
    app.use('*', correlationMiddleware);
    app.onError((err, c) => {
      const handler = errorHandler();
      return handler(c, () => {
        throw err;
      }) as unknown as Response;
    });
    app.get('/test', async () => {
      throw new ConflictError('Already exists');
    });

    const res = await app.request('/test');
    expect(res.status).toBe(409);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Conflict');
  });
});
