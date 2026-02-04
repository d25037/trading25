import { describe, expect, test } from 'bun:test';
import { Hono } from 'hono';
import { handleDomainError, handleRouteError, type KnownErrorConfig, withErrorHandling } from '../route-handler';

describe('handleRouteError', () => {
  test('returns 500 for generic errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('boom'), 'corr-1', {
        operationName: 'test operation',
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Internal Server Error');
    expect(body.message).toBe('boom');
    expect(body.correlationId).toBe('corr-1');
  });

  test('returns 404 for "not found" errors via common mappings', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('Resource not found'), 'corr-2', {
        operationName: 'find resource',
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(404);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Not Found');
  });

  test('returns 409 for "already exists" errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('Item already exists'), 'corr-3', {
        operationName: 'create item',
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(409);
  });

  test('checks database errors when enabled', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('Database not initialized'), 'corr-4', {
        operationName: 'get data',
        checkDatabaseErrors: true,
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(422);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Unprocessable Entity');
  });

  test('respects custom error mappings', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('custom pattern match'), 'corr-5', {
        operationName: 'custom op',
        errorMappings: [{ pattern: 'custom pattern', errorType: 'Bad Request', statusCode: 400 }],
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(400);
  });

  test('uses custom defaultErrorType', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('unknown issue'), 'corr-6', {
        operationName: 'op',
        defaultErrorType: 'Not Implemented',
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Not Implemented');
  });

  test('logs at error level for 500-level custom mapping', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('critical failure'), 'corr-8', {
        operationName: 'critical op',
        errorMappings: [{ pattern: 'critical failure', errorType: 'Internal Server Error', statusCode: 500 }],
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Internal Server Error');
    expect(body.message).toBe('critical failure');
  });

  test('uses custom message from error mapping', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('server failure detected'), 'corr-9', {
        operationName: 'op',
        errorMappings: [
          {
            pattern: 'server failure',
            errorType: 'Internal Server Error',
            statusCode: 500,
            message: 'Custom 500 message',
          },
        ],
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.message).toBe('Custom 500 message');
  });

  test('handles non-Error objects', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, 'string error', 'corr-7', {
        operationName: 'op',
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
  });

  test('uses allowedStatusCodes to constrain mapped error status', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      // "not found" maps to 404 via COMMON_ERROR_MAPPINGS, but 404 is not in allowedStatusCodes
      return handleRouteError(c, new Error('Resource not found'), 'corr-10', {
        operationName: 'find resource',
        allowedStatusCodes: [400, 500] as const,
      });
    });

    const res = await app.request('/test');
    // 404 not allowed → falls back to 500
    expect(res.status).toBe(500);
  });

  test('uses allowedStatusCodes for default 500 error path', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('unknown'), 'corr-11', {
        operationName: 'op',
        allowedStatusCodes: [400, 500] as const,
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
  });

  test('uses allowedStatusCodes with database error path', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleRouteError(c, new Error('Database not initialized'), 'corr-12', {
        operationName: 'get data',
        checkDatabaseErrors: true,
        allowedStatusCodes: [422, 500] as const,
      });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(422);
  });

  test('warns when checkDatabaseErrors is true but 422 not in allowedStatusCodes', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      // This should trigger a warning log but still work (falls back via resolveAllowedStatus)
      return handleRouteError(c, new Error('some error'), 'corr-13', {
        operationName: 'get data',
        checkDatabaseErrors: true,
        allowedStatusCodes: [400, 500] as const,
      });
    });

    const res = await app.request('/test');
    // Not a database error, so it falls through to 500
    expect(res.status).toBe(500);
  });
});

describe('handleDomainError', () => {
  const classifyTestError = (error: unknown): KnownErrorConfig | null => {
    if (error instanceof TypeError) return { type: 'Bad Request', status: 400 };
    if (error instanceof RangeError) return { type: 'Not Found', status: 404 };
    return null;
  };

  test('returns classified error response for known errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleDomainError(c, new TypeError('invalid input'), 'corr-d1', 'validate', classifyTestError);
    });

    const res = await app.request('/test');
    expect(res.status).toBe(400);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Bad Request');
    expect(body.message).toBe('invalid input');
  });

  test('returns 500 for unknown errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleDomainError(c, new Error('unexpected'), 'corr-d2', 'process', classifyTestError);
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.error).toBe('Internal Server Error');
  });

  test('respects allowedStatusCodes for known errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      // RangeError classifies as 404, but only 400 and 500 are allowed
      return handleDomainError(c, new RangeError('out of range'), 'corr-d3', 'lookup', classifyTestError, undefined, [
        400, 500,
      ] as const);
    });

    const res = await app.request('/test');
    // 404 not allowed → falls back to 500
    expect(res.status).toBe(500);
  });

  test('respects allowedStatusCodes for unknown errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      return handleDomainError(c, new Error('unknown'), 'corr-d4', 'process', classifyTestError, { key: 'val' }, [
        400, 500,
      ] as const);
    });

    const res = await app.request('/test');
    expect(res.status).toBe(500);
  });
});

describe('withErrorHandling', () => {
  test('passes through successful handler responses', async () => {
    const app = new Hono();

    const handler = withErrorHandling(async (c) => c.json({ ok: true }, 200), { operationName: 'test' });

    app.get('/test', handler);

    const res = await app.request('/test');
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.ok).toBe(true);
  });

  test('catches errors and returns standardized response', async () => {
    const app = new Hono();

    const handler = withErrorHandling(
      async () => {
        throw new Error('handler failed');
      },
      { operationName: 'failing op' }
    );

    app.get('/test', handler);

    const res = await app.request('/test');
    expect(res.status).toBe(500);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.message).toBe('handler failed');
  });

  test('uses getLogContext when provided', async () => {
    const app = new Hono();

    const handler = withErrorHandling(
      async () => {
        throw new Error('Resource not found');
      },
      {
        operationName: 'find',
        getLogContext: () => ({ key: 'value' }),
      }
    );

    app.get('/test', handler);

    const res = await app.request('/test');
    expect(res.status).toBe(404);
  });

  test('uses correlationId from header', async () => {
    const app = new Hono();

    const handler = withErrorHandling(
      async () => {
        throw new Error('fail');
      },
      { operationName: 'test' }
    );

    app.get('/test', handler);

    const res = await app.request('/test', {
      headers: { 'x-correlation-id': 'custom-corr' },
    });
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.correlationId).toBe('custom-corr');
  });
});
