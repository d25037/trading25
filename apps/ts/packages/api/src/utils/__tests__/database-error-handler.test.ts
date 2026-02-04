import { describe, expect, test } from 'bun:test';
import { Hono } from 'hono';
import {
  COMMON_ERROR_MAPPINGS,
  detectDatabaseError,
  findErrorMapping,
  handleDatabaseError,
} from '../database-error-handler';

describe('detectDatabaseError', () => {
  test('detects "not_initialized" errors', () => {
    const result = detectDatabaseError('Database not initialized');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('not_initialized');
  });

  test('detects "not_initialized" case-insensitively', () => {
    const result = detectDatabaseError('database not ready for queries');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('not_initialized');
  });

  test('detects "no_data" errors', () => {
    const result = detectDatabaseError('No trading data for 2024-01-01');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('no_data');
  });

  test('detects "No data available"', () => {
    const result = detectDatabaseError('No data available for this period');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('no_data');
  });

  test('detects "Insufficient data"', () => {
    const result = detectDatabaseError('Insufficient data for analysis');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('no_data');
  });

  test('detects "table_missing" errors', () => {
    const result = detectDatabaseError('no such table: stocks');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('table_missing');
  });

  test('detects "sqlite_error" errors', () => {
    const result = detectDatabaseError('SQLITE_ERROR: something failed');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('sqlite_error');
  });

  test('detects SQLITE_CONSTRAINT', () => {
    const result = detectDatabaseError('SQLITE_CONSTRAINT: unique violation');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('sqlite_error');
  });

  test('detects SQLITE_BUSY', () => {
    const result = detectDatabaseError('SQLITE_BUSY: database is locked');
    expect(result.isDatabaseError).toBe(true);
    expect(result.errorType).toBe('sqlite_error');
  });

  test('returns false for non-database errors', () => {
    const result = detectDatabaseError('Something unexpected happened');
    expect(result.isDatabaseError).toBe(false);
    expect(result.errorType).toBeNull();
  });
});

describe('handleDatabaseError', () => {
  test('returns null for non-database errors', async () => {
    const app = new Hono();
    let responseResult: Response | null = null;

    app.get('/test', (c) => {
      responseResult = handleDatabaseError(c, 'Unknown error', 'corr-1');
      return c.json({ ok: true });
    });

    await app.request('/test');
    expect(responseResult).toBeNull();
  });

  test('returns 422 for database errors', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      const response = handleDatabaseError(c, 'Database not initialized', 'corr-2');
      if (response) return response;
      return c.json({ ok: true });
    });

    const res = await app.request('/test');
    expect(res.status).toBe(422);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.status).toBe('error');
    expect(body.error).toBe('Unprocessable Entity');
    expect(body.correlationId).toBe('corr-2');
  });

  test('uses custom notReadyMessage', async () => {
    const app = new Hono();

    app.get('/test', (c) => {
      const response = handleDatabaseError(c, 'Database not initialized', 'corr-3', {
        notReadyMessage: 'Custom message',
      });
      if (response) return response;
      return c.json({ ok: true });
    });

    const res = await app.request('/test');
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.message).toBe('Custom message');
  });
});

describe('findErrorMapping', () => {
  test('finds matching mapping case-insensitively', () => {
    const mapping = findErrorMapping('Resource Not Found in database', COMMON_ERROR_MAPPINGS);
    expect(mapping).toBeDefined();
    expect(mapping?.errorType).toBe('Not Found');
    expect(mapping?.statusCode).toBe(404);
  });

  test('finds "Insufficient data" mapping', () => {
    const mapping = findErrorMapping('Insufficient data for analysis', COMMON_ERROR_MAPPINGS);
    expect(mapping).toBeDefined();
    expect(mapping?.errorType).toBe('Unprocessable Entity');
    expect(mapping?.statusCode).toBe(422);
  });

  test('finds "already exists" mapping', () => {
    const mapping = findErrorMapping('Portfolio already exists', COMMON_ERROR_MAPPINGS);
    expect(mapping).toBeDefined();
    expect(mapping?.errorType).toBe('Conflict');
    expect(mapping?.statusCode).toBe(409);
  });

  test('finds "already running" mapping', () => {
    const mapping = findErrorMapping('Job already running', COMMON_ERROR_MAPPINGS);
    expect(mapping).toBeDefined();
    expect(mapping?.statusCode).toBe(409);
  });

  test('returns undefined for unmatched error', () => {
    const mapping = findErrorMapping('Something random', COMMON_ERROR_MAPPINGS);
    expect(mapping).toBeUndefined();
  });

  test('works with custom mappings', () => {
    const customMappings = [{ pattern: 'custom error', errorType: 'Bad Request' as const, statusCode: 400 }];
    const mapping = findErrorMapping('This is a custom error case', customMappings);
    expect(mapping).toBeDefined();
    expect(mapping?.statusCode).toBe(400);
  });
});
