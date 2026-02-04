import { describe, expect, test } from 'bun:test';
import {
  createErrorResponse,
  ERROR_STATUS_CODES,
  type ErrorStatusCode,
  isErrorStatusCode,
  resolveAllowedStatus,
} from '../error-responses';

describe('createErrorResponse', () => {
  test('creates error response with all fields', () => {
    const result = createErrorResponse({
      error: 'Bad Request',
      message: 'Invalid input',
      correlationId: 'test-123',
      details: [{ field: 'name', message: 'required' }],
    });

    expect(result.status).toBe('error');
    expect(result.error).toBe('Bad Request');
    expect(result.message).toBe('Invalid input');
    expect(result.correlationId).toBe('test-123');
    expect(result.details).toEqual([{ field: 'name', message: 'required' }]);
    expect(result.timestamp).toBeDefined();
  });

  test('creates error response without details', () => {
    const result = createErrorResponse({
      error: 'Not Found',
      message: 'Resource not found',
      correlationId: 'test-456',
    });

    expect(result.status).toBe('error');
    expect(result.error).toBe('Not Found');
    expect(result.details).toBeUndefined();
  });

  test('generates valid ISO timestamp', () => {
    const result = createErrorResponse({
      error: 'Internal Server Error',
      message: 'Something went wrong',
      correlationId: 'test-789',
    });

    expect(() => new Date(result.timestamp)).not.toThrow();
  });
});

describe('isErrorStatusCode', () => {
  test('returns true for valid error status codes', () => {
    for (const code of ERROR_STATUS_CODES) {
      expect(isErrorStatusCode(code)).toBe(true);
    }
  });

  test('returns false for non-error status codes', () => {
    expect(isErrorStatusCode(200)).toBe(false);
    expect(isErrorStatusCode(201)).toBe(false);
    expect(isErrorStatusCode(301)).toBe(false);
    expect(isErrorStatusCode(418)).toBe(false);
  });
});

describe('resolveAllowedStatus', () => {
  test('returns statusCode as-is when no allowedStatusCodes', () => {
    expect(resolveAllowedStatus(400)).toBe(400);
    expect(resolveAllowedStatus(500)).toBe(500);
  });

  test('returns statusCode as-is when allowedStatusCodes is empty', () => {
    expect(resolveAllowedStatus(404, [] as readonly ErrorStatusCode[])).toBe(404);
  });

  test('returns statusCode when it is in allowedStatusCodes', () => {
    expect(resolveAllowedStatus(400, [400, 500])).toBe(400);
    expect(resolveAllowedStatus(404, [404, 500])).toBe(404);
  });

  test('falls back to 500 when statusCode not in allowed and 500 is allowed', () => {
    expect(resolveAllowedStatus(404, [400, 500])).toBe(500);
  });

  test('falls back to first allowed code when statusCode and 500 not in allowed', () => {
    expect(resolveAllowedStatus(404, [400, 422])).toBe(400);
  });
});
