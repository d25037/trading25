import { describe, expect, it } from 'bun:test';
import {
  BadRequestError,
  ConflictError,
  getErrorMessage,
  InternalError,
  isTrading25Error,
  NotFoundError,
} from '../index';

describe('BadRequestError', () => {
  it('has code BAD_REQUEST and httpStatus 400', () => {
    const err = new BadRequestError('invalid input');
    expect(err.code).toBe('BAD_REQUEST');
    expect(err.httpStatus).toBe(400);
    expect(err.message).toBe('invalid input');
    expect(err.name).toBe('BadRequestError');
  });
});

describe('NotFoundError', () => {
  it('has code NOT_FOUND and httpStatus 404', () => {
    const err = new NotFoundError('not found');
    expect(err.code).toBe('NOT_FOUND');
    expect(err.httpStatus).toBe(404);
  });
});

describe('ConflictError', () => {
  it('has code CONFLICT and httpStatus 409', () => {
    const err = new ConflictError('conflict');
    expect(err.code).toBe('CONFLICT');
    expect(err.httpStatus).toBe(409);
  });
});

describe('InternalError', () => {
  it('has code INTERNAL_ERROR and httpStatus 500', () => {
    const err = new InternalError('server error');
    expect(err.code).toBe('INTERNAL_ERROR');
    expect(err.httpStatus).toBe(500);
  });
});

describe('isTrading25Error', () => {
  it('returns true for Trading25Error subclasses', () => {
    expect(isTrading25Error(new BadRequestError('test'))).toBe(true);
    expect(isTrading25Error(new NotFoundError('test'))).toBe(true);
    expect(isTrading25Error(new ConflictError('test'))).toBe(true);
    expect(isTrading25Error(new InternalError('test'))).toBe(true);
  });

  it('returns false for regular Error', () => {
    expect(isTrading25Error(new Error('test'))).toBe(false);
  });

  it('returns false for non-Error values', () => {
    expect(isTrading25Error('error')).toBe(false);
    expect(isTrading25Error(null)).toBe(false);
    expect(isTrading25Error(undefined)).toBe(false);
  });
});

describe('getErrorMessage', () => {
  it('extracts message from Error', () => {
    expect(getErrorMessage(new Error('test message'))).toBe('test message');
  });

  it('converts string to string', () => {
    expect(getErrorMessage('string error')).toBe('string error');
  });

  it('converts null to string', () => {
    expect(getErrorMessage(null)).toBe('null');
  });
});

describe('Trading25Error cause', () => {
  it('preserves cause', () => {
    const cause = new Error('original');
    const err = new BadRequestError('wrapped', cause);
    expect(err.cause).toBe(cause);
  });
});
