import { describe, expect, it } from 'bun:test';
import { BadRequestError } from '../../errors';
import {
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  WatchlistError,
  WatchlistItemNotFoundError,
  WatchlistNameNotFoundError,
  WatchlistNotFoundError,
} from '../types';

describe('WatchlistError', () => {
  it('has default code WATCHLIST_ERROR', () => {
    const err = new WatchlistError('test');
    expect(err.code).toBe('WATCHLIST_ERROR');
    expect(err.httpStatus).toBe(400);
    expect(err.name).toBe('WatchlistError');
  });

  it('accepts custom code', () => {
    const err = new WatchlistError('test', 'CUSTOM_CODE');
    expect(err.code).toBe('CUSTOM_CODE');
  });

  it('extends BadRequestError', () => {
    const err = new WatchlistError('test');
    expect(err).toBeInstanceOf(BadRequestError);
  });
});

describe('WatchlistNotFoundError', () => {
  it('has correct code and message', () => {
    const err = new WatchlistNotFoundError(42);
    expect(err.code).toBe('WATCHLIST_NOT_FOUND');
    expect(err.message).toBe('Watchlist with ID 42 not found');
  });
});

describe('WatchlistNameNotFoundError', () => {
  it('has correct code and message', () => {
    const err = new WatchlistNameNotFoundError('Tech Stocks');
    expect(err.code).toBe('WATCHLIST_NAME_NOT_FOUND');
    expect(err.message).toContain('Tech Stocks');
  });
});

describe('WatchlistItemNotFoundError', () => {
  it('has correct code and message', () => {
    const err = new WatchlistItemNotFoundError(99);
    expect(err.code).toBe('ITEM_NOT_FOUND');
    expect(err.message).toContain('99');
  });
});

describe('StockNotFoundInWatchlistError', () => {
  it('has correct code and message', () => {
    const err = new StockNotFoundInWatchlistError('7203', 1);
    expect(err.code).toBe('STOCK_NOT_FOUND_IN_WATCHLIST');
    expect(err.message).toContain('7203');
    expect(err.message).toContain('1');
  });
});

describe('DuplicateWatchlistStockError', () => {
  it('has correct code and message', () => {
    const err = new DuplicateWatchlistStockError('7203', 1);
    expect(err.code).toBe('DUPLICATE_STOCK');
    expect(err.message).toContain('7203');
  });
});

describe('DuplicateWatchlistNameError', () => {
  it('has correct code and message', () => {
    const err = new DuplicateWatchlistNameError('My List');
    expect(err.code).toBe('DUPLICATE_NAME');
    expect(err.message).toContain('My List');
  });
});
