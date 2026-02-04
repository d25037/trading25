import { Database } from 'bun:sqlite';
import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  WatchlistError,
  WatchlistItemNotFoundError,
  WatchlistNotFoundError,
} from '../watchlist/types';
import { DrizzleWatchlistDatabase } from './drizzle-watchlist-database';

/**
 * Create portfolio_metadata table that the watchlist DB depends on
 * (watchlist shares the portfolio.db file)
 */
function createPrerequisiteSchema(dbPath: string): void {
  const sqlite = new Database(dbPath);
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS portfolio_metadata (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
  sqlite.close();
}

describe('DrizzleWatchlistDatabase', () => {
  let db: DrizzleWatchlistDatabase;
  let testDbPath: string;
  let testDir: string;

  beforeEach(() => {
    testDir = join(tmpdir(), `drizzle-watchlist-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    mkdirSync(testDir, { recursive: true });
    testDbPath = join(testDir, 'test-watchlist.db');
    createPrerequisiteSchema(testDbPath);
    db = new DrizzleWatchlistDatabase(testDbPath);
  });

  afterEach(() => {
    db.close();
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('Watchlist CRUD', () => {
    test('creates a watchlist', () => {
      const watchlist = db.createWatchlist({ name: 'Tech Stocks' });
      expect(watchlist.name).toBe('Tech Stocks');
      expect(watchlist.id).toBeGreaterThan(0);
      expect(watchlist.createdAt).toBeInstanceOf(Date);
    });

    test('creates watchlist with description', () => {
      const watchlist = db.createWatchlist({
        name: 'Growth',
        description: 'High growth stocks',
      });
      expect(watchlist.description).toBe('High growth stocks');
    });

    test('throws on duplicate watchlist name', () => {
      db.createWatchlist({ name: 'Tech' });
      expect(() => db.createWatchlist({ name: 'Tech' })).toThrow(DuplicateWatchlistNameError);
    });

    test('gets watchlist by id', () => {
      const created = db.createWatchlist({ name: 'Test' });
      const found = db.getWatchlist(created.id);
      expect(found).not.toBeNull();
      expect(found?.name).toBe('Test');
    });

    test('returns null for nonexistent id', () => {
      expect(db.getWatchlist(9999)).toBeNull();
    });

    test('gets watchlist by name', () => {
      db.createWatchlist({ name: 'MyList' });
      const found = db.getWatchlistByName('MyList');
      expect(found).not.toBeNull();
      expect(found?.name).toBe('MyList');
    });

    test('returns null for nonexistent name', () => {
      expect(db.getWatchlistByName('nonexistent')).toBeNull();
    });

    test('lists all watchlists', () => {
      db.createWatchlist({ name: 'List1' });
      db.createWatchlist({ name: 'List2' });
      const lists = db.listWatchlists();
      expect(lists.length).toBe(2);
    });

    test('updates watchlist name', () => {
      const created = db.createWatchlist({ name: 'Old' });
      const updated = db.updateWatchlist(created.id, { name: 'New' });
      expect(updated.name).toBe('New');
    });

    test('updates watchlist description', () => {
      const created = db.createWatchlist({ name: 'Test' });
      const updated = db.updateWatchlist(created.id, { description: 'Updated desc' });
      expect(updated.description).toBe('Updated desc');
    });

    test('update with no changes returns existing', () => {
      const created = db.createWatchlist({ name: 'Test' });
      const updated = db.updateWatchlist(created.id, {});
      expect(updated.name).toBe('Test');
    });

    test('update throws for nonexistent id', () => {
      expect(() => db.updateWatchlist(9999, { name: 'New' })).toThrow(WatchlistNotFoundError);
    });

    test('update throws on duplicate name', () => {
      db.createWatchlist({ name: 'A' });
      const b = db.createWatchlist({ name: 'B' });
      expect(() => db.updateWatchlist(b.id, { name: 'A' })).toThrow(DuplicateWatchlistNameError);
    });

    test('deletes watchlist', () => {
      const created = db.createWatchlist({ name: 'ToDelete' });
      db.deleteWatchlist(created.id);
      expect(db.getWatchlist(created.id)).toBeNull();
    });

    test('delete throws for nonexistent id', () => {
      expect(() => db.deleteWatchlist(9999)).toThrow(WatchlistNotFoundError);
    });
  });

  describe('Watchlist Items', () => {
    let watchlistId: number;

    beforeEach(() => {
      const wl = db.createWatchlist({ name: 'TestList' });
      watchlistId = wl.id;
    });

    test('adds item to watchlist', () => {
      const item = db.addItem({
        watchlistId,
        code: '7203',
        companyName: 'Toyota',
      });
      expect(item.code).toBe('7203');
      expect(item.companyName).toBe('Toyota');
      expect(item.watchlistId).toBe(watchlistId);
    });

    test('adds item with memo', () => {
      const item = db.addItem({
        watchlistId,
        code: '7203',
        companyName: 'Toyota',
        memo: 'Large cap',
      });
      expect(item.memo).toBe('Large cap');
    });

    test('throws on duplicate stock in same watchlist', () => {
      db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      expect(() => db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' })).toThrow(
        DuplicateWatchlistStockError
      );
    });

    test('throws when adding to nonexistent watchlist', () => {
      expect(() => db.addItem({ watchlistId: 9999, code: '7203', companyName: 'Toyota' })).toThrow(
        WatchlistNotFoundError
      );
    });

    test('throws on invalid stock code', () => {
      expect(() => db.addItem({ watchlistId, code: 'INVALID', companyName: 'Bad' })).toThrow(WatchlistError);
    });

    test('gets item by id', () => {
      const added = db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      const found = db.getItem(added.id);
      expect(found).not.toBeNull();
      expect(found?.code).toBe('7203');
    });

    test('returns null for nonexistent item id', () => {
      expect(db.getItem(9999)).toBeNull();
    });

    test('lists items in watchlist', () => {
      db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      db.addItem({ watchlistId, code: '6758', companyName: 'Sony' });
      const items = db.listItems(watchlistId);
      expect(items.length).toBe(2);
    });

    test('deletes item by id', () => {
      const added = db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      db.deleteItem(added.id);
      expect(db.getItem(added.id)).toBeNull();
    });

    test('delete item throws for nonexistent id', () => {
      expect(() => db.deleteItem(9999)).toThrow(WatchlistItemNotFoundError);
    });

    test('gets item by code', () => {
      db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      const found = db.getItemByCode(watchlistId, '7203');
      expect(found).not.toBeNull();
      expect(found?.code).toBe('7203');
    });

    test('returns null for nonexistent code', () => {
      expect(db.getItemByCode(watchlistId, '9999')).toBeNull();
    });

    test('deletes item by code', () => {
      db.addItem({ watchlistId, code: '7203', companyName: 'Toyota' });
      const deleted = db.deleteItemByCode(watchlistId, '7203');
      expect(deleted.code).toBe('7203');
      expect(db.getItemByCode(watchlistId, '7203')).toBeNull();
    });

    test('deleteItemByCode throws for nonexistent stock', () => {
      expect(() => db.deleteItemByCode(watchlistId, '9999')).toThrow(StockNotFoundInWatchlistError);
    });
  });

  describe('Watchlist with Items', () => {
    test('getWatchlistWithItems returns watchlist and items', () => {
      const wl = db.createWatchlist({ name: 'Full' });
      db.addItem({ watchlistId: wl.id, code: '7203', companyName: 'Toyota' });
      db.addItem({ watchlistId: wl.id, code: '6758', companyName: 'Sony' });

      const result = db.getWatchlistWithItems(wl.id);
      expect(result).not.toBeNull();
      expect(result?.name).toBe('Full');
      expect(result?.items.length).toBe(2);
    });

    test('getWatchlistWithItems returns null for nonexistent id', () => {
      expect(db.getWatchlistWithItems(9999)).toBeNull();
    });
  });

  describe('Watchlist Summary', () => {
    test('getWatchlistSummary returns stock count', () => {
      const wl = db.createWatchlist({ name: 'Summary' });
      db.addItem({ watchlistId: wl.id, code: '7203', companyName: 'Toyota' });
      db.addItem({ watchlistId: wl.id, code: '6758', companyName: 'Sony' });

      const summary = db.getWatchlistSummary(wl.id);
      expect(summary).not.toBeNull();
      expect(summary?.stockCount).toBe(2);
    });

    test('getWatchlistSummary returns null for nonexistent', () => {
      expect(db.getWatchlistSummary(9999)).toBeNull();
    });

    test('listWatchlistSummaries returns all summaries', () => {
      const wl1 = db.createWatchlist({ name: 'List1' });
      db.createWatchlist({ name: 'List2' });
      db.addItem({ watchlistId: wl1.id, code: '7203', companyName: 'Toyota' });

      const summaries = db.listWatchlistSummaries();
      expect(summaries.length).toBe(2);
      const s1 = summaries.find((s) => s.name === 'List1');
      const s2 = summaries.find((s) => s.name === 'List2');
      expect(s1?.stockCount).toBe(1);
      expect(s2?.stockCount).toBe(0);
    });
  });

  describe('Cascade Delete', () => {
    test('deleting watchlist removes its items', () => {
      const wl = db.createWatchlist({ name: 'Cascade' });
      db.addItem({ watchlistId: wl.id, code: '7203', companyName: 'Toyota' });
      db.deleteWatchlist(wl.id);
      expect(db.listItems(wl.id)).toEqual([]);
    });
  });

  describe('Debug Mode', () => {
    test('debug mode does not throw', () => {
      const debugPath = join(testDir, 'debug.db');
      createPrerequisiteSchema(debugPath);
      const debugDb = new DrizzleWatchlistDatabase(debugPath, true);
      const wl = debugDb.createWatchlist({ name: 'Debug' });
      debugDb.addItem({ watchlistId: wl.id, code: '7203', companyName: 'Toyota' });
      debugDb.deleteItemByCode(wl.id, '7203');
      debugDb.deleteWatchlist(wl.id);
      debugDb.close();
    });
  });
});
