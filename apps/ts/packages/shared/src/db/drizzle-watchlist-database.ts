/**
 * Drizzle-based Watchlist Database
 *
 * Type-safe watchlist management using Drizzle ORM.
 * Shares portfolio.db with the portfolio module.
 */

import { Database } from 'bun:sqlite';
import { and, eq, sql } from 'drizzle-orm';
import type { BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';
import { drizzle } from 'drizzle-orm/bun-sqlite';
import {
  type CreateWatchlistInput,
  type CreateWatchlistItemInput,
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  StockNotFoundInWatchlistError,
  type UpdateWatchlistInput,
  type Watchlist,
  WatchlistError,
  type WatchlistItem,
  WatchlistItemNotFoundError,
  WatchlistNotFoundError,
  type WatchlistSummary,
  type WatchlistWithItems,
} from '../watchlist/types';
import { isValidStockCode, normalizeStockCode } from './columns/stock-code';
import { portfolioMetadata } from './schema/portfolio-schema';
import { WATCHLIST_SCHEMA_VERSION, watchlistItems, watchlists } from './schema/watchlist-schema';

export class DrizzleWatchlistDatabase {
  private sqlite: Database;
  private db: BunSQLiteDatabase;

  constructor(
    dbPath: string,
    private debug: boolean = false
  ) {
    this.sqlite = new Database(dbPath);
    this.db = drizzle(this.sqlite);
    this.initializeSchema();
  }

  private initializeSchema(): void {
    this.sqlite.exec('PRAGMA journal_mode = WAL');
    this.sqlite.exec('PRAGMA foreign_keys = ON');

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS watchlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS watchlist_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        watchlist_id INTEGER NOT NULL,
        code TEXT NOT NULL,
        company_name TEXT NOT NULL,
        memo TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE,
        UNIQUE(watchlist_id, code)
      );
    `);

    this.sqlite.exec(`
      CREATE INDEX IF NOT EXISTS idx_watchlist_items_watchlist_id
        ON watchlist_items(watchlist_id);
      CREATE INDEX IF NOT EXISTS idx_watchlist_items_code
        ON watchlist_items(code);
    `);

    this.setMetadata('watchlist_schema_version', WATCHLIST_SCHEMA_VERSION);
  }

  private setMetadata(key: string, value: string): void {
    this.db
      .insert(portfolioMetadata)
      .values({ key, value, updatedAt: sql`CURRENT_TIMESTAMP` })
      .onConflictDoUpdate({
        target: portfolioMetadata.key,
        set: { value, updatedAt: sql`CURRENT_TIMESTAMP` },
      })
      .run();
  }

  private assertValidStockCode(code: string): void {
    if (!isValidStockCode(code)) {
      throw new WatchlistError(
        `Invalid stock code: ${code}. Must be 4 characters (e.g., 7203 or 285A).`,
        'INVALID_STOCK_CODE'
      );
    }
  }

  createWatchlist(input: CreateWatchlistInput): Watchlist {
    try {
      const result = this.db
        .insert(watchlists)
        .values({
          name: input.name,
          description: input.description ?? null,
          createdAt: sql`CURRENT_TIMESTAMP`,
          updatedAt: sql`CURRENT_TIMESTAMP`,
        })
        .returning()
        .get();

      if (this.debug) {
        console.log(`[DrizzleWatchlistDatabase] Created watchlist: ${input.name} (ID: ${result.id})`);
      }

      return this.mapWatchlistRow(result);
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicateWatchlistNameError(input.name);
      }
      throw error;
    }
  }

  getWatchlist(id: number): Watchlist | null {
    const result = this.db.select().from(watchlists).where(eq(watchlists.id, id)).get();
    return result ? this.mapWatchlistRow(result) : null;
  }

  getWatchlistByName(name: string): Watchlist | null {
    const result = this.db.select().from(watchlists).where(eq(watchlists.name, name)).get();
    return result ? this.mapWatchlistRow(result) : null;
  }

  listWatchlists(): Watchlist[] {
    const results = this.db.select().from(watchlists).orderBy(sql`created_at DESC`).all();
    return results.map((row) => this.mapWatchlistRow(row));
  }

  updateWatchlist(id: number, input: UpdateWatchlistInput): Watchlist {
    const existing = this.getWatchlist(id);
    if (!existing) {
      throw new WatchlistNotFoundError(id);
    }

    const updateData: Record<string, unknown> = { updatedAt: sql`CURRENT_TIMESTAMP` };
    if (input.name !== undefined) updateData.name = input.name;
    if (input.description !== undefined) updateData.description = input.description;

    if (Object.keys(updateData).length === 1) {
      return existing;
    }

    try {
      this.db.update(watchlists).set(updateData).where(eq(watchlists.id, id)).run();

      if (this.debug) {
        console.log(`[DrizzleWatchlistDatabase] Updated watchlist ID: ${id}`);
      }

      const updated = this.getWatchlist(id);
      if (!updated) {
        throw new WatchlistError('Failed to update watchlist');
      }
      return updated;
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicateWatchlistNameError(input.name ?? '');
      }
      throw error;
    }
  }

  deleteWatchlist(id: number): void {
    const existing = this.getWatchlist(id);
    if (!existing) {
      throw new WatchlistNotFoundError(id);
    }

    this.db.delete(watchlists).where(eq(watchlists.id, id)).run();

    if (this.debug) {
      console.log(`[DrizzleWatchlistDatabase] Deleted watchlist ID: ${id} (${existing.name})`);
    }
  }

  addItem(input: CreateWatchlistItemInput): WatchlistItem {
    const normalizedCode = normalizeStockCode(input.code);
    this.assertValidStockCode(normalizedCode);

    const watchlist = this.getWatchlist(input.watchlistId);
    if (!watchlist) {
      throw new WatchlistNotFoundError(input.watchlistId);
    }

    try {
      const result = this.db
        .insert(watchlistItems)
        .values({
          watchlistId: input.watchlistId,
          code: normalizedCode,
          companyName: input.companyName,
          memo: input.memo ?? null,
          createdAt: sql`CURRENT_TIMESTAMP`,
        })
        .returning()
        .get();

      if (this.debug) {
        console.log(`[DrizzleWatchlistDatabase] Added item: ${normalizedCode} to watchlist ${input.watchlistId}`);
      }

      return this.mapWatchlistItemRow(result);
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicateWatchlistStockError(normalizedCode, input.watchlistId);
      }
      throw error;
    }
  }

  getItem(id: number): WatchlistItem | null {
    const result = this.db.select().from(watchlistItems).where(eq(watchlistItems.id, id)).get();
    return result ? this.mapWatchlistItemRow(result) : null;
  }

  listItems(watchlistId: number): WatchlistItem[] {
    const results = this.db
      .select()
      .from(watchlistItems)
      .where(eq(watchlistItems.watchlistId, watchlistId))
      .orderBy(sql`created_at DESC`)
      .all();
    return results.map((row) => this.mapWatchlistItemRow(row));
  }

  deleteItem(id: number): void {
    const existing = this.getItem(id);
    if (!existing) {
      throw new WatchlistItemNotFoundError(id);
    }

    this.db.delete(watchlistItems).where(eq(watchlistItems.id, id)).run();

    if (this.debug) {
      console.log(`[DrizzleWatchlistDatabase] Deleted item ID: ${id} (${existing.code})`);
    }
  }

  getItemByCode(watchlistId: number, code: string): WatchlistItem | null {
    const normalizedCode = normalizeStockCode(code);
    const result = this.db
      .select()
      .from(watchlistItems)
      .where(and(eq(watchlistItems.watchlistId, watchlistId), eq(watchlistItems.code, normalizedCode)))
      .get();
    return result ? this.mapWatchlistItemRow(result) : null;
  }

  deleteItemByCode(watchlistId: number, code: string): WatchlistItem {
    const normalizedCode = normalizeStockCode(code);
    const existing = this.getItemByCode(watchlistId, normalizedCode);
    if (!existing) {
      throw new StockNotFoundInWatchlistError(normalizedCode, watchlistId);
    }

    this.db
      .delete(watchlistItems)
      .where(and(eq(watchlistItems.watchlistId, watchlistId), eq(watchlistItems.code, normalizedCode)))
      .run();

    if (this.debug) {
      console.log(`[DrizzleWatchlistDatabase] Deleted item: ${normalizedCode} from watchlist ${watchlistId}`);
    }

    return existing;
  }

  getWatchlistWithItems(id: number): WatchlistWithItems | null {
    const watchlist = this.getWatchlist(id);
    if (!watchlist) return null;

    const items = this.listItems(id);
    return { ...watchlist, items };
  }

  getWatchlistSummary(id: number): WatchlistSummary | null {
    const watchlist = this.getWatchlist(id);
    if (!watchlist) return null;

    const result = this.db
      .select({
        stockCount: sql<number>`COUNT(*)`,
      })
      .from(watchlistItems)
      .where(eq(watchlistItems.watchlistId, id))
      .get();

    return {
      ...watchlist,
      stockCount: result?.stockCount ?? 0,
    };
  }

  listWatchlistSummaries(): WatchlistSummary[] {
    const allWatchlists = this.listWatchlists();
    return allWatchlists.map((watchlist) => {
      const summary = this.getWatchlistSummary(watchlist.id);
      if (!summary) {
        throw new WatchlistError(`Failed to get summary for watchlist ${watchlist.id}`);
      }
      return summary;
    });
  }

  close(): void {
    try {
      this.sqlite.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch {
      // Ignore checkpoint errors on close
    }
    this.sqlite.close();
    if (this.debug) {
      console.log('[DrizzleWatchlistDatabase] Database connection closed');
    }
  }

  private mapWatchlistRow(row: typeof watchlists.$inferSelect): Watchlist {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      createdAt: row.createdAt ? new Date(row.createdAt) : new Date(),
      updatedAt: row.updatedAt ? new Date(row.updatedAt) : new Date(),
    };
  }

  private mapWatchlistItemRow(row: typeof watchlistItems.$inferSelect): WatchlistItem {
    return {
      id: row.id,
      watchlistId: row.watchlistId,
      code: row.code,
      companyName: row.companyName,
      memo: row.memo ?? undefined,
      createdAt: row.createdAt ? new Date(row.createdAt) : new Date(),
    };
  }
}
