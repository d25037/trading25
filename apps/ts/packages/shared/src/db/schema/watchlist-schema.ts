/**
 * Watchlist Database Drizzle Schema
 *
 * This schema defines the structure for watchlist management:
 * - watchlists: Collection of stock watchlists
 * - watchlist_items: Individual stock entries within watchlists
 *
 * Stored in portfolio.db alongside portfolio tables.
 */

import { sql } from 'drizzle-orm';
import { index, integer, sqliteTable, text, unique } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../columns/stock-code';

/**
 * Schema version for watchlist tables
 */
export const WATCHLIST_SCHEMA_VERSION = '1.0.0';

/**
 * Watchlists table - collection of stock watchlists
 */
export const watchlists = sqliteTable('watchlists', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  name: text('name').notNull().unique(),
  description: text('description'),
  createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Watchlist items table - individual stock entries
 * Uses custom stockCode column type for automatic 4-digit normalization
 */
export const watchlistItems = sqliteTable(
  'watchlist_items',
  {
    id: integer('id').primaryKey({ autoIncrement: true }),
    watchlistId: integer('watchlist_id')
      .notNull()
      .references(() => watchlists.id, { onDelete: 'cascade' }),
    code: stockCode('code').notNull(),
    companyName: text('company_name').notNull(),
    memo: text('memo'),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [
    unique('watchlist_items_watchlist_code_unique').on(table.watchlistId, table.code),
    index('idx_watchlist_items_watchlist_id').on(table.watchlistId),
    index('idx_watchlist_items_code').on(table.code),
  ]
);

/**
 * Type inference helpers for Drizzle
 */
export type WatchlistRow = typeof watchlists.$inferSelect;
export type WatchlistInsert = typeof watchlists.$inferInsert;

export type WatchlistItemRow = typeof watchlistItems.$inferSelect;
export type WatchlistItemInsert = typeof watchlistItems.$inferInsert;
