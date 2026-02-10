/**
 * Common TOPIX Data Schema
 *
 * Shared TOPIX index table definition for both market.db and dataset.db
 * Uses snake_case column naming convention
 *
 * Note: JQuants API does not provide volume for TOPIX data
 */

import { sql } from 'drizzle-orm';
import { index, real, sqliteTable, text } from 'drizzle-orm/sqlite-core';

/**
 * TOPIX data table - index daily OHLC data
 * Used as trading calendar reference
 */
export const topixData = sqliteTable(
  'topix_data',
  {
    date: text('date').primaryKey(),
    open: real('open').notNull(),
    high: real('high').notNull(),
    low: real('low').notNull(),
    close: real('close').notNull(),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [index('idx_topix_date').on(table.date)]
);

/**
 * Type inference helpers
 */
export type TopixDataRow = typeof topixData.$inferSelect;
export type TopixDataInsert = typeof topixData.$inferInsert;
