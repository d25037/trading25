/**
 * Common Indices Data Schema
 *
 * Shared indices table definition for both market.db and dataset.db
 * Uses snake_case column naming convention
 *
 * Note: JQuants API does not provide volume for indices data
 */

import { sql } from 'drizzle-orm';
import { index, primaryKey, real, sqliteTable, text } from 'drizzle-orm/sqlite-core';

/**
 * Indices data table - daily OHLC data for all indices
 * Stores data for TOPIX variants, sector indices, market indices, etc.
 *
 * For market.db: references index_master table
 * For dataset.db: includes sector_name inline
 */
export const indicesData = sqliteTable(
  'indices_data',
  {
    code: text('code').notNull(),
    date: text('date').notNull(),
    open: real('open'),
    high: real('high'),
    low: real('low'),
    close: real('close'),
    // Sector name is stored inline for dataset.db compatibility
    // market.db can join with index_master for this info
    sectorName: text('sector_name'),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [
    primaryKey({ columns: [table.code, table.date] }),
    index('idx_indices_data_date').on(table.date),
    index('idx_indices_data_code').on(table.code),
  ]
);

/**
 * Type inference helpers
 */
export type IndicesDataRow = typeof indicesData.$inferSelect;
export type IndicesDataInsert = typeof indicesData.$inferInsert;
