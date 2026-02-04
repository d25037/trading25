/**
 * Dataset-specific Margin Data Schema
 *
 * Margin trading volumes for stocks
 */

import { index, primaryKey, real, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../../columns/stock-code';
import { stocks } from '../common/stocks';

/**
 * Margin data table - margin trading volumes
 * Uses composite primary key (code, date) instead of autoincrement id
 */
export const marginData = sqliteTable(
  'margin_data',
  {
    code: stockCode('code')
      .notNull()
      .references(() => stocks.code, { onDelete: 'cascade' }),
    date: text('date').notNull(),
    longMarginVolume: real('long_margin_volume'),
    shortMarginVolume: real('short_margin_volume'),
  },
  (table) => [
    primaryKey({ columns: [table.code, table.date] }),
    index('idx_margin_data_date').on(table.date),
    index('idx_margin_data_code').on(table.code),
  ]
);

/**
 * Type inference helpers
 */
export type MarginDataRow = typeof marginData.$inferSelect;
export type MarginDataInsert = typeof marginData.$inferInsert;
