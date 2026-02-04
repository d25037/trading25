/**
 * Common Stock Data Schema
 *
 * Shared daily OHLCV table definition for both market.db and dataset.db
 * Uses snake_case column naming convention
 */

import { sql } from 'drizzle-orm';
import { index, integer, primaryKey, real, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../../columns/stock-code';
import { stocks } from './stocks';

/**
 * Stock data table - daily OHLCV data
 * Uses custom stockCode column type for automatic 4-digit normalization
 */
export const stockData = sqliteTable(
  'stock_data',
  {
    code: stockCode('code')
      .notNull()
      .references(() => stocks.code),
    date: text('date').notNull(),
    open: real('open').notNull(),
    high: real('high').notNull(),
    low: real('low').notNull(),
    close: real('close').notNull(),
    volume: integer('volume').notNull(),
    adjustmentFactor: real('adjustment_factor'),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [
    primaryKey({ columns: [table.code, table.date] }),
    index('idx_stock_data_date').on(table.date),
    index('idx_stock_data_code').on(table.code),
  ]
);

/**
 * Type inference helpers
 */
export type StockDataRow = typeof stockData.$inferSelect;
export type StockDataInsert = typeof stockData.$inferInsert;
