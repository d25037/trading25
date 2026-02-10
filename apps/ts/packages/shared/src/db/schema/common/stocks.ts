/**
 * Common Stocks Schema
 *
 * Shared stock master table definition for both market.db and dataset.db
 * Uses snake_case column naming convention
 */

import { sql } from 'drizzle-orm';
import { index, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../../columns/stock-code';

/**
 * Stocks table - company master information
 * Uses custom stockCode column type for automatic 4-digit normalization
 */
export const stocks = sqliteTable(
  'stocks',
  {
    code: stockCode('code').primaryKey(),
    companyName: text('company_name').notNull(),
    companyNameEnglish: text('company_name_english'),
    marketCode: text('market_code').notNull(),
    marketName: text('market_name').notNull(),
    sector17Code: text('sector_17_code').notNull(),
    sector17Name: text('sector_17_name').notNull(),
    sector33Code: text('sector_33_code').notNull(),
    sector33Name: text('sector_33_name').notNull(),
    scaleCategory: text('scale_category'),
    listedDate: text('listed_date').notNull(),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
    updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [index('idx_stocks_market').on(table.marketCode), index('idx_stocks_sector').on(table.sector33Code)]
);

/**
 * Type inference helpers
 */
export type StockRow = typeof stocks.$inferSelect;
export type StockInsert = typeof stocks.$inferInsert;
