/**
 * Portfolio Database Drizzle Schema
 *
 * This schema defines the structure for portfolio management:
 * - portfolios: Collection of stock portfolios
 * - portfolio_items: Individual stock holdings within portfolios
 * - portfolio_metadata: Key-value storage for schema versioning
 */

import { sql } from 'drizzle-orm';
import { index, integer, real, sqliteTable, text, unique } from 'drizzle-orm/sqlite-core';
import { stockCode } from '../columns/stock-code';

/**
 * Schema version for portfolio database
 */
export const PORTFOLIO_SCHEMA_VERSION = '1.1.0';

/**
 * Portfolio metadata table for schema versioning and configuration
 */
export const portfolioMetadata = sqliteTable('portfolio_metadata', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Portfolios table - collection of stock portfolios
 */
export const portfolios = sqliteTable('portfolios', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  name: text('name').notNull().unique(),
  description: text('description'),
  createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Portfolio items table - individual stock holdings
 * Uses custom stockCode column type for automatic 4-digit normalization
 */
export const portfolioItems = sqliteTable(
  'portfolio_items',
  {
    id: integer('id').primaryKey({ autoIncrement: true }),
    portfolioId: integer('portfolio_id')
      .notNull()
      .references(() => portfolios.id, { onDelete: 'cascade' }),
    code: stockCode('code').notNull(),
    companyName: text('company_name').notNull(),
    quantity: integer('quantity').notNull(),
    purchasePrice: real('purchase_price').notNull(),
    purchaseDate: text('purchase_date').notNull(),
    account: text('account'),
    notes: text('notes'),
    createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
    updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
  },
  (table) => [
    unique('portfolio_items_portfolio_code_unique').on(table.portfolioId, table.code),
    index('idx_portfolio_items_portfolio_id').on(table.portfolioId),
    index('idx_portfolio_items_code').on(table.code),
    index('idx_portfolio_items_purchase_date').on(table.purchaseDate),
  ]
);

/**
 * Type inference helpers for Drizzle
 */
export type PortfolioMetadataRow = typeof portfolioMetadata.$inferSelect;
export type PortfolioMetadataInsert = typeof portfolioMetadata.$inferInsert;

export type PortfolioRow = typeof portfolios.$inferSelect;
export type PortfolioInsert = typeof portfolios.$inferInsert;

export type PortfolioItemRow = typeof portfolioItems.$inferSelect;
export type PortfolioItemInsert = typeof portfolioItems.$inferInsert;
