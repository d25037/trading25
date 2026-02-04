/**
 * Market-specific Index Master Schema
 *
 * Contains metadata for all supported indices (TOPIX, sector, market, etc.)
 */

import { sql } from 'drizzle-orm';
import { sqliteTable, text } from 'drizzle-orm/sqlite-core';

/**
 * Index category types
 */
export const INDEX_CATEGORIES = {
  TOPIX: 'topix', // TOPIX and size indices (0000, 0028-002E)
  SECTOR33: 'sector33', // 33 sector indices (0040-0060)
  SECTOR17: 'sector17', // TOPIX-17 sector indices (0080-0090)
  MARKET: 'market', // Market indices (0500-0502)
  GROWTH: 'growth', // Growth market indices (0070)
  REIT: 'reit', // REIT indices (0075, 8501-8503)
  STYLE: 'style', // Style indices (8100, 8200, 812C, 822C, 812D, 822D)
} as const;

export type IndexCategory = (typeof INDEX_CATEGORIES)[keyof typeof INDEX_CATEGORIES];

/**
 * Index master table - index definitions
 * Contains metadata for all supported indices (TOPIX, sector, market, etc.)
 */
export const indexMaster = sqliteTable('index_master', {
  code: text('code').primaryKey(), // e.g., '0000', '0028', '0040'
  name: text('name').notNull(), // e.g., 'TOPIX', 'TOPIX Core30'
  nameEnglish: text('name_english'), // English name if available
  category: text('category').notNull(), // 'topix', 'sector33', 'sector17', 'market', 'growth', 'reit'
  dataStartDate: text('data_start_date'), // When data becomes available (e.g., '2008-05-07')
  createdAt: text('created_at').default(sql`CURRENT_TIMESTAMP`),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Type inference helpers
 */
export type IndexMasterRow = typeof indexMaster.$inferSelect;
export type IndexMasterInsert = typeof indexMaster.$inferInsert;
