/**
 * Dataset-specific Info Schema
 *
 * Metadata and configuration for the dataset
 */

import { sql } from 'drizzle-orm';
import { sqliteTable, text } from 'drizzle-orm/sqlite-core';

/**
 * Dataset info table - metadata and configuration
 */
export const datasetInfo = sqliteTable('dataset_info', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Type inference helpers
 */
export type DatasetInfoRow = typeof datasetInfo.$inferSelect;
export type DatasetInfoInsert = typeof datasetInfo.$inferInsert;
