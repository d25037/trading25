/**
 * Market-specific Sync Metadata Schema
 *
 * Key-value storage for tracking sync state
 */

import { sql } from 'drizzle-orm';
import { sqliteTable, text } from 'drizzle-orm/sqlite-core';

/**
 * Sync metadata table for tracking sync state
 */
export const syncMetadata = sqliteTable('sync_metadata', {
  key: text('key').primaryKey(),
  value: text('value').notNull(),
  updatedAt: text('updated_at').default(sql`CURRENT_TIMESTAMP`),
});

/**
 * Type inference helpers
 */
export type SyncMetadataRow = typeof syncMetadata.$inferSelect;
export type SyncMetadataInsert = typeof syncMetadata.$inferInsert;
