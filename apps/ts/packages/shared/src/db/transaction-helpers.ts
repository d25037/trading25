/**
 * Transaction Helpers
 * Common transaction patterns for database operations
 */

import type { Database } from 'bun:sqlite';

/**
 * Execute a transaction with debug error logging
 */
export function executeTransaction<T>(
  sqlite: Database,
  operation: () => T,
  options?: {
    debug?: boolean;
    operationName?: string;
  }
): T {
  const { debug = false, operationName = 'operation' } = options ?? {};

  try {
    return sqlite.transaction(operation)();
  } catch (error) {
    if (debug) {
      console.error(`[DrizzleMarketDatabase] ${operationName} transaction failed:`, error);
    }
    throw error;
  }
}

/**
 * Execute bulk insert with transaction and enhanced error context
 */
export function executeBulkInsert<T>(
  sqlite: Database,
  items: T[],
  insertFn: (item: T) => void,
  options?: {
    debug?: boolean;
    operationName?: string;
  }
): void {
  const { debug = false, operationName = 'bulkInsert' } = options ?? {};

  executeTransaction(
    sqlite,
    () => {
      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        if (item === undefined) continue;
        try {
          insertFn(item);
        } catch (error) {
          if (debug) {
            console.error(`[DrizzleMarketDatabase] ${operationName} failed at item ${i + 1}/${items.length}:`, error);
          }
          throw error;
        }
      }
    },
    { debug, operationName }
  );
}
