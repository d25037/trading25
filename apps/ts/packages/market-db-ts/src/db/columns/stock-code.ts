/**
 * Custom Drizzle column type for stock codes
 *
 * JQuants API returns 5-digit codes (e.g., "72030")
 * This module normalizes them to 4-digit format (e.g., "7203")
 * for consistent storage and user-friendly display.
 */

import { customType } from 'drizzle-orm/sqlite-core';

/**
 * Regular expression for validating 4-digit stock codes
 * Supports formats like: 7203, 285A, 1301
 */
const STOCK_CODE_4_DIGIT_REGEX = /^\d[0-9A-Z]\d[0-9A-Z]$/;

/**
 * Normalize stock code from 5-digit to 4-digit format
 *
 * JQuants uses 5-digit codes with a trailing '0' (e.g., "72030" for Toyota)
 * This function converts them to the standard 4-digit format (e.g., "7203")
 *
 * @param code - Stock code (4 or 5 digits)
 * @returns Normalized 4-digit stock code
 *
 * @example
 * normalizeStockCode("72030") // => "7203"
 * normalizeStockCode("7203")  // => "7203"
 * normalizeStockCode("285A0") // => "285A"
 */
export function normalizeStockCode(code: string): string {
  if (code.length === 5 && code.endsWith('0')) {
    return code.slice(0, 4);
  }
  return code;
}

/**
 * Expand stock code from 4-digit to 5-digit format for JQuants API compatibility
 *
 * @param code - Stock code (4 digits)
 * @returns Expanded 5-digit stock code
 *
 * @example
 * expandStockCode("7203")  // => "72030"
 * expandStockCode("285A")  // => "285A0"
 * expandStockCode("72030") // => "72030" (already 5 digits)
 */
export function expandStockCode(code: string): string {
  if (code.length === 4) {
    return `${code}0`;
  }
  return code;
}

/**
 * Validate if a string is a valid 4-digit stock code
 *
 * @param code - Stock code to validate
 * @returns true if valid 4-digit format
 */
export function isValidStockCode(code: string): boolean {
  return STOCK_CODE_4_DIGIT_REGEX.test(code);
}

/**
 * Custom Drizzle column type for stock codes
 *
 * Automatically normalizes 5-digit codes to 4-digit on both read and write.
 * This ensures all stock codes in the database are stored in 4-digit format.
 *
 * @example
 * ```typescript
 * import { sqliteTable, text } from 'drizzle-orm/sqlite-core';
 * import { stockCode } from '../db/columns/stock-code';
 *
 * export const stocks = sqliteTable('stocks', {
 *   code: stockCode('code').primaryKey(),
 *   companyName: text('company_name').notNull(),
 * });
 * ```
 */
export const stockCode = customType<{
  data: string;
  driverData: string;
}>({
  dataType() {
    return 'text';
  },
  fromDriver(value: string): string {
    // When reading from DB, normalize to 4-digit
    return normalizeStockCode(value);
  },
  toDriver(value: string): string {
    // When writing to DB, normalize to 4-digit
    return normalizeStockCode(value);
  },
});
