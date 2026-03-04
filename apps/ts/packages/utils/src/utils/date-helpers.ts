/**
 * Date utility functions for type-safe date-to-string conversion
 * Eliminates non-null assertions by handling the split operation safely
 */

/**
 * Convert a Date to ISO date string (YYYY-MM-DD format)
 *
 * @param date - The date to convert
 * @returns ISO date string in YYYY-MM-DD format
 * @throws Error if the date is invalid or conversion fails
 *
 * @example
 * ```typescript
 * const dateStr = toISODateString(new Date('2024-01-15'));
 * console.log(dateStr); // "2024-01-15"
 * ```
 */
export function toISODateString(date: Date): string {
  // Check if date is valid before attempting conversion
  if (Number.isNaN(date.getTime())) {
    throw new Error('Invalid date: Date object represents an invalid date');
  }

  const isoString = date.toISOString();
  const parts = isoString.split('T');

  if (!parts[0]) {
    throw new Error(`Invalid date: failed to extract date string from ISO string "${isoString}"`);
  }

  return parts[0];
}

/**
 * Convert a Date to ISO date string (YYYY-MM-DD format)
 * Returns default value if date is null/undefined
 *
 * @param date - The date to convert (can be null or undefined)
 * @param defaultValue - Value to return if date is null/undefined (default: empty string)
 * @returns ISO date string or default value
 * @throws Error if the date is invalid (but not for null/undefined)
 *
 * @example
 * ```typescript
 * const dateStr = toISODateStringOrDefault(maybeDate, 'N/A');
 * console.log(dateStr); // "2024-01-15" or "N/A"
 * ```
 */
export function toISODateStringOrDefault(date: Date | null | undefined, defaultValue = ''): string {
  if (!date) return defaultValue;
  return toISODateString(date);
}

/**
 * Convert a Date to ISO date string (YYYY-MM-DD format)
 * Returns null if date is null/undefined
 *
 * @param date - The date to convert (can be null or undefined)
 * @returns ISO date string or null
 * @throws Error if the date is invalid (but not for null/undefined)
 *
 * @example
 * ```typescript
 * const dateStr = toISODateStringOrNull(maybeDate);
 * console.log(dateStr); // "2024-01-15" or null
 * ```
 */
export function toISODateStringOrNull(date: Date | null | undefined): string | null {
  if (!date) return null;
  return toISODateString(date);
}

/**
 * Date range interface for conversion
 */
interface DateRange {
  from: Date;
  to: Date;
}

/**
 * Convert a DateRange to ISO date string tuple for SQL parameters
 * Validates both dates and ensures from <= to
 *
 * @param range - The date range to convert
 * @returns Object with from and to ISO date strings
 * @throws Error if dates are invalid or from > to
 *
 * @example
 * ```typescript
 * const { from, to } = dateRangeToISO({
 *   from: new Date('2024-01-01'),
 *   to: new Date('2024-12-31')
 * });
 * console.log(from, to); // "2024-01-01" "2024-12-31"
 * ```
 */
export function dateRangeToISO(range: DateRange): { from: string; to: string } {
  const from = toISODateString(range.from);
  const to = toISODateString(range.to);

  if (from > to) {
    throw new Error(`Invalid date range: from (${from}) > to (${to})`);
  }

  return { from, to };
}
