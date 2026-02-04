/**
 * Database query result helpers for type-safe access
 * These utilities help avoid non-null assertions when working with database query results
 */

/**
 * Safely get the first result from a query result array
 */
export function getFirstQueryResult<T>(results: T[], errorMessage?: string): T {
  if (results.length === 0) {
    throw new Error(errorMessage || 'Expected at least one result from query');
  }
  const result = results[0];
  if (result === undefined) {
    throw new Error(errorMessage || 'First query result is undefined');
  }
  return result;
}

/**
 * Assert that query results have expected structure
 */
export function assertQueryResults<T>(
  results: T[],
  expectedCount: number,
  errorMessage?: string
): T[] & { length: typeof expectedCount } {
  if (results.length !== expectedCount) {
    throw new Error(errorMessage || `Expected ${expectedCount} query results, got ${results.length}`);
  }
  return results as T[] & { length: typeof expectedCount };
}

/**
 * Safely extract database statistics from multiple query results
 */
export function extractDatabaseStats<TStats, TMargin, TDates>(
  totalStats: TStats[],
  marginStats: TMargin[],
  dateRange: TDates[],
  context = 'database statistics'
): {
  stats: TStats;
  margin: TMargin;
  dates: TDates;
} {
  return {
    stats: getFirstQueryResult(totalStats, `Expected ${context} total stats`),
    margin: getFirstQueryResult(marginStats, `Expected ${context} margin stats`),
    dates: getFirstQueryResult(dateRange, `Expected ${context} date range`),
  };
}

/**
 * Type guard for non-null database connection
 */
export function assertDatabaseConnection<T>(
  connection: T | null | undefined,
  errorMessage = 'Database connection not initialized'
): asserts connection is NonNullable<T> {
  if (!connection) {
    throw new Error(errorMessage);
  }
}
