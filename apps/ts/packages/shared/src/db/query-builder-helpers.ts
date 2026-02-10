/**
 * Query Builder Helpers
 * Common patterns for building SQL queries
 */

import type { RankingItem } from './drizzle-market-reader';

/**
 * Valid market codes for filtering
 */
export const VALID_MARKET_CODES = ['prime', 'standard', 'growth'] as const;

/**
 * Build market code filter clause
 *
 * @param marketCodes - Array of market codes to filter by (must be valid codes)
 * @throws Error if any invalid market code is provided
 */
export function buildMarketCodeFilter(marketCodes: string[] | undefined): {
  clause: string;
  params: string[];
} {
  if (!marketCodes || marketCodes.length === 0) {
    return { clause: '', params: [] };
  }

  // Validate market codes
  const invalidCodes = marketCodes.filter(
    (code) => !VALID_MARKET_CODES.includes(code as (typeof VALID_MARKET_CODES)[number])
  );
  if (invalidCodes.length > 0) {
    throw new Error(`Invalid market codes: ${invalidCodes.join(', ')}. Valid codes: ${VALID_MARKET_CODES.join(', ')}`);
  }

  const placeholders = marketCodes.map(() => '?').join(',');
  return {
    clause: ` AND s.market_code IN (${placeholders})`,
    params: marketCodes,
  };
}

/**
 * Build ORDER BY + LIMIT clause for ranking queries
 */
export function buildOrderLimitClause(orderColumn: string, order: 'ASC' | 'DESC' = 'DESC'): string {
  return ` ORDER BY ${orderColumn} ${order} LIMIT ?`;
}

/**
 * Map raw SQL row to RankingItem
 */
export function mapToRankingItem(
  row: {
    code: string;
    company_name: string;
    market_code: string;
    sector33_name: string;
    current_price: number;
    volume: number;
    trading_value?: number;
    avg_trading_value?: number;
    previous_price?: number;
    base_price?: number;
    change_amount?: number;
    change_percentage?: number;
  },
  index: number,
  lookbackDays?: number
): RankingItem {
  return {
    rank: index + 1,
    code: row.code,
    companyName: row.company_name,
    marketCode: row.market_code,
    sector33Name: row.sector33_name,
    currentPrice: row.current_price,
    volume: row.volume,
    tradingValue: row.trading_value,
    tradingValueAverage: row.avg_trading_value,
    previousPrice: row.previous_price,
    basePrice: row.base_price,
    changeAmount: row.change_amount,
    changePercentage: row.change_percentage,
    lookbackDays,
  };
}

/**
 * Base stock select columns for ranking queries
 */
export const RANKING_BASE_COLUMNS = `
  s.code,
  s.company_name,
  s.market_code,
  s.sector33_name
`;
