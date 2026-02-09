/**
 * Drizzle-based Market Data Reader
 *
 * Type-safe read-only market data access using Drizzle ORM.
 * Replaces the raw SQL implementation in market-sync/reader.ts
 */

import { Database } from 'bun:sqlite';
import { and, eq, gte, lt, lte, sql } from 'drizzle-orm';
import type { BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';
import { drizzle } from 'drizzle-orm/bun-sqlite';
import type { DateRange, StockData, StockInfo, TopixData } from '../dataset/types';
import { dateRangeToISO, toISODateString } from '../utils/date-helpers';
import { normalizeStockCode } from './columns/stock-code';
import { buildMarketCodeFilter, mapToRankingItem, RANKING_BASE_COLUMNS } from './query-builder-helpers';
import { stockData, stocks, topixData } from './schema/market-schema';

/**
 * Ranking item type
 */
export interface RankingItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  currentPrice: number;
  volume: number;
  tradingValue?: number;
  previousPrice?: number;
  changeAmount?: number;
  changePercentage?: number;
  lookbackDays?: number;
  basePrice?: number;
  tradingValueAverage?: number;
}

/**
 * Stock search result item
 */
export interface StockSearchResult {
  code: string;
  companyName: string;
  companyNameEnglish: string | null;
  marketCode: string;
  marketName: string;
  sector33Name: string;
}

/**
 * Drizzle-based Market Data Reader
 * Provides read-only access to market-wide stock data
 */
export class DrizzleMarketDataReader {
  private sqlite: Database;
  private db: BunSQLiteDatabase;

  constructor(databasePath: string) {
    this.sqlite = new Database(databasePath, { readonly: true });
    this.db = drizzle(this.sqlite);
  }

  getStockByCode(code: string): StockInfo | null {
    const normalizedCode = normalizeStockCode(code);
    const result = this.db.select().from(stocks).where(eq(stocks.code, normalizedCode)).get();

    if (!result) return null;

    return {
      code: result.code,
      companyName: result.companyName,
      companyNameEnglish: result.companyNameEnglish || '',
      marketCode: result.marketCode,
      marketName: result.marketName,
      sector17Code: result.sector17Code,
      sector17Name: result.sector17Name,
      sector33Code: result.sector33Code,
      sector33Name: result.sector33Name,
      scaleCategory: result.scaleCategory || '',
      listedDate: new Date(result.listedDate),
    };
  }

  getStockList(marketCodes?: string[]): StockInfo[] {
    let query = this.db.select().from(stocks);

    if (marketCodes && marketCodes.length > 0) {
      query = query.where(
        sql`market_code IN (${sql.join(
          marketCodes.map((c) => sql`${c}`),
          sql`, `
        )})`
      ) as typeof query;
    }

    const results = query.orderBy(stocks.code).all();

    return results.map((row) => ({
      code: row.code,
      companyName: row.companyName,
      companyNameEnglish: row.companyNameEnglish || '',
      marketCode: row.marketCode,
      marketName: row.marketName,
      sector17Code: row.sector17Code,
      sector17Name: row.sector17Name,
      sector33Code: row.sector33Code,
      sector33Name: row.sector33Name,
      scaleCategory: row.scaleCategory || '',
      listedDate: new Date(row.listedDate),
    }));
  }

  getStockData(stockCode: string, dateRange?: DateRange): StockData[] {
    const normalizedCode = normalizeStockCode(stockCode);

    // Build where conditions
    const conditions = [eq(stockData.code, normalizedCode)];
    if (dateRange) {
      const { from, to } = dateRangeToISO(dateRange);
      conditions.push(gte(stockData.date, from), lte(stockData.date, to));
    }

    const results = this.db
      .select()
      .from(stockData)
      .where(and(...conditions))
      .orderBy(stockData.date)
      .all();

    return results.map((row) => ({
      code: row.code,
      date: new Date(row.date),
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
      volume: row.volume,
      adjustmentFactor: row.adjustmentFactor ?? undefined,
    }));
  }

  getDatasetInfo(): {
    totalStocks: number;
    totalQuotes: number;
    markets: string[];
    dateRange: DateRange;
    databaseSize: string;
  } {
    const stocksResult = this.db.select({ count: sql<number>`COUNT(*)` }).from(stocks).get();
    const totalStocks = stocksResult?.count ?? 0;

    const quotesResult = this.db.select({ count: sql<number>`COUNT(*)` }).from(stockData).get();
    const totalQuotes = quotesResult?.count ?? 0;

    const marketsResult = this.db
      .selectDistinct({ marketCode: stocks.marketCode })
      .from(stocks)
      .orderBy(stocks.marketCode)
      .all();
    const markets = marketsResult.map((r) => r.marketCode);

    const dateRangeResult = this.db
      .select({
        min: sql<string>`MIN(date)`,
        max: sql<string>`MAX(date)`,
      })
      .from(stockData)
      .get();

    const range: DateRange = {
      from: new Date(dateRangeResult?.min ?? ''),
      to: new Date(dateRangeResult?.max ?? ''),
    };

    // Get database size using raw SQL (pragma not available in Drizzle)
    const sizeRow = this.sqlite
      .query('SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()')
      .get() as { size: number } | undefined;
    const sizeInMB = sizeRow ? (sizeRow.size / (1024 * 1024)).toFixed(1) : 'unknown';

    return {
      totalStocks,
      totalQuotes,
      markets,
      dateRange: range,
      databaseSize: `${sizeInMB} MB`,
    };
  }

  testConnection(): boolean {
    try {
      this.sqlite.query('SELECT 1').get();
      return true;
    } catch {
      return false;
    }
  }

  getLatestTradingDate(): Date | null {
    const result = this.db.select({ maxDate: sql<string | null>`MAX(date)` }).from(stockData).get();
    return result?.maxDate ? new Date(result.maxDate) : null;
  }

  getPreviousTradingDate(date: Date): Date | null {
    const dateStr = toISODateString(date);
    const result = this.db
      .select({ prevDate: sql<string | null>`MAX(date)` })
      .from(stockData)
      .where(lt(stockData.date, dateStr))
      .get();
    return result?.prevDate ? new Date(result.prevDate) : null;
  }

  getTradingDateBefore(date: Date, days: number): Date | null {
    if (days < 1) {
      throw new Error('Days must be at least 1');
    }

    const dateStr = toISODateString(date);
    const result = this.sqlite
      .query('SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?')
      .get(dateStr, days - 1) as { date: string } | undefined;
    return result?.date ? new Date(result.date) : null;
  }

  getRankingByTradingValueAverage(
    date: Date,
    lookbackDays: number,
    limit: number,
    marketCodes?: string[]
  ): RankingItem[] {
    if (lookbackDays < 1) {
      throw new Error('Lookback days must be at least 1');
    }

    const startDate = this.getTradingDateBefore(date, lookbackDays - 1);
    if (!startDate) return [];

    const dateStr = toISODateString(date);
    const startDateStr = toISODateString(startDate);
    const marketFilter = buildMarketCodeFilter(marketCodes);

    const sqlQuery = `
      SELECT ${RANKING_BASE_COLUMNS},
        MAX(sd.close) as current_price,
        SUM(sd.volume) as volume,
        AVG(sd.close * sd.volume) as avg_trading_value
      FROM stock_data sd
      JOIN stocks s ON s.code = sd.code
      WHERE sd.date >= ? AND sd.date <= ?${marketFilter.clause}
      GROUP BY s.code, s.company_name, s.market_code, s.sector33_name
      ORDER BY avg_trading_value DESC LIMIT ?
    `;

    const params: (string | number)[] = [startDateStr, dateStr, ...marketFilter.params, limit];

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      avg_trading_value: number;
    }>;

    return rows.map((row, index) => mapToRankingItem(row, index, lookbackDays));
  }

  getRankingByTradingValue(date: Date, limit: number, marketCodes?: string[]): RankingItem[] {
    const dateStr = toISODateString(date);
    const marketFilter = buildMarketCodeFilter(marketCodes);

    const sqlQuery = `
      SELECT ${RANKING_BASE_COLUMNS},
        sd.close as current_price,
        sd.volume,
        sd.close * sd.volume as trading_value
      FROM stock_data sd
      JOIN stocks s ON s.code = sd.code
      WHERE sd.date = ?${marketFilter.clause}
      ORDER BY trading_value DESC LIMIT ?
    `;

    const params: (string | number)[] = [dateStr, ...marketFilter.params, limit];

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      trading_value: number;
    }>;

    return rows.map((row, index) => mapToRankingItem(row, index));
  }

  getRankingByPriceChange(
    date: Date,
    limit: number,
    marketCodes?: string[],
    order: 'gainers' | 'losers' = 'gainers'
  ): RankingItem[] {
    const dateStr = toISODateString(date);
    const prevDate = this.getPreviousTradingDate(date);

    if (!prevDate) return [];

    const prevDateStr = toISODateString(prevDate);
    const marketFilter = buildMarketCodeFilter(marketCodes);
    const orderDir = order === 'gainers' ? 'DESC' : 'ASC';

    const sqlQuery = `
      SELECT ${RANKING_BASE_COLUMNS},
        curr.close as current_price,
        curr.volume,
        prev.close as previous_price,
        (curr.close - prev.close) as change_amount,
        ((curr.close - prev.close) / prev.close * 100) as change_percentage
      FROM stock_data curr
      JOIN stock_data prev ON curr.code = prev.code
      JOIN stocks s ON s.code = curr.code
      WHERE curr.date = ?
        AND prev.date = ?
        AND prev.close > 0
        AND curr.close > 0
        AND curr.close != prev.close${marketFilter.clause}
      ORDER BY change_percentage ${orderDir} LIMIT ?
    `;

    const params: (string | number)[] = [dateStr, prevDateStr, ...marketFilter.params, limit];

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      previous_price: number;
      change_amount: number;
      change_percentage: number;
    }>;

    return rows.map((row, index) => mapToRankingItem(row, index));
  }

  getRankingByPriceChangeFromDays(
    date: Date,
    lookbackDays: number,
    limit: number,
    marketCodes?: string[],
    order: 'gainers' | 'losers' = 'gainers'
  ): RankingItem[] {
    if (lookbackDays < 1) {
      throw new Error('Lookback days must be at least 1');
    }

    const dateStr = toISODateString(date);
    const baseDate = this.getTradingDateBefore(date, lookbackDays);

    if (!baseDate) return [];

    const baseDateStr = toISODateString(baseDate);
    const marketFilter = buildMarketCodeFilter(marketCodes);
    const orderDir = order === 'gainers' ? 'DESC' : 'ASC';

    const sqlQuery = `
      SELECT ${RANKING_BASE_COLUMNS},
        curr.close as current_price,
        curr.volume,
        base.close as base_price,
        (curr.close - base.close) as change_amount,
        ((curr.close - base.close) / base.close * 100) as change_percentage
      FROM stock_data curr
      JOIN stock_data base ON curr.code = base.code
      JOIN stocks s ON s.code = curr.code
      WHERE curr.date = ?
        AND base.date = ?
        AND base.close > 0
        AND curr.close > 0
        AND curr.close != base.close${marketFilter.clause}
      ORDER BY change_percentage ${orderDir} LIMIT ?
    `;

    const params: (string | number)[] = [dateStr, baseDateStr, ...marketFilter.params, limit];

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      base_price: number;
      change_amount: number;
      change_percentage: number;
    }>;

    return rows.map((row, index) => mapToRankingItem(row, index, lookbackDays));
  }

  getPriceAtDate(code: string, date: Date): { date: Date; close: number } | null {
    const normalizedCode = normalizeStockCode(code);
    const dateStr = toISODateString(date);
    const result = this.sqlite
      .query('SELECT date, close FROM stock_data WHERE code = ? AND date <= ? ORDER BY date DESC LIMIT 1')
      .get(normalizedCode, dateStr) as { date: string; close: number } | null;
    return result ? { date: new Date(result.date), close: result.close } : null;
  }

  getPricesAtDates(code: string, dates: Date[]): Map<string, number> {
    const result = new Map<string, number>();
    if (dates.length === 0) return result;

    const normalizedCode = normalizeStockCode(code);
    const sortedDates = [...dates].sort((a, b) => a.getTime() - b.getTime());
    const minDate = sortedDates[0];
    const maxDate = sortedDates[sortedDates.length - 1];
    if (!minDate || !maxDate) return result;

    const rows = this.sqlite
      .query('SELECT date, close FROM stock_data WHERE code = ? AND date >= ? AND date <= ? ORDER BY date')
      .all(normalizedCode, toISODateString(minDate), toISODateString(maxDate)) as Array<{
      date: string;
      close: number;
    }>;

    const availableDates = rows.map((r) => r.date);
    const priceByDate = new Map(rows.map((r) => [r.date, r.close]));

    for (const date of dates) {
      const dateStr = toISODateString(date);
      const price = priceByDate.get(dateStr) ?? this.findClosestPriorPrice(dateStr, availableDates, priceByDate);
      if (price !== null) result.set(dateStr, price);
    }

    return result;
  }

  private findClosestPriorPrice(
    targetDate: string,
    sortedDates: string[],
    priceByDate: Map<string, number>
  ): number | null {
    let left = 0;
    let right = sortedDates.length - 1;
    let closestDate: string | null = null;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      const midDate = sortedDates[mid];
      if (midDate === undefined) break;

      if (midDate <= targetDate) {
        closestDate = midDate;
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }

    return closestDate ? (priceByDate.get(closestDate) ?? null) : null;
  }

  /**
   * Get TOPIX data with optional date range filter
   */
  getTopixData(dateRange?: DateRange): TopixData[] {
    const conditions = [];
    if (dateRange) {
      const { from, to } = dateRangeToISO(dateRange);
      conditions.push(gte(topixData.date, from), lte(topixData.date, to));
    }

    const query =
      conditions.length > 0
        ? this.db
            .select()
            .from(topixData)
            .where(and(...conditions))
            .orderBy(topixData.date)
        : this.db.select().from(topixData).orderBy(topixData.date);

    const results = query.all();

    return results.map((row) => ({
      date: new Date(row.date),
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
      volume: 0, // TOPIX doesn't have volume data
    }));
  }

  /**
   * Get TOPIX data count
   */
  getTopixDataCount(): number {
    const result = this.db.select({ count: sql<number>`COUNT(*)` }).from(topixData).get();
    return result?.count ?? 0;
  }

  /**
   * Build ORDER BY clause for sector stocks query
   * Uses strict type checking and explicit validation
   */
  private buildSectorStocksOrderClause(
    sortBy: 'tradingValue' | 'changePercentage' | 'code',
    sortOrder: 'asc' | 'desc'
  ): string {
    const orderMap = {
      tradingValue: { asc: 'ORDER BY trading_value ASC', desc: 'ORDER BY trading_value DESC' },
      changePercentage: {
        asc: 'ORDER BY change_percentage ASC NULLS LAST',
        desc: 'ORDER BY change_percentage DESC NULLS LAST',
      },
      code: { asc: 'ORDER BY s.code ASC', desc: 'ORDER BY s.code DESC' },
    } as const;

    // Type-safe access with runtime validation
    const sortByClause = orderMap[sortBy];
    if (!sortByClause) {
      throw new Error(`Invalid sortBy parameter: ${sortBy}`);
    }
    const orderClause = sortByClause[sortOrder];
    if (!orderClause) {
      throw new Error(`Invalid sortOrder parameter: ${sortOrder}`);
    }
    return orderClause;
  }

  /** Valid market codes for sector stocks query (lowercase to match database) */
  private static readonly VALID_MARKET_CODES = ['prime', 'standard', 'growth'] as const;

  /**
   * Build WHERE conditions for sector stocks query
   * Validates marketCodes against allowed values
   */
  private buildSectorStocksConditions(
    dateStr: string,
    sector33Name?: string,
    sector17Name?: string,
    marketCodes?: string[]
  ): { conditions: string[]; params: (string | number)[] } {
    const conditions: string[] = ['curr.date = ?'];
    const params: (string | number)[] = [dateStr];

    if (sector33Name) {
      conditions.push('s.sector33_name = ?');
      params.push(sector33Name);
    }
    if (sector17Name) {
      conditions.push('s.sector17_name = ?');
      params.push(sector17Name);
    }
    if (marketCodes && marketCodes.length > 0) {
      // Validate market codes against whitelist
      const invalidCodes = marketCodes.filter(
        (code) => !DrizzleMarketDataReader.VALID_MARKET_CODES.includes(code as 'prime' | 'standard' | 'growth')
      );
      if (invalidCodes.length > 0) {
        throw new Error(`Invalid market codes: ${invalidCodes.join(', ')}`);
      }
      const placeholders = marketCodes.map(() => '?').join(',');
      conditions.push(`s.market_code IN (${placeholders})`);
      params.push(...marketCodes);
    }
    return { conditions, params };
  }

  /**
   * Get stocks by sector with trading data and optional sorting
   * Trading value is calculated as 15-day average
   */
  getStocksBySector(options: {
    sector33Name?: string;
    sector17Name?: string;
    marketCodes?: string[];
    lookbackDays?: number;
    sortBy?: 'tradingValue' | 'changePercentage' | 'code';
    sortOrder?: 'asc' | 'desc';
    limit?: number;
  }): RankingItem[] {
    const {
      sector33Name,
      sector17Name,
      marketCodes,
      lookbackDays = 5,
      sortBy = 'tradingValue',
      sortOrder = 'desc',
      limit = 100,
    } = options;

    const targetDate = this.getLatestTradingDate();
    if (!targetDate) return [];

    const dateStr = toISODateString(targetDate);
    const baseDate = this.getTradingDateBefore(targetDate, lookbackDays);
    const baseDateStr = baseDate ? toISODateString(baseDate) : null;

    // Get date for 15-day average calculation
    const tradingValueBaseDate = this.getTradingDateBefore(targetDate, 15);
    const tradingValueBaseDateStr = tradingValueBaseDate ? toISODateString(tradingValueBaseDate) : dateStr;

    // Build params array in correct order for SQL:
    // 1. Subquery params (in SELECT clause) come first
    // 2. JOIN params come second
    // 3. WHERE params come last
    const params: (string | number)[] = [];

    // Subquery for 15-day average trading value (params used in SELECT)
    const tradingValueSubquery = `(
      SELECT AVG(sd.close * sd.volume)
      FROM stock_data sd
      WHERE sd.code = curr.code AND sd.date > ? AND sd.date <= ?
    )`;
    params.push(tradingValueBaseDateStr, dateStr);

    // JOIN params for base date (price change calculation)
    const baseJoinClause = baseDateStr ? 'LEFT JOIN stock_data base ON curr.code = base.code AND base.date = ?' : '';
    if (baseDateStr) {
      params.push(baseDateStr);
    }

    // WHERE conditions
    const { conditions, params: whereParams } = this.buildSectorStocksConditions(
      dateStr,
      sector33Name,
      sector17Name,
      marketCodes
    );
    params.push(...whereParams);

    const selectFields = baseDateStr
      ? `s.code, s.company_name, s.market_code, s.sector33_name, curr.close as current_price, curr.volume,
         ${tradingValueSubquery} as trading_value, base.close as base_price,
         CASE WHEN base.close > 0 THEN (curr.close - base.close) ELSE NULL END as change_amount,
         CASE WHEN base.close > 0 THEN ((curr.close - base.close) / base.close * 100) ELSE NULL END as change_percentage`
      : `s.code, s.company_name, s.market_code, s.sector33_name, curr.close as current_price, curr.volume, ${tradingValueSubquery} as trading_value`;

    const orderClause = this.buildSectorStocksOrderClause(sortBy, sortOrder);
    params.push(limit);

    const sqlQuery = `SELECT ${selectFields} FROM stock_data curr JOIN stocks s ON s.code = curr.code ${baseJoinClause} WHERE ${conditions.join(' AND ')} ${orderClause} LIMIT ?`;

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      trading_value: number;
      base_price?: number;
      change_amount?: number;
      change_percentage?: number;
    }>;

    return rows.map((row, index) => ({
      rank: index + 1,
      code: row.code,
      companyName: row.company_name,
      marketCode: row.market_code,
      sector33Name: row.sector33_name,
      currentPrice: row.current_price,
      volume: row.volume,
      tradingValue: row.trading_value,
      tradingValueAverage: row.trading_value,
      basePrice: row.base_price,
      changeAmount: row.change_amount,
      changePercentage: row.change_percentage,
      lookbackDays,
    }));
  }

  /**
   * Search stocks by code or company name (fuzzy search)
   * Searches in: code, company_name, company_name_english
   * Returns up to `limit` results ordered by relevance (exact match first, then partial)
   */
  searchStocks(query: string, limit = 20): StockSearchResult[] {
    if (!query || query.trim().length === 0) {
      return [];
    }

    const searchTerm = query.trim();
    const searchPattern = `%${searchTerm}%`;

    // Search with ordering: exact code match first, then code prefix, then name matches
    const sqlQuery = `
      SELECT
        code,
        company_name,
        company_name_english,
        market_code,
        market_name,
        sector_33_name,
        CASE
          WHEN code = ? THEN 1
          WHEN code LIKE ? THEN 2
          WHEN company_name LIKE ? THEN 3
          WHEN company_name_english LIKE ? THEN 4
          ELSE 5
        END as relevance
      FROM stocks
      WHERE code LIKE ?
         OR company_name LIKE ?
         OR company_name_english LIKE ?
      ORDER BY relevance, code
      LIMIT ?
    `;

    const rows = this.sqlite.query(sqlQuery).all(
      searchTerm, // exact match
      `${searchTerm}%`, // code prefix
      searchPattern, // company_name
      searchPattern, // company_name_english
      searchPattern, // WHERE code
      searchPattern, // WHERE company_name
      searchPattern, // WHERE company_name_english
      limit
    ) as Array<{
      code: string;
      company_name: string;
      company_name_english: string | null;
      market_code: string;
      market_name: string;
      sector_33_name: string;
      relevance: number;
    }>;

    return rows.map((row) => ({
      code: row.code,
      companyName: row.company_name,
      companyNameEnglish: row.company_name_english,
      marketCode: row.market_code,
      marketName: row.market_name,
      sector33Name: row.sector_33_name,
    }));
  }

  /**
   * Get stocks that hit N-day high (highest close in the past N trading days)
   * Returns stocks where current close >= max(close) over past N days
   */
  getRankingByPeriodHigh(date: Date, periodDays: number, limit: number, marketCodes?: string[]): RankingItem[] {
    if (periodDays < 1) {
      throw new Error('Period days must be at least 1');
    }

    const dateStr = toISODateString(date);
    const startDate = this.getTradingDateBefore(date, periodDays);
    if (!startDate) return [];

    const startDateStr = toISODateString(startDate);

    let sqlQuery = `
      WITH period_high AS (
        SELECT
          code,
          MAX(high) as max_high
        FROM stock_data
        WHERE date > ? AND date < ?
        GROUP BY code
      )
      SELECT
        s.code,
        s.company_name,
        s.market_code,
        s.sector33_name,
        curr.close as current_price,
        curr.volume,
        curr.close * curr.volume as trading_value,
        ph.max_high as period_high_price,
        (curr.close - ph.max_high) as break_amount,
        ((curr.close - ph.max_high) / ph.max_high * 100) as break_percentage
      FROM stock_data curr
      JOIN stocks s ON s.code = curr.code
      JOIN period_high ph ON ph.code = curr.code
      WHERE curr.date = ?
        AND curr.close >= ph.max_high
        AND ph.max_high > 0
    `;

    const params: (string | number)[] = [startDateStr, dateStr, dateStr];

    if (marketCodes && marketCodes.length > 0) {
      const placeholders = marketCodes.map(() => '?').join(',');
      sqlQuery += ` AND s.market_code IN (${placeholders})`;
      params.push(...marketCodes);
    }

    sqlQuery += ' ORDER BY break_percentage DESC LIMIT ?';
    params.push(limit);

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      trading_value: number;
      period_high_price: number;
      break_amount: number;
      break_percentage: number;
    }>;

    return rows.map((row, index) => ({
      rank: index + 1,
      code: row.code,
      companyName: row.company_name,
      marketCode: row.market_code,
      sector33Name: row.sector33_name,
      currentPrice: row.current_price,
      volume: row.volume,
      tradingValue: row.trading_value,
      basePrice: row.period_high_price,
      changeAmount: row.break_amount,
      changePercentage: row.break_percentage,
      lookbackDays: periodDays,
    }));
  }

  /**
   * Get stocks that hit N-day low (lowest close in the past N trading days)
   * Returns stocks where current close <= min(low) over past N days
   */
  getRankingByPeriodLow(date: Date, periodDays: number, limit: number, marketCodes?: string[]): RankingItem[] {
    if (periodDays < 1) {
      throw new Error('Period days must be at least 1');
    }

    const dateStr = toISODateString(date);
    const startDate = this.getTradingDateBefore(date, periodDays);
    if (!startDate) return [];

    const startDateStr = toISODateString(startDate);

    let sqlQuery = `
      WITH period_low AS (
        SELECT
          code,
          MIN(low) as min_low
        FROM stock_data
        WHERE date > ? AND date < ?
        GROUP BY code
      )
      SELECT
        s.code,
        s.company_name,
        s.market_code,
        s.sector33_name,
        curr.close as current_price,
        curr.volume,
        curr.close * curr.volume as trading_value,
        pl.min_low as period_low_price,
        (curr.close - pl.min_low) as break_amount,
        ((curr.close - pl.min_low) / pl.min_low * 100) as break_percentage
      FROM stock_data curr
      JOIN stocks s ON s.code = curr.code
      JOIN period_low pl ON pl.code = curr.code
      WHERE curr.date = ?
        AND curr.close <= pl.min_low
        AND pl.min_low > 0
    `;

    const params: (string | number)[] = [startDateStr, dateStr, dateStr];

    if (marketCodes && marketCodes.length > 0) {
      const placeholders = marketCodes.map(() => '?').join(',');
      sqlQuery += ` AND s.market_code IN (${placeholders})`;
      params.push(...marketCodes);
    }

    // Sort by break_percentage ASC (most negative = most below the low)
    sqlQuery += ' ORDER BY break_percentage ASC LIMIT ?';
    params.push(limit);

    const rows = this.sqlite.query(sqlQuery).all(...params) as Array<{
      code: string;
      company_name: string;
      market_code: string;
      sector33_name: string;
      current_price: number;
      volume: number;
      trading_value: number;
      period_low_price: number;
      break_amount: number;
      break_percentage: number;
    }>;

    return rows.map((row, index) => ({
      rank: index + 1,
      code: row.code,
      companyName: row.company_name,
      marketCode: row.market_code,
      sector33Name: row.sector33_name,
      currentPrice: row.current_price,
      volume: row.volume,
      tradingValue: row.trading_value,
      basePrice: row.period_low_price,
      changeAmount: row.break_amount,
      changePercentage: row.break_percentage,
      lookbackDays: periodDays,
    }));
  }

  close(): void {
    this.sqlite.close();
  }
}
