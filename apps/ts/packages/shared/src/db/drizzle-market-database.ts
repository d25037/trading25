/**
 * Drizzle-based Market Database
 *
 * Type-safe market data management using Drizzle ORM.
 * Replaces the raw SQL implementation in market-sync/database.ts
 */

import { Database } from 'bun:sqlite';
import { and, eq, sql } from 'drizzle-orm';
import type { BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';
import { drizzle } from 'drizzle-orm/bun-sqlite';
import type { StockData, StockInfo, TopixData } from '../dataset/types';
import { toISODateString } from '../utils/date-helpers';
import { normalizeStockCode } from './columns/stock-code';
import { INDEX_MASTER_DATA } from './constants/index-master-data';
import {
  indexMaster,
  indicesData,
  MARKET_SCHEMA_VERSION,
  stockData,
  stocks,
  syncMetadata,
  topixData,
} from './schema/market-schema';
import { executeBulkInsert } from './transaction-helpers';

/**
 * Metadata keys for tracking sync state
 */
export const METADATA_KEYS = {
  INIT_COMPLETED: 'init_completed',
  LAST_SYNC_DATE: 'last_sync_date',
  LAST_STOCKS_REFRESH: 'last_stocks_refresh',
  FAILED_DATES: 'failed_dates',
  REFETCHED_STOCKS: 'refetched_stocks',
} as const;

/**
 * Adjustment event interface
 */
export interface AdjustmentEvent {
  code: string;
  date: Date;
  adjustmentFactor: number;
  close: number;
}

/**
 * Drizzle-based Market Database
 * Provides type-safe database operations with automatic stock code normalization
 */
export class DrizzleMarketDatabase {
  private sqlite: Database;
  private db: BunSQLiteDatabase;

  constructor(
    dbPath: string,
    private debug: boolean = false
  ) {
    this.sqlite = new Database(dbPath);
    this.db = drizzle(this.sqlite);
    this.initializeSchema();
  }

  /**
   * Initialize database schema
   */
  private initializeSchema(): void {
    // Enable WAL mode for better concurrency
    this.sqlite.exec('PRAGMA journal_mode = WAL');

    // Create tables using raw SQL (Drizzle push would be used in production)
    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS sync_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS stocks (
        code TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        company_name_english TEXT,
        market_code TEXT NOT NULL,
        market_name TEXT NOT NULL,
        sector_17_code TEXT NOT NULL,
        sector_17_name TEXT NOT NULL,
        sector_33_code TEXT NOT NULL,
        sector_33_name TEXT NOT NULL,
        scale_category TEXT,
        listed_date DATE NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS stock_data (
        code TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        adjustment_factor REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, date),
        FOREIGN KEY (code) REFERENCES stocks(code)
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS topix_data (
        date DATE PRIMARY KEY,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS index_master (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        name_english TEXT,
        category TEXT NOT NULL,
        data_start_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS indices_data (
        code TEXT NOT NULL,
        date DATE NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        sector_name TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, date),
        FOREIGN KEY (code) REFERENCES index_master(code)
      );
    `);

    // Create indexes
    this.sqlite.exec(`
      CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data(date);
      CREATE INDEX IF NOT EXISTS idx_stock_data_code ON stock_data(code);
      CREATE INDEX IF NOT EXISTS idx_topix_date ON topix_data(date);
      CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market_code);
      CREATE INDEX IF NOT EXISTS idx_indices_data_date ON indices_data(date);
      CREATE INDEX IF NOT EXISTS idx_indices_data_code ON indices_data(code);
    `);

    // Store schema version
    this.setMetadata('schema_version', MARKET_SCHEMA_VERSION);
  }

  // ===== METADATA MANAGEMENT =====

  getMetadata(key: string): string | null {
    const result = this.db.select().from(syncMetadata).where(eq(syncMetadata.key, key)).get();
    return result?.value ?? null;
  }

  setMetadata(key: string, value: string): void {
    this.db
      .insert(syncMetadata)
      .values({ key, value, updatedAt: sql`CURRENT_TIMESTAMP` })
      .onConflictDoUpdate({
        target: syncMetadata.key,
        set: { value, updatedAt: sql`CURRENT_TIMESTAMP` },
      })
      .run();
  }

  isInitialized(): boolean {
    return this.getMetadata(METADATA_KEYS.INIT_COMPLETED) === 'true';
  }

  markInitialized(): void {
    this.setMetadata(METADATA_KEYS.INIT_COMPLETED, 'true');
    this.setMetadata(METADATA_KEYS.LAST_SYNC_DATE, new Date().toISOString());
  }

  // ===== TRADING DAYS MANAGEMENT =====

  getTradingDays(): Date[] {
    const results = this.db.select({ date: topixData.date }).from(topixData).orderBy(sql`date ASC`).all();
    return results.map((row) => new Date(row.date));
  }

  getMissingTradingDays(topixDates: Date[]): Date[] {
    if (topixDates.length === 0) return [];

    const existingDates = new Set(this.getTradingDays().map(toISODateString));
    return topixDates.filter((date) => !existingDates.has(toISODateString(date)));
  }

  // ===== FAILED DATES TRACKING =====

  getFailedDates(): Date[] {
    const value = this.getMetadata(METADATA_KEYS.FAILED_DATES);
    if (!value) return [];

    try {
      const dates = JSON.parse(value) as string[];
      return dates.map((d) => new Date(d));
    } catch {
      return [];
    }
  }

  recordFailedDate(date: Date): void {
    const failedDates = this.getFailedDates();
    const dateStr = toISODateString(date);

    if (!failedDates.some((d) => toISODateString(d) === dateStr)) {
      failedDates.push(date);
      this.setMetadata(METADATA_KEYS.FAILED_DATES, JSON.stringify(failedDates.map((d) => d.toISOString())));
    }
  }

  clearFailedDate(date: Date): void {
    const failedDates = this.getFailedDates();
    const dateStr = toISODateString(date);
    const filtered = failedDates.filter((d) => toISODateString(d) !== dateStr);
    this.setMetadata(METADATA_KEYS.FAILED_DATES, JSON.stringify(filtered.map((d) => d.toISOString())));
  }

  // ===== DATA INSERTION =====

  updateStocksList(stocksList: StockInfo[]): void {
    executeBulkInsert(
      this.sqlite,
      stocksList,
      (stock) => {
        const normalizedCode = normalizeStockCode(stock.code);
        this.db
          .insert(stocks)
          .values({
            code: normalizedCode,
            companyName: stock.companyName,
            companyNameEnglish: stock.companyNameEnglish || null,
            marketCode: stock.marketCode,
            marketName: stock.marketName,
            sector17Code: stock.sector17Code,
            sector17Name: stock.sector17Name,
            sector33Code: stock.sector33Code,
            sector33Name: stock.sector33Name,
            scaleCategory: stock.scaleCategory || null,
            listedDate: toISODateString(stock.listedDate),
            updatedAt: sql`CURRENT_TIMESTAMP`,
          })
          .onConflictDoUpdate({
            target: stocks.code,
            set: {
              companyName: stock.companyName,
              companyNameEnglish: stock.companyNameEnglish || null,
              marketCode: stock.marketCode,
              marketName: stock.marketName,
              sector17Code: stock.sector17Code,
              sector17Name: stock.sector17Name,
              sector33Code: stock.sector33Code,
              sector33Name: stock.sector33Name,
              scaleCategory: stock.scaleCategory || null,
              listedDate: toISODateString(stock.listedDate),
              updatedAt: sql`CURRENT_TIMESTAMP`,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'updateStocksList' }
    );
  }

  insertStockDataBulk(code: string, data: StockData[]): void {
    const normalizedCode = normalizeStockCode(code);
    executeBulkInsert(
      this.sqlite,
      data,
      (item) => {
        const dateStr = toISODateString(item.date);
        this.db
          .insert(stockData)
          .values({
            code: normalizedCode,
            date: dateStr,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
            volume: item.volume,
            adjustmentFactor: item.adjustmentFactor ?? null,
          })
          .onConflictDoUpdate({
            target: [stockData.code, stockData.date],
            set: {
              open: item.open,
              high: item.high,
              low: item.low,
              close: item.close,
              volume: item.volume,
              adjustmentFactor: item.adjustmentFactor ?? null,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'insertStockDataBulk' }
    );
  }

  insertStockDataForDate(date: Date, data: StockData[]): void {
    // Get all stock codes from stocks table
    const existingCodes = new Set(
      this.db
        .select({ code: stocks.code })
        .from(stocks)
        .all()
        .map((r) => r.code)
    );

    // Filter data to only include stocks that exist in stocks table
    const validData = data.filter((item) => existingCodes.has(normalizeStockCode(item.code)));
    const skippedCount = data.length - validData.length;

    if (this.debug && skippedCount > 0) {
      console.error(`[DB DEBUG] Skipping ${skippedCount} quotes for unlisted stocks`);
    }

    const dateStr = toISODateString(date);

    executeBulkInsert(
      this.sqlite,
      validData,
      (item) => {
        const normalizedCode = normalizeStockCode(item.code);
        this.db
          .insert(stockData)
          .values({
            code: normalizedCode,
            date: dateStr,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
            volume: item.volume,
            adjustmentFactor: item.adjustmentFactor ?? null,
          })
          .onConflictDoUpdate({
            target: [stockData.code, stockData.date],
            set: {
              open: item.open,
              high: item.high,
              low: item.low,
              close: item.close,
              volume: item.volume,
              adjustmentFactor: item.adjustmentFactor ?? null,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'insertStockDataForDate' }
    );
  }

  insertTopixData(data: TopixData[]): void {
    executeBulkInsert(
      this.sqlite,
      data,
      (item) => {
        const dateStr = toISODateString(item.date);
        this.db
          .insert(topixData)
          .values({
            date: dateStr,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
          })
          .onConflictDoUpdate({
            target: topixData.date,
            set: {
              open: item.open,
              high: item.high,
              low: item.low,
              close: item.close,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'insertTopixData' }
    );
  }

  // ===== DATA RETRIEVAL =====

  getStockCount(): { total: number; byMarket: Record<string, number> } {
    const totalResult = this.db.select({ count: sql<number>`COUNT(*)` }).from(stocks).get();
    const total = totalResult?.count ?? 0;

    const marketResults = this.db
      .select({
        marketCode: stocks.marketCode,
        count: sql<number>`COUNT(*)`,
      })
      .from(stocks)
      .groupBy(stocks.marketCode)
      .all();

    const byMarket: Record<string, number> = {};
    for (const row of marketResults) {
      if (row.marketCode) {
        byMarket[row.marketCode] = row.count;
      }
    }

    return { total, byMarket };
  }

  getStockDataDateRange(): { min: Date | null; max: Date | null; count: number; dateCount: number } {
    const result = this.db
      .select({
        min: sql<string | null>`MIN(date)`,
        max: sql<string | null>`MAX(date)`,
        count: sql<number>`COUNT(*)`,
        dateCount: sql<number>`COUNT(DISTINCT date)`,
      })
      .from(stockData)
      .get();

    return {
      min: result?.min ? new Date(result.min) : null,
      max: result?.max ? new Date(result.max) : null,
      count: result?.count ?? 0,
      dateCount: result?.dateCount ?? 0,
    };
  }

  getTopixDateRange(): { min: Date | null; max: Date | null; count: number } {
    const result = this.db
      .select({
        min: sql<string | null>`MIN(date)`,
        max: sql<string | null>`MAX(date)`,
        count: sql<number>`COUNT(*)`,
      })
      .from(topixData)
      .get();

    return {
      min: result?.min ? new Date(result.min) : null,
      max: result?.max ? new Date(result.max) : null,
      count: result?.count ?? 0,
    };
  }

  getMissingStockDataDates(): Date[] {
    const tradingDays = this.getTradingDays();
    if (tradingDays.length === 0) return [];

    const stockDatesResult = this.db.select({ date: stockData.date }).from(stockData).groupBy(stockData.date).all();
    const stockDates = new Set(stockDatesResult.map((r) => r.date));

    return tradingDays.filter((date) => !stockDates.has(toISODateString(date)));
  }

  // ===== STOCK SPLIT / MERGER MANAGEMENT =====

  getAdjustmentEvents(limit: number = 20): AdjustmentEvent[] {
    const results = this.db
      .select({
        code: stockData.code,
        date: stockData.date,
        adjustmentFactor: stockData.adjustmentFactor,
        close: stockData.close,
      })
      .from(stockData)
      .where(and(sql`adjustment_factor IS NOT NULL`, sql`adjustment_factor != 1.0`))
      .orderBy(sql`date DESC`)
      .limit(limit)
      .all();

    return results.map((row) => ({
      code: row.code,
      date: new Date(row.date),
      adjustmentFactor: row.adjustmentFactor ?? 1.0,
      close: row.close,
    }));
  }

  /**
   * Get stocks needing refresh with their latest adjustment factor
   * Returns stocks where adjustment_factor != 1.0
   */
  getStocksNeedingRefresh(): string[] {
    const results = this.db
      .selectDistinct({ code: stockData.code })
      .from(stockData)
      .where(and(sql`adjustment_factor IS NOT NULL`, sql`adjustment_factor != 1.0`))
      .orderBy(stockData.code)
      .all();

    return results.map((r) => r.code);
  }

  /**
   * Get the latest adjustment factor for a stock (most recent date with adjustment != 1.0)
   */
  getLatestAdjustmentFactor(code: string): number | null {
    const normalizedCode = normalizeStockCode(code);
    const result = this.db
      .select({ adjustmentFactor: stockData.adjustmentFactor })
      .from(stockData)
      .where(and(eq(stockData.code, normalizedCode), sql`adjustment_factor IS NOT NULL`, sql`adjustment_factor != 1.0`))
      .orderBy(sql`date DESC`)
      .limit(1)
      .get();

    return result?.adjustmentFactor ?? null;
  }

  /**
   * Mark stock as refreshed with the adjustment factor at time of refresh
   */
  markStockRefreshed(code: string, adjustmentFactor?: number, date: Date = new Date()): void {
    const normalizedCode = normalizeStockCode(code);
    const refetchedStocks = this.getRefreshedStocks();

    // Remove existing entry for this code (will be replaced with new data)
    const filtered = refetchedStocks.filter((item) => item.code !== normalizedCode);

    // Get current adjustment factor if not provided
    const factor = adjustmentFactor ?? this.getLatestAdjustmentFactor(normalizedCode);

    filtered.push({
      code: normalizedCode,
      refetchedAt: date.toISOString(),
      lastAdjustmentFactor: factor,
    });
    this.setMetadata(METADATA_KEYS.REFETCHED_STOCKS, JSON.stringify(filtered));
  }

  getRefreshedStocks(): Array<{ code: string; refetchedAt: string; lastAdjustmentFactor?: number | null }> {
    const value = this.getMetadata(METADATA_KEYS.REFETCHED_STOCKS);
    if (!value) return [];

    try {
      return JSON.parse(value) as Array<{ code: string; refetchedAt: string; lastAdjustmentFactor?: number | null }>;
    } catch {
      return [];
    }
  }

  /**
   * Check if a stock needs refetch based on adjustment factor change
   */
  needsRefetch(code: string): boolean {
    const normalizedCode = normalizeStockCode(code);
    const refetchedStocks = this.getRefreshedStocks();
    const refetchedEntry = refetchedStocks.find((item) => item.code === normalizedCode);

    if (!refetchedEntry) {
      // Never refetched, needs refetch
      return true;
    }

    const currentFactor = this.getLatestAdjustmentFactor(normalizedCode);
    if (currentFactor === null) {
      // No adjustment factor anymore (shouldn't happen but handle gracefully)
      return false;
    }

    // If lastAdjustmentFactor wasn't stored (old format), migrate and skip refetch
    // 旧形式: refetchedAt はあるが lastAdjustmentFactor がない
    // → すでに refetch 済みなので、現在の factor を記録して false を返す
    if (refetchedEntry.lastAdjustmentFactor === undefined) {
      this.markStockRefreshed(normalizedCode, currentFactor);
      return false;
    }

    // Compare factors - if changed, needs refetch
    return refetchedEntry.lastAdjustmentFactor !== currentFactor;
  }

  /**
   * Mark all stocks with existing adjustment events as already processed.
   * Should be called after initial sync to prevent unnecessary refetching.
   */
  markExistingAdjustmentsAsProcessed(): number {
    const stocksWithAdjustments = this.getStocksNeedingRefresh();

    for (const code of stocksWithAdjustments) {
      const factor = this.getLatestAdjustmentFactor(code);
      if (factor !== null) {
        this.markStockRefreshed(code, factor);
      }
    }

    return stocksWithAdjustments.length;
  }

  getStocksOutsideTopixRange(): Array<{ code: string; count: number }> {
    const topixRange = this.getTopixDateRange();
    if (!topixRange.min || !topixRange.max) return [];

    const minDate = toISODateString(topixRange.min);
    const maxDate = toISODateString(topixRange.max);

    const results = this.db
      .select({
        code: stockData.code,
        count: sql<number>`COUNT(*)`,
      })
      .from(stockData)
      .where(sql`date < ${minDate} OR date > ${maxDate}`)
      .groupBy(stockData.code)
      .orderBy(sql`COUNT(*) DESC`)
      .all();

    return results.map((r) => ({ code: r.code, count: r.count }));
  }

  // ===== INDEX MASTER MANAGEMENT =====

  /**
   * Initialize index master data from static definitions
   */
  initializeIndexMaster(): void {
    executeBulkInsert(
      this.sqlite,
      INDEX_MASTER_DATA,
      (index) => {
        this.db
          .insert(indexMaster)
          .values({
            code: index.code,
            name: index.name,
            nameEnglish: index.nameEnglish,
            category: index.category,
            dataStartDate: index.dataStartDate,
            updatedAt: sql`CURRENT_TIMESTAMP`,
          })
          .onConflictDoUpdate({
            target: indexMaster.code,
            set: {
              name: index.name,
              nameEnglish: index.nameEnglish,
              category: index.category,
              dataStartDate: index.dataStartDate,
              updatedAt: sql`CURRENT_TIMESTAMP`,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'initializeIndexMaster' }
    );

    if (this.debug) {
      console.error(`[DrizzleMarketDatabase] Initialized ${INDEX_MASTER_DATA.length} index master records`);
    }
  }

  /**
   * Get all index codes from master table
   */
  getIndexCodes(): string[] {
    const results = this.db.select({ code: indexMaster.code }).from(indexMaster).orderBy(indexMaster.code).all();
    return results.map((r) => r.code);
  }

  /**
   * Check if index master is initialized
   */
  isIndexMasterInitialized(): boolean {
    const result = this.db.select({ count: sql<number>`COUNT(*)` }).from(indexMaster).get();
    return (result?.count ?? 0) > 0;
  }

  // ===== INDICES DATA MANAGEMENT =====

  /**
   * Index data interface for insertion
   */
  insertIndicesData(
    data: Array<{
      code: string;
      date: Date;
      open: number | null;
      high: number | null;
      low: number | null;
      close: number | null;
    }>
  ): void {
    executeBulkInsert(
      this.sqlite,
      data,
      (item) => {
        const dateStr = toISODateString(item.date);
        this.db
          .insert(indicesData)
          .values({
            code: item.code,
            date: dateStr,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
          })
          .onConflictDoUpdate({
            target: [indicesData.code, indicesData.date],
            set: {
              open: item.open,
              high: item.high,
              low: item.low,
              close: item.close,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'insertIndicesData' }
    );
  }

  /**
   * Insert indices data for a specific date (all indices for that date)
   */
  insertIndicesDataForDate(
    date: Date,
    data: Array<{ code: string; open: number | null; high: number | null; low: number | null; close: number | null }>
  ): void {
    const dateStr = toISODateString(date);

    executeBulkInsert(
      this.sqlite,
      data,
      (item) => {
        this.db
          .insert(indicesData)
          .values({
            code: item.code,
            date: dateStr,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
          })
          .onConflictDoUpdate({
            target: [indicesData.code, indicesData.date],
            set: {
              open: item.open,
              high: item.high,
              low: item.low,
              close: item.close,
            },
          })
          .run();
      },
      { debug: this.debug, operationName: 'insertIndicesDataForDate' }
    );

    if (this.debug) {
      console.error(`[DrizzleMarketDatabase] Inserted ${data.length} indices for ${dateStr}`);
    }
  }

  /**
   * Get indices data date range
   */
  getIndicesDataRange(): { min: Date | null; max: Date | null; count: number } {
    const result = this.db
      .select({
        min: sql<string | null>`MIN(date)`,
        max: sql<string | null>`MAX(date)`,
        count: sql<number>`COUNT(DISTINCT date)`,
      })
      .from(indicesData)
      .get();

    return {
      min: result?.min ? new Date(result.min) : null,
      max: result?.max ? new Date(result.max) : null,
      count: result?.count ?? 0,
    };
  }

  /**
   * Get missing indices dates compared to trading days
   */
  getMissingIndicesDates(): Date[] {
    const tradingDays = this.getTradingDays();
    if (tradingDays.length === 0) return [];

    const indicesDatesResult = this.db
      .select({ date: indicesData.date })
      .from(indicesData)
      .groupBy(indicesData.date)
      .all();
    const indicesDates = new Set(indicesDatesResult.map((r) => r.date));

    return tradingDays.filter((date) => !indicesDates.has(toISODateString(date)));
  }

  /**
   * Get indices data count by code
   */
  getIndicesDataCountByCode(): Record<string, number> {
    const results = this.db
      .select({
        code: indicesData.code,
        count: sql<number>`COUNT(*)`,
      })
      .from(indicesData)
      .groupBy(indicesData.code)
      .all();

    const counts: Record<string, number> = {};
    for (const row of results) {
      counts[row.code] = row.count;
    }
    return counts;
  }

  /**
   * Get index master count
   */
  getIndexMasterCount(): number {
    const result = this.db.select({ count: sql<number>`COUNT(*)` }).from(indexMaster).get();
    return result?.count ?? 0;
  }

  /**
   * Get index master count by category
   */
  getIndexMasterCountByCategory(): Record<string, number> {
    const results = this.db
      .select({
        category: indexMaster.category,
        count: sql<number>`COUNT(*)`,
      })
      .from(indexMaster)
      .groupBy(indexMaster.category)
      .all();

    const counts: Record<string, number> = {};
    for (const row of results) {
      counts[row.category] = row.count;
    }
    return counts;
  }

  /**
   * Get total indices data count
   */
  getIndicesDataCount(): number {
    const result = this.db.select({ count: sql<number>`COUNT(*)` }).from(indicesData).get();
    return result?.count ?? 0;
  }

  /**
   * Get indices data for a specific code and date range
   */
  getIndicesDataByCode(
    code: string,
    from?: Date,
    to?: Date
  ): Array<{ date: string; open: number | null; high: number | null; low: number | null; close: number | null }> {
    if (from && to) {
      const fromStr = toISODateString(from);
      const toStr = toISODateString(to);
      return this.db
        .select({
          date: indicesData.date,
          open: indicesData.open,
          high: indicesData.high,
          low: indicesData.low,
          close: indicesData.close,
        })
        .from(indicesData)
        .where(and(eq(indicesData.code, code), sql`date >= ${fromStr} AND date <= ${toStr}`))
        .orderBy(sql`date ASC`)
        .all();
    }

    return this.db
      .select({
        date: indicesData.date,
        open: indicesData.open,
        high: indicesData.high,
        low: indicesData.low,
        close: indicesData.close,
      })
      .from(indicesData)
      .where(eq(indicesData.code, code))
      .orderBy(sql`date ASC`)
      .all();
  }

  close(): void {
    // Force WAL checkpoint before closing to ensure data durability
    try {
      this.sqlite.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch {
      // Ignore checkpoint errors on close
    }
    this.sqlite.close();
    if (this.debug) {
      console.log('[DrizzleMarketDatabase] Database connection closed');
    }
  }
}
