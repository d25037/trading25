/**
 * Drizzle-based Dataset Database
 *
 * Type-safe dataset management using Drizzle ORM.
 * Replaces the raw SQL implementation in dataset/database/
 */

import { Database } from 'bun:sqlite';
import { and, count, eq, gte, lte, max, min, sql } from 'drizzle-orm';
import type { BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';
import { drizzle } from 'drizzle-orm/bun-sqlite';
import type {
  DatasetStats,
  DateRange,
  MarginData,
  SectorData,
  StatementsData,
  StatementsFieldCoverage,
  StockData,
  StockInfo,
  TopixData,
} from '../dataset/types';
import { DatabaseError } from '../dataset/types';
import { dateRangeToISO, toISODateString } from '../utils/date-helpers';
import { normalizeStockCode } from './columns/stock-code';
import {
  datasetDailyQuotes,
  datasetIndices,
  datasetInfo,
  datasetMarginData,
  datasetStatements,
  datasetStocks,
  datasetTopix,
} from './schema/dataset-schema';

/**
 * Schema version for dataset database
 */
const DATASET_SCHEMA_VERSION = '2.3.0';

/**
 * Metadata keys for dataset database
 */
export const DATASET_METADATA_KEYS = {
  PRESET: 'preset',
  CREATED_AT: 'created_at',
} as const;

/**
 * Drizzle-based Dataset Database
 * Provides type-safe database operations with automatic stock code normalization
 */
export class DrizzleDatasetDatabase {
  private sqlite: Database;
  private db: BunSQLiteDatabase;
  private isDebugMode: boolean;

  constructor(databasePath: string, debug: boolean = false) {
    this.isDebugMode = process.env.DATASET_DEBUG === 'true' || debug;
    this.sqlite = new Database(databasePath);
    this.db = drizzle(this.sqlite);
    this.initializeSchema();
  }

  /**
   * Initialize database schema
   */
  private initializeSchema(): void {
    // Enable WAL mode for better concurrency
    this.sqlite.exec('PRAGMA journal_mode = WAL');

    // Enable foreign keys for cascade operations
    this.sqlite.exec('PRAGMA foreign_keys = ON');

    // Performance optimizations
    this.sqlite.exec('PRAGMA synchronous = NORMAL');
    this.sqlite.exec('PRAGMA temp_store = memory');

    // Create tables using raw SQL (snake_case naming to match market.db)
    this.sqlite.run(`
      CREATE TABLE IF NOT EXISTS dataset_info (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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
        listed_date TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS stock_data (
        code TEXT NOT NULL,
        date TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        adjustment_factor REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, date),
        FOREIGN KEY (code) REFERENCES stocks(code) ON DELETE CASCADE
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS margin_data (
        code TEXT NOT NULL,
        date TEXT NOT NULL,
        long_margin_volume REAL,
        short_margin_volume REAL,
        PRIMARY KEY (code, date),
        FOREIGN KEY (code) REFERENCES stocks(code) ON DELETE CASCADE
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS topix_data (
        date TEXT PRIMARY KEY,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS indices_data (
        code TEXT NOT NULL,
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        sector_name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (code, date)
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS statements (
        code TEXT NOT NULL,
        disclosed_date TEXT NOT NULL,
        earnings_per_share REAL,
        profit REAL,
        equity REAL,
        type_of_current_period TEXT,
        type_of_document TEXT,
        next_year_forecast_earnings_per_share REAL,
        bps REAL,
        sales REAL,
        operating_profit REAL,
        ordinary_profit REAL,
        operating_cash_flow REAL,
        dividend_fy REAL,
        forecast_eps REAL,
        investing_cash_flow REAL,
        financing_cash_flow REAL,
        cash_and_equivalents REAL,
        total_assets REAL,
        shares_outstanding REAL,
        treasury_shares REAL,
        PRIMARY KEY (code, disclosed_date),
        FOREIGN KEY (code) REFERENCES stocks(code) ON DELETE CASCADE
      );
    `);

    // Create indexes
    this.sqlite.exec(`
      CREATE INDEX IF NOT EXISTS idx_stocks_market ON stocks(market_code);
      CREATE INDEX IF NOT EXISTS idx_stocks_sector ON stocks(sector_33_code);
      CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data(date);
      CREATE INDEX IF NOT EXISTS idx_stock_data_code ON stock_data(code);
      CREATE INDEX IF NOT EXISTS idx_margin_data_date ON margin_data(date);
      CREATE INDEX IF NOT EXISTS idx_margin_data_code ON margin_data(code);
      CREATE INDEX IF NOT EXISTS idx_topix_date ON topix_data(date);
      CREATE INDEX IF NOT EXISTS idx_indices_data_date ON indices_data(date);
      CREATE INDEX IF NOT EXISTS idx_indices_data_code ON indices_data(code);
      CREATE INDEX IF NOT EXISTS idx_statements_date ON statements(disclosed_date);
      CREATE INDEX IF NOT EXISTS idx_statements_code ON statements(code);
    `);

    // Store schema version
    this.setMetadata('schema_version', DATASET_SCHEMA_VERSION);
  }

  // ===== METADATA OPERATIONS =====

  setMetadata(key: string, value: string): void {
    this.db
      .insert(datasetInfo)
      .values({ key, value, updatedAt: sql`datetime('now')` })
      .onConflictDoUpdate({
        target: datasetInfo.key,
        set: { value, updatedAt: sql`datetime('now')` },
      })
      .run();
  }

  getMetadata(key: string): string | null {
    try {
      const result = this.db.select().from(datasetInfo).where(eq(datasetInfo.key, key)).get();
      return result?.value ?? null;
    } catch (error) {
      throw new DatabaseError(`Failed to get metadata for key ${key}`, error instanceof Error ? error : undefined);
    }
  }

  // ===== STOCK OPERATIONS =====

  async insertStock(stock: StockInfo): Promise<void> {
    try {
      const normalizedCode = normalizeStockCode(stock.code);
      this.db
        .insert(datasetStocks)
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
          scaleCategory: stock.scaleCategory,
          listedDate: stock.listedDate.toISOString(),
        })
        .onConflictDoUpdate({
          target: datasetStocks.code,
          set: {
            companyName: stock.companyName,
            companyNameEnglish: stock.companyNameEnglish || null,
            marketCode: stock.marketCode,
            marketName: stock.marketName,
            sector17Code: stock.sector17Code,
            sector17Name: stock.sector17Name,
            sector33Code: stock.sector33Code,
            sector33Name: stock.sector33Name,
            scaleCategory: stock.scaleCategory,
            listedDate: stock.listedDate.toISOString(),
          },
        })
        .run();
    } catch (error) {
      throw new DatabaseError(
        `Failed to insert stock ${stock.code}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  async insertStocks(stocks: StockInfo[]): Promise<void> {
    const transaction = this.sqlite.transaction(() => {
      for (const stock of stocks) {
        this.logStockDebugInfo(stock);
        const normalizedCode = normalizeStockCode(stock.code);
        const dateString = this.validateAndFormatDate(stock);

        this.db
          .insert(datasetStocks)
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
            scaleCategory: stock.scaleCategory,
            listedDate: dateString,
          })
          .onConflictDoUpdate({
            target: datasetStocks.code,
            set: {
              companyName: stock.companyName,
              companyNameEnglish: stock.companyNameEnglish || null,
              marketCode: stock.marketCode,
              marketName: stock.marketName,
              sector17Code: stock.sector17Code,
              sector17Name: stock.sector17Name,
              sector33Code: stock.sector33Code,
              sector33Name: stock.sector33Name,
              scaleCategory: stock.scaleCategory,
              listedDate: dateString,
            },
          })
          .run();
      }
    });

    try {
      transaction();
    } catch (error) {
      console.log(`Failed to insert ${stocks.length} stocks`);
      throw new DatabaseError(
        `Failed to insert ${stocks.length} stocks: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  private logStockDebugInfo(stock: StockInfo): void {
    if (!this.isDebugMode) return;

    console.log(`DEBUG: Inserting stock ${stock.code}:`, {
      code: stock.code,
      companyName: stock.companyName,
      marketCode: stock.marketCode,
      listedDate: stock.listedDate instanceof Date ? stock.listedDate.toISOString() : stock.listedDate,
      sector17Code: stock.sector17Code,
      sector33Code: stock.sector33Code,
    });
  }

  private validateAndFormatDate(stock: StockInfo): string {
    if (stock.listedDate instanceof Date && !Number.isNaN(stock.listedDate.getTime())) {
      return stock.listedDate.toISOString();
    }

    if (this.isDebugMode) {
      console.log(`DEBUG: Invalid listedDate for stock ${stock.code}, using default`);
    }
    return '1970-01-01T00:00:00.000Z';
  }

  getStockList(marketCodes?: string[]): StockInfo[] {
    try {
      let results: (typeof datasetStocks.$inferSelect)[];

      if (marketCodes && marketCodes.length > 0) {
        // Use raw SQL for IN clause
        const placeholders = marketCodes.map(() => '?').join(', ');
        const rows = this.sqlite
          .query(`SELECT * FROM stocks WHERE market_code IN (${placeholders}) ORDER BY code`)
          .all(...marketCodes) as (typeof datasetStocks.$inferSelect)[];
        results = rows;
      } else {
        results = this.db.select().from(datasetStocks).orderBy(datasetStocks.code).all();
      }

      return results.map((row) => this.mapStockRow(row));
    } catch (error) {
      throw new DatabaseError('Failed to get stock list', error instanceof Error ? error : new Error(String(error)));
    }
  }

  // ===== QUOTE OPERATIONS =====

  async insertStockData(stockCode: string, quotes: StockData[]): Promise<void> {
    const normalizedCode = normalizeStockCode(stockCode);
    const transaction = this.sqlite.transaction(() => {
      for (const quote of quotes) {
        this.db
          .insert(datasetDailyQuotes)
          .values({
            code: normalizedCode,
            date: toISODateString(quote.date),
            open: quote.open,
            high: quote.high,
            low: quote.low,
            close: quote.close,
            volume: quote.volume,
            adjustmentFactor: quote.adjustmentFactor ?? null,
          })
          .onConflictDoUpdate({
            target: [datasetDailyQuotes.code, datasetDailyQuotes.date],
            set: {
              open: quote.open,
              high: quote.high,
              low: quote.low,
              close: quote.close,
              volume: quote.volume,
              adjustmentFactor: quote.adjustmentFactor ?? null,
            },
          })
          .run();
      }
    });

    try {
      transaction();
    } catch (error) {
      throw new DatabaseError(
        `Failed to insert quotes for ${stockCode}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  getStockData(stockCode: string, dateRange?: DateRange): StockData[] {
    try {
      const normalizedCode = normalizeStockCode(stockCode);
      let results: (typeof datasetDailyQuotes.$inferSelect)[];

      if (dateRange) {
        const { from, to } = dateRangeToISO(dateRange);
        results = this.db
          .select()
          .from(datasetDailyQuotes)
          .where(
            and(
              eq(datasetDailyQuotes.code, normalizedCode),
              gte(datasetDailyQuotes.date, from),
              lte(datasetDailyQuotes.date, to)
            )
          )
          .orderBy(datasetDailyQuotes.date)
          .all();
      } else {
        results = this.db
          .select()
          .from(datasetDailyQuotes)
          .where(eq(datasetDailyQuotes.code, normalizedCode))
          .orderBy(datasetDailyQuotes.date)
          .all();
      }

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
    } catch (error) {
      throw new DatabaseError(
        `Failed to get stock data for ${stockCode}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== MARGIN DATA OPERATIONS =====

  async insertMarginData(stockCode: string, marginData: MarginData[]): Promise<void> {
    const normalizedCode = normalizeStockCode(stockCode);
    const transaction = this.sqlite.transaction(() => {
      for (const data of marginData) {
        this.db
          .insert(datasetMarginData)
          .values({
            code: normalizedCode,
            date: toISODateString(data.date),
            longMarginVolume: data.longMarginVolume,
            shortMarginVolume: data.shortMarginVolume,
          })
          .onConflictDoUpdate({
            target: [datasetMarginData.code, datasetMarginData.date],
            set: {
              longMarginVolume: data.longMarginVolume,
              shortMarginVolume: data.shortMarginVolume,
            },
          })
          .run();
      }
    });

    try {
      transaction();
    } catch (error) {
      throw new DatabaseError(
        `Failed to insert margin data for ${stockCode}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  getMarginData(stockCode: string, dateRange?: DateRange): MarginData[] {
    try {
      const normalizedCode = normalizeStockCode(stockCode);
      let results: (typeof datasetMarginData.$inferSelect)[];

      if (dateRange) {
        const { from, to } = dateRangeToISO(dateRange);
        results = this.db
          .select()
          .from(datasetMarginData)
          .where(
            and(
              eq(datasetMarginData.code, normalizedCode),
              gte(datasetMarginData.date, from),
              lte(datasetMarginData.date, to)
            )
          )
          .orderBy(datasetMarginData.date)
          .all();
      } else {
        results = this.db
          .select()
          .from(datasetMarginData)
          .where(eq(datasetMarginData.code, normalizedCode))
          .orderBy(datasetMarginData.date)
          .all();
      }

      return results.map((row) => ({
        code: row.code,
        date: new Date(row.date),
        longMarginVolume: row.longMarginVolume,
        shortMarginVolume: row.shortMarginVolume,
      }));
    } catch (error) {
      throw new DatabaseError(
        `Failed to get margin data for ${stockCode}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== TOPIX OPERATIONS =====

  async insertTopixData(topixData: TopixData[]): Promise<void> {
    const transaction = this.sqlite.transaction(() => {
      for (const data of topixData) {
        this.db
          .insert(datasetTopix)
          .values({
            date: toISODateString(data.date),
            open: data.open,
            high: data.high,
            low: data.low,
            close: data.close,
          })
          .onConflictDoUpdate({
            target: datasetTopix.date,
            set: {
              open: data.open,
              high: data.high,
              low: data.low,
              close: data.close,
            },
          })
          .run();
      }
    });

    try {
      transaction();
    } catch (error) {
      throw new DatabaseError('Failed to insert TOPIX data', error instanceof Error ? error : new Error(String(error)));
    }
  }

  getTopixData(dateRange?: DateRange): TopixData[] {
    try {
      let results: (typeof datasetTopix.$inferSelect)[];

      if (dateRange) {
        const { from, to } = dateRangeToISO(dateRange);
        results = this.db
          .select()
          .from(datasetTopix)
          .where(and(gte(datasetTopix.date, from), lte(datasetTopix.date, to)))
          .orderBy(datasetTopix.date)
          .all();
      } else {
        results = this.db.select().from(datasetTopix).orderBy(datasetTopix.date).all();
      }

      return results.map((row) => ({
        date: new Date(row.date),
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
      }));
    } catch (error) {
      throw new DatabaseError('Failed to get TOPIX data', error instanceof Error ? error : new Error(String(error)));
    }
  }

  // ===== SECTOR INDICES OPERATIONS =====

  async insertSectorData(sectorData: SectorData[]): Promise<void> {
    const transaction = this.sqlite.transaction(() => {
      for (const data of sectorData) {
        this.db
          .insert(datasetIndices)
          .values({
            code: data.sectorCode,
            sectorName: data.sectorName,
            date: toISODateString(data.date),
            open: data.open,
            high: data.high,
            low: data.low,
            close: data.close,
          })
          .onConflictDoUpdate({
            target: [datasetIndices.code, datasetIndices.date],
            set: {
              sectorName: data.sectorName,
              open: data.open,
              high: data.high,
              low: data.low,
              close: data.close,
            },
          })
          .run();
      }
    });

    try {
      transaction();
    } catch (error) {
      throw new DatabaseError(
        'Failed to insert sector data',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  getSectorData(sectorCode?: string, dateRange?: DateRange): SectorData[] {
    try {
      const conditions: ReturnType<typeof eq>[] = [];

      if (sectorCode) {
        conditions.push(eq(datasetIndices.code, sectorCode));
      }

      if (dateRange) {
        const { from, to } = dateRangeToISO(dateRange);
        conditions.push(gte(datasetIndices.date, from));
        conditions.push(lte(datasetIndices.date, to));
      }

      let results: (typeof datasetIndices.$inferSelect)[];

      if (conditions.length > 0) {
        results = this.db
          .select()
          .from(datasetIndices)
          .where(and(...conditions))
          .orderBy(datasetIndices.code, datasetIndices.date)
          .all();
      } else {
        results = this.db.select().from(datasetIndices).orderBy(datasetIndices.code, datasetIndices.date).all();
      }

      return results.map((row) => ({
        sectorCode: row.code,
        sectorName: row.sectorName ?? '',
        date: new Date(row.date),
        open: row.open ?? 0,
        high: row.high ?? 0,
        low: row.low ?? 0,
        close: row.close ?? 0,
      }));
    } catch (error) {
      throw new DatabaseError('Failed to get sector data', error instanceof Error ? error : new Error(String(error)));
    }
  }

  // ===== STATEMENTS OPERATIONS =====

  async insertStatementsData(stockCode: string, statements: StatementsData[]): Promise<void> {
    const normalizedCode = normalizeStockCode(stockCode);

    if (this.isDebugMode) {
      console.log(`[DATABASE] Starting statements insertion for ${stockCode}:`, {
        stockCode,
        statementsCount: statements.length,
      });
    }

    const transaction = this.sqlite.transaction(() => {
      for (const data of statements) {
        const fields = {
          earningsPerShare: data.earningsPerShare,
          profit: data.profit,
          equity: data.equity,
          typeOfCurrentPeriod: data.typeOfCurrentPeriod,
          typeOfDocument: data.typeOfDocument,
          nextYearForecastEarningsPerShare: data.nextYearForecastEarningsPerShare,
          bps: data.bps,
          sales: data.sales,
          operatingProfit: data.operatingProfit,
          ordinaryProfit: data.ordinaryProfit,
          operatingCashFlow: data.operatingCashFlow,
          dividendFY: data.dividendFY,
          forecastEps: data.forecastEps,
          investingCashFlow: data.investingCashFlow,
          financingCashFlow: data.financingCashFlow,
          cashAndEquivalents: data.cashAndEquivalents,
          totalAssets: data.totalAssets,
          sharesOutstanding: data.sharesOutstanding,
          treasuryShares: data.treasuryShares,
        };

        this.db
          .insert(datasetStatements)
          .values({
            code: normalizedCode,
            disclosedDate: toISODateString(data.disclosedDate),
            ...fields,
          })
          .onConflictDoUpdate({
            target: [datasetStatements.code, datasetStatements.disclosedDate],
            set: fields,
          })
          .run();
      }
    });

    try {
      transaction();
      if (this.isDebugMode) {
        console.log(`[DATABASE] Successfully completed statements transaction for ${stockCode}`);
      }
    } catch (error) {
      if (this.isDebugMode) {
        console.log(`[DATABASE] Failed statements transaction for ${stockCode}:`, error);
      }
      throw new DatabaseError(
        `Failed to insert statements for ${stockCode}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  getStatementsData(stockCode?: string, dateRange?: DateRange): StatementsData[] {
    try {
      const conditions: ReturnType<typeof eq>[] = [];

      if (stockCode) {
        conditions.push(eq(datasetStatements.code, normalizeStockCode(stockCode)));
      }

      if (dateRange) {
        const { from, to } = dateRangeToISO(dateRange);
        conditions.push(gte(datasetStatements.disclosedDate, from));
        conditions.push(lte(datasetStatements.disclosedDate, to));
      }

      const results =
        conditions.length > 0
          ? this.db
              .select()
              .from(datasetStatements)
              .where(and(...conditions))
              .orderBy(sql`code, disclosed_date DESC`)
              .all()
          : this.db.select().from(datasetStatements).orderBy(sql`code, disclosed_date DESC`).all();

      return results.map((row) => ({
        code: row.code,
        disclosedDate: new Date(row.disclosedDate),
        earningsPerShare: row.earningsPerShare,
        profit: row.profit,
        equity: row.equity,
        typeOfCurrentPeriod: row.typeOfCurrentPeriod ?? '',
        typeOfDocument: row.typeOfDocument ?? '',
        nextYearForecastEarningsPerShare: row.nextYearForecastEarningsPerShare,
        // Extended financial metrics (added 2026-01)
        bps: row.bps ?? null,
        sales: row.sales ?? null,
        operatingProfit: row.operatingProfit ?? null,
        ordinaryProfit: row.ordinaryProfit ?? null,
        operatingCashFlow: row.operatingCashFlow ?? null,
        dividendFY: row.dividendFY ?? null,
        forecastEps: row.forecastEps ?? null,
        // Cash flow extended metrics (added 2026-01)
        investingCashFlow: row.investingCashFlow ?? null,
        financingCashFlow: row.financingCashFlow ?? null,
        cashAndEquivalents: row.cashAndEquivalents ?? null,
        totalAssets: row.totalAssets ?? null,
        sharesOutstanding: row.sharesOutstanding ?? null,
        treasuryShares: row.treasuryShares ?? null,
      }));
    } catch (error) {
      throw new DatabaseError(
        `Failed to retrieve statements data${stockCode ? ` for ${stockCode}` : ''}`,
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  // ===== STATISTICS =====

  async getDatasetStats(): Promise<DatasetStats> {
    try {
      const totalStocksResult = this.db.select({ count: count() }).from(datasetStocks).get();
      const totalQuotesResult = this.db.select({ count: count() }).from(datasetDailyQuotes).get();
      const dateRangeResult = this.db
        .select({
          minDate: min(datasetDailyQuotes.date),
          maxDate: max(datasetDailyQuotes.date),
        })
        .from(datasetDailyQuotes)
        .get();

      const marketsResult = this.db
        .selectDistinct({ marketName: datasetStocks.marketName })
        .from(datasetStocks)
        .orderBy(datasetStocks.marketName)
        .all();

      const hasMarginDataResult = this.db.select({ count: count() }).from(datasetMarginData).limit(1).get();
      const hasTopixResult = this.db.select({ count: count() }).from(datasetTopix).limit(1).get();
      const hasSectorResult = this.db.select({ count: count() }).from(datasetIndices).limit(1).get();
      const hasStatementsResult = this.db.select({ count: count() }).from(datasetStatements).limit(1).get();

      return {
        totalStocks: totalStocksResult?.count ?? 0,
        totalQuotes: totalQuotesResult?.count ?? 0,
        dateRange: {
          from: dateRangeResult?.minDate ? new Date(dateRangeResult.minDate) : new Date(),
          to: dateRangeResult?.maxDate ? new Date(dateRangeResult.maxDate) : new Date(),
        },
        markets: marketsResult.map((r) => r.marketName),
        hasMarginData: (hasMarginDataResult?.count ?? 0) > 0,
        hasTOPIXData: (hasTopixResult?.count ?? 0) > 0,
        hasSectorData: (hasSectorResult?.count ?? 0) > 0,
        hasStatementsData: (hasStatementsResult?.count ?? 0) > 0,
        databaseSize: this.getDatabaseSize(),
        lastUpdated: new Date(),
      };
    } catch (error) {
      throw new DatabaseError(
        'Failed to get dataset statistics',
        error instanceof Error ? error : new Error(String(error))
      );
    }
  }

  /**
   * Get statements field coverage statistics
   * Returns the count of non-null values for each financial metric field
   */
  getStatementsFieldCoverage(): StatementsFieldCoverage {
    const schemaInfo = this.getStatementsSchemaInfo();
    const coreFields = this.queryCoreFieldCounts();

    const baseCoverage: StatementsFieldCoverage = {
      ...coreFields,
      ...schemaInfo,
      bps: 0,
      sales: 0,
      operatingProfit: 0,
      ordinaryProfit: 0,
      operatingCashFlow: 0,
      dividendFY: 0,
      forecastEps: 0,
      investingCashFlow: 0,
      financingCashFlow: 0,
      cashAndEquivalents: 0,
      totalAssets: 0,
      sharesOutstanding: 0,
      treasuryShares: 0,
    };

    if (!schemaInfo.hasExtendedFields) {
      return baseCoverage;
    }

    const extendedFields = this.queryExtendedFieldCounts(schemaInfo.hasCashFlowFields);
    return { ...baseCoverage, ...extendedFields };
  }

  /**
   * Check which optional field groups exist in the statements schema
   */
  private getStatementsSchemaInfo(): { hasExtendedFields: boolean; hasCashFlowFields: boolean } {
    const columns = this.sqlite.query('PRAGMA table_info(statements)').all() as { name: string }[];
    const columnNames = new Set(columns.map((c) => c.name));

    const cashFlowColumns = [
      'investing_cash_flow',
      'financing_cash_flow',
      'cash_and_equivalents',
      'total_assets',
      'shares_outstanding',
      'treasury_shares',
    ];

    return {
      hasExtendedFields: columnNames.has('bps'),
      hasCashFlowFields: cashFlowColumns.every((col) => columnNames.has(col)),
    };
  }

  /**
   * Query core field counts and period type totals
   */
  private queryCoreFieldCounts(): {
    total: number;
    totalFY: number;
    totalHalf: number;
    earningsPerShare: number;
    profit: number;
    equity: number;
    nextYearForecastEps: number;
  } {
    const result = this.sqlite
      .query(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN type_of_current_period = 'FY' THEN 1 ELSE 0 END) as totalFY,
          SUM(CASE WHEN type_of_current_period IN ('FY', '2Q') THEN 1 ELSE 0 END) as totalHalf,
          COUNT(earnings_per_share) as earningsPerShare,
          COUNT(profit) as profit,
          COUNT(equity) as equity,
          COUNT(next_year_forecast_earnings_per_share) as nextYearForecastEps
        FROM statements
      `)
      .get() as {
      total: number;
      totalFY: number;
      totalHalf: number;
      earningsPerShare: number;
      profit: number;
      equity: number;
      nextYearForecastEps: number;
    } | null;

    return {
      total: result?.total ?? 0,
      totalFY: result?.totalFY ?? 0,
      totalHalf: result?.totalHalf ?? 0,
      earningsPerShare: result?.earningsPerShare ?? 0,
      profit: result?.profit ?? 0,
      equity: result?.equity ?? 0,
      nextYearForecastEps: result?.nextYearForecastEps ?? 0,
    };
  }

  /**
   * Query extended and cash flow field counts in a single query
   */
  private queryExtendedFieldCounts(includeCashFlow: boolean): Partial<StatementsFieldCoverage> {
    const cashFlowSelect = includeCashFlow
      ? `,
            COUNT(investing_cash_flow) as investingCashFlow,
            COUNT(financing_cash_flow) as financingCashFlow,
            COUNT(cash_and_equivalents) as cashAndEquivalents,
            COUNT(total_assets) as totalAssets,
            COUNT(shares_outstanding) as sharesOutstanding,
            COUNT(treasury_shares) as treasuryShares`
      : '';

    try {
      const result = this.sqlite
        .query(`
          SELECT
            COUNT(bps) as bps,
            COUNT(sales) as sales,
            COUNT(operating_profit) as operatingProfit,
            COUNT(ordinary_profit) as ordinaryProfit,
            COUNT(operating_cash_flow) as operatingCashFlow,
            COUNT(dividend_fy) as dividendFY,
            COUNT(forecast_eps) as forecastEps
            ${cashFlowSelect}
          FROM statements
        `)
        .get() as Record<string, number>;

      return {
        bps: result.bps,
        sales: result.sales,
        operatingProfit: result.operatingProfit,
        ordinaryProfit: result.ordinaryProfit,
        operatingCashFlow: result.operatingCashFlow,
        dividendFY: result.dividendFY,
        forecastEps: result.forecastEps,
        ...(includeCashFlow && {
          investingCashFlow: result.investingCashFlow,
          financingCashFlow: result.financingCashFlow,
          cashAndEquivalents: result.cashAndEquivalents,
          totalAssets: result.totalAssets,
          sharesOutstanding: result.sharesOutstanding,
          treasuryShares: result.treasuryShares,
        }),
      };
    } catch {
      return {};
    }
  }

  private getDatabaseSize(): number {
    try {
      const result = this.sqlite
        .query('SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()')
        .get() as { size: number };
      return result.size;
    } catch {
      return 0;
    }
  }

  // ===== VALIDATION QUERIES =====

  /**
   * Get date gaps between TOPIX trading calendar and stock data
   * @param snapshotDate - Optional snapshot date (YYYY-MM-DD format) to exclude dates after this date
   */
  getDateGaps(snapshotDate?: string): { totalTradingDays: number; missingDatesCount: number } {
    try {
      const dateFilter = snapshotDate ? 'WHERE date <= ?' : '';
      const params = snapshotDate ? [snapshotDate] : [];

      const tradingDaysResult = this.sqlite
        .query(`SELECT COUNT(DISTINCT date) as count FROM topix_data ${dateFilter}`)
        .get(...params) as { count: number } | undefined;

      const missingResult = this.sqlite
        .query(`
          SELECT COUNT(*) as count FROM (
            SELECT DISTINCT date FROM topix_data ${dateFilter}
            EXCEPT
            SELECT DISTINCT date FROM stock_data ${dateFilter}
          )
        `)
        .get(...params, ...params) as { count: number } | undefined;

      return {
        totalTradingDays: tradingDaysResult?.count ?? 0,
        missingDatesCount: missingResult?.count ?? 0,
      };
    } catch {
      return { totalTradingDays: 0, missingDatesCount: 0 };
    }
  }

  /**
   * Get foreign key integrity issues (orphan records referencing non-existent stocks)
   */
  getFKIntegrityIssues(): {
    stockDataOrphans: number;
    marginDataOrphans: number;
    statementsOrphans: number;
  } {
    try {
      const stockDataOrphans = this.sqlite
        .query(`
          SELECT COUNT(DISTINCT sd.code) as count FROM stock_data sd
          LEFT JOIN stocks s ON sd.code = s.code
          WHERE s.code IS NULL
        `)
        .get() as { count: number } | undefined;

      const marginDataOrphans = this.sqlite
        .query(`
          SELECT COUNT(DISTINCT md.code) as count FROM margin_data md
          LEFT JOIN stocks s ON md.code = s.code
          WHERE s.code IS NULL
        `)
        .get() as { count: number } | undefined;

      const statementsOrphans = this.sqlite
        .query(`
          SELECT COUNT(DISTINCT st.code) as count FROM statements st
          LEFT JOIN stocks s ON st.code = s.code
          WHERE s.code IS NULL
        `)
        .get() as { count: number } | undefined;

      return {
        stockDataOrphans: stockDataOrphans?.count ?? 0,
        marginDataOrphans: marginDataOrphans?.count ?? 0,
        statementsOrphans: statementsOrphans?.count ?? 0,
      };
    } catch {
      return { stockDataOrphans: 0, marginDataOrphans: 0, statementsOrphans: 0 };
    }
  }

  /**
   * Get count of stocks without any quote data
   */
  getOrphanStocksCount(): number {
    try {
      const result = this.sqlite
        .query(`
          SELECT COUNT(DISTINCT s.code) as count FROM stocks s
          LEFT JOIN stock_data sd ON s.code = sd.code
          WHERE sd.code IS NULL
        `)
        .get() as { count: number } | undefined;

      return result?.count ?? 0;
    } catch {
      return 0;
    }
  }

  /**
   * Get preset name from metadata (for stock count validation)
   */
  getPreset(): string | null {
    return this.getMetadata(DATASET_METADATA_KEYS.PRESET);
  }

  // ===== RESUME SUPPORT =====

  /**
   * Get stock codes missing data from a related table
   */
  private getStocksWithMissingData(table: string, joinColumn: string): string[] {
    try {
      const results = this.sqlite
        .query(`
          SELECT s.code FROM stocks s
          LEFT JOIN ${table} t ON s.code = t.${joinColumn}
          WHERE t.${joinColumn} IS NULL
          ORDER BY s.code
        `)
        .all() as { code: string }[];
      return results.map((r) => r.code);
    } catch {
      return [];
    }
  }

  /**
   * Get stock codes that are missing quote data
   */
  getStocksWithMissingQuotes(): string[] {
    return this.getStocksWithMissingData('stock_data', 'code');
  }

  /**
   * Get stock codes that are missing statements data
   */
  getStocksWithMissingStatements(): string[] {
    return this.getStocksWithMissingData('statements', 'code');
  }

  /**
   * Get stock codes that are missing margin data
   */
  getStocksWithMissingMargin(): string[] {
    return this.getStocksWithMissingData('margin_data', 'code');
  }

  /**
   * Count stocks missing data from a related table
   */
  private countStocksWithMissingData(table: string, joinColumn: string): number {
    const result = this.sqlite
      .query(`
        SELECT COUNT(*) as count FROM stocks s
        LEFT JOIN ${table} t ON s.code = t.${joinColumn}
        WHERE t.${joinColumn} IS NULL
      `)
      .get() as { count: number };
    return result.count;
  }

  /**
   * Get resume status summary
   */
  getResumeStatus(): {
    totalStocks: number;
    missingQuotes: number;
    missingStatements: number;
    missingMargin: number;
  } {
    try {
      const totalResult = this.sqlite.query('SELECT COUNT(*) as count FROM stocks').get() as { count: number };
      return {
        totalStocks: totalResult.count,
        missingQuotes: this.countStocksWithMissingData('stock_data', 'code'),
        missingStatements: this.countStocksWithMissingData('statements', 'code'),
        missingMargin: this.countStocksWithMissingData('margin_data', 'code'),
      };
    } catch {
      return { totalStocks: 0, missingQuotes: 0, missingStatements: 0, missingMargin: 0 };
    }
  }

  /**
   * Check if a table has any data
   */
  private hasTableData(table: string): boolean {
    try {
      const result = this.sqlite.query(`SELECT COUNT(*) as count FROM ${table}`).get() as { count: number };
      return result.count > 0;
    } catch {
      return false;
    }
  }

  /**
   * Check if TOPIX data exists
   */
  hasTopixData(): boolean {
    return this.hasTableData('topix_data');
  }

  /**
   * Check if sector indices data exists
   */
  hasSectorIndicesData(): boolean {
    return this.hasTableData('indices_data');
  }

  // ===== TRANSACTION UTILITIES =====

  withTransaction<T>(operation: () => T): T {
    const transaction = this.sqlite.transaction(operation);
    return transaction();
  }

  // ===== CLEANUP =====

  async close(): Promise<void> {
    try {
      // Force WAL checkpoint before closing
      this.sqlite.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch {
      // Ignore checkpoint errors
    }
    this.sqlite.close();

    if (this.isDebugMode) {
      console.log('[DrizzleDatasetDatabase] Database connection closed');
    }
  }

  getSchemaVersion(): string {
    return this.getMetadata('schema_version') ?? 'unknown';
  }

  // ===== MAPPING HELPERS =====

  private mapStockRow(row: typeof datasetStocks.$inferSelect): StockInfo {
    return {
      code: row.code,
      companyName: row.companyName,
      companyNameEnglish: row.companyNameEnglish ?? '',
      marketCode: row.marketCode,
      marketName: row.marketName,
      sector17Code: row.sector17Code ?? '',
      sector17Name: row.sector17Name ?? '',
      sector33Code: row.sector33Code,
      sector33Name: row.sector33Name,
      scaleCategory: row.scaleCategory ?? '',
      listedDate: row.listedDate ? new Date(row.listedDate) : new Date('1970-01-01'),
    };
  }
}
