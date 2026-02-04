/**
 * Drizzle-based Portfolio Database
 *
 * Type-safe portfolio management using Drizzle ORM.
 * Replaces the raw SQL implementation in portfolio/database.ts
 */

import { Database } from 'bun:sqlite';
import { and, eq, sql } from 'drizzle-orm';
import type { BunSQLiteDatabase } from 'drizzle-orm/bun-sqlite';
import { drizzle } from 'drizzle-orm/bun-sqlite';
import {
  type CreatePortfolioInput,
  type CreatePortfolioItemInput,
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  type Portfolio,
  PortfolioError,
  type PortfolioItem,
  PortfolioItemNotFoundError,
  PortfolioNotFoundError,
  type PortfolioSummary,
  type PortfolioWithItems,
  StockNotFoundInPortfolioError,
  type UpdatePortfolioInput,
  type UpdatePortfolioItemInput,
  ValidationError,
} from '../portfolio/types';
import { isValidStockCode, normalizeStockCode } from './columns/stock-code';
import { PORTFOLIO_SCHEMA_VERSION, portfolioItems, portfolioMetadata, portfolios } from './schema/portfolio-schema';

/**
 * Drizzle-based Portfolio Database
 * Provides type-safe database operations with automatic stock code normalization
 */
export class DrizzlePortfolioDatabase {
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

    // Enable foreign keys for cascade operations
    this.sqlite.exec('PRAGMA foreign_keys = ON');

    // Create tables using raw SQL (Drizzle push would be used in production)
    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS portfolio_metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS portfolios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    this.sqlite.exec(`
      CREATE TABLE IF NOT EXISTS portfolio_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER NOT NULL,
        code TEXT NOT NULL,
        company_name TEXT NOT NULL,
        quantity INTEGER NOT NULL CHECK(quantity > 0),
        purchase_price REAL NOT NULL CHECK(purchase_price > 0),
        purchase_date DATE NOT NULL,
        account TEXT,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE,
        UNIQUE(portfolio_id, code)
      );
    `);

    // Create indexes
    this.sqlite.exec(`
      CREATE INDEX IF NOT EXISTS idx_portfolio_items_portfolio_id
        ON portfolio_items(portfolio_id);
      CREATE INDEX IF NOT EXISTS idx_portfolio_items_code
        ON portfolio_items(code);
      CREATE INDEX IF NOT EXISTS idx_portfolio_items_purchase_date
        ON portfolio_items(purchase_date);
    `);

    // Store schema version
    this.setMetadata('schema_version', PORTFOLIO_SCHEMA_VERSION);
  }

  // ===== METADATA MANAGEMENT =====

  private getMetadata(key: string): string | null {
    const result = this.db.select().from(portfolioMetadata).where(eq(portfolioMetadata.key, key)).get();
    return result?.value ?? null;
  }

  private setMetadata(key: string, value: string): void {
    this.db
      .insert(portfolioMetadata)
      .values({ key, value, updatedAt: sql`CURRENT_TIMESTAMP` })
      .onConflictDoUpdate({
        target: portfolioMetadata.key,
        set: { value, updatedAt: sql`CURRENT_TIMESTAMP` },
      })
      .run();
  }

  // ===== VALIDATION =====

  validateStockCode(code: string): boolean {
    return isValidStockCode(code);
  }

  private assertValidStockCode(code: string): void {
    if (!this.validateStockCode(code)) {
      throw new InvalidStockCodeError(code);
    }
  }

  private assertPositiveNumber(value: number, fieldName: string): void {
    if (value <= 0) {
      throw new ValidationError(`${fieldName} must be greater than 0`);
    }
  }

  // ===== PORTFOLIO CRUD =====

  createPortfolio(input: CreatePortfolioInput): Portfolio {
    try {
      const result = this.db
        .insert(portfolios)
        .values({
          name: input.name,
          description: input.description ?? null,
          createdAt: sql`CURRENT_TIMESTAMP`,
          updatedAt: sql`CURRENT_TIMESTAMP`,
        })
        .returning()
        .get();

      if (this.debug) {
        console.log(`[DrizzlePortfolioDatabase] Created portfolio: ${input.name} (ID: ${result.id})`);
      }

      return this.mapPortfolioRow(result);
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicatePortfolioNameError(input.name);
      }
      throw error;
    }
  }

  getPortfolio(id: number): Portfolio | null {
    const result = this.db.select().from(portfolios).where(eq(portfolios.id, id)).get();
    return result ? this.mapPortfolioRow(result) : null;
  }

  getPortfolioByName(name: string): Portfolio | null {
    const result = this.db.select().from(portfolios).where(eq(portfolios.name, name)).get();
    return result ? this.mapPortfolioRow(result) : null;
  }

  listPortfolios(): Portfolio[] {
    const results = this.db.select().from(portfolios).orderBy(sql`created_at DESC`).all();
    return results.map((row) => this.mapPortfolioRow(row));
  }

  updatePortfolio(id: number, input: UpdatePortfolioInput): Portfolio {
    const existing = this.getPortfolio(id);
    if (!existing) {
      throw new PortfolioNotFoundError(id);
    }

    const updateData: Record<string, unknown> = { updatedAt: sql`CURRENT_TIMESTAMP` };
    if (input.name !== undefined) updateData.name = input.name;
    if (input.description !== undefined) updateData.description = input.description;

    if (Object.keys(updateData).length === 1) {
      return existing; // Only updatedAt, no actual changes
    }

    try {
      this.db.update(portfolios).set(updateData).where(eq(portfolios.id, id)).run();

      if (this.debug) {
        console.log(`[DrizzlePortfolioDatabase] Updated portfolio ID: ${id}`);
      }

      const updated = this.getPortfolio(id);
      if (!updated) {
        throw new PortfolioError('Failed to update portfolio');
      }
      return updated;
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicatePortfolioNameError(input.name ?? '');
      }
      throw error;
    }
  }

  deletePortfolio(id: number): void {
    const existing = this.getPortfolio(id);
    if (!existing) {
      throw new PortfolioNotFoundError(id);
    }

    this.db.delete(portfolios).where(eq(portfolios.id, id)).run();

    if (this.debug) {
      console.log(`[DrizzlePortfolioDatabase] Deleted portfolio ID: ${id} (${existing.name})`);
    }
  }

  // ===== PORTFOLIO ITEM CRUD =====

  addItem(input: CreatePortfolioItemInput): PortfolioItem {
    // Normalize stock code to 4-digit format
    const normalizedCode = normalizeStockCode(input.code);
    this.assertValidStockCode(normalizedCode);
    this.assertPositiveNumber(input.quantity, 'quantity');
    this.assertPositiveNumber(input.purchasePrice, 'purchase_price');

    const portfolio = this.getPortfolio(input.portfolioId);
    if (!portfolio) {
      throw new PortfolioNotFoundError(input.portfolioId);
    }

    try {
      const purchaseDate = input.purchaseDate.toISOString().split('T')[0] as string;
      const result = this.db
        .insert(portfolioItems)
        .values({
          portfolioId: input.portfolioId,
          code: normalizedCode,
          companyName: input.companyName,
          quantity: input.quantity,
          purchasePrice: input.purchasePrice,
          purchaseDate,
          account: input.account ?? null,
          notes: input.notes ?? null,
          createdAt: sql`CURRENT_TIMESTAMP`,
          updatedAt: sql`CURRENT_TIMESTAMP`,
        })
        .returning()
        .get();

      if (this.debug) {
        console.log(`[DrizzlePortfolioDatabase] Added item: ${normalizedCode} to portfolio ${input.portfolioId}`);
      }

      return this.mapPortfolioItemRow(result);
    } catch (error) {
      if (error instanceof Error && error.message.includes('UNIQUE constraint')) {
        throw new DuplicateStockError(normalizedCode, input.portfolioId);
      }
      throw error;
    }
  }

  getItem(id: number): PortfolioItem | null {
    const result = this.db.select().from(portfolioItems).where(eq(portfolioItems.id, id)).get();
    return result ? this.mapPortfolioItemRow(result) : null;
  }

  listItems(portfolioId: number): PortfolioItem[] {
    const results = this.db
      .select()
      .from(portfolioItems)
      .where(eq(portfolioItems.portfolioId, portfolioId))
      .orderBy(sql`purchase_date DESC`)
      .all();
    return results.map((row) => this.mapPortfolioItemRow(row));
  }

  updateItem(id: number, input: UpdatePortfolioItemInput): PortfolioItem {
    const existing = this.getItem(id);
    if (!existing) {
      throw new PortfolioItemNotFoundError(id);
    }

    const updateData: Record<string, unknown> = { updatedAt: sql`CURRENT_TIMESTAMP` };

    if (input.quantity !== undefined) {
      this.assertPositiveNumber(input.quantity, 'quantity');
      updateData.quantity = input.quantity;
    }
    if (input.purchasePrice !== undefined) {
      this.assertPositiveNumber(input.purchasePrice, 'purchase_price');
      updateData.purchasePrice = input.purchasePrice;
    }
    if (input.purchaseDate !== undefined) {
      updateData.purchaseDate = input.purchaseDate.toISOString().split('T')[0];
    }
    if (input.account !== undefined) updateData.account = input.account;
    if (input.notes !== undefined) updateData.notes = input.notes;

    if (Object.keys(updateData).length === 1) {
      return existing;
    }

    this.db.update(portfolioItems).set(updateData).where(eq(portfolioItems.id, id)).run();

    if (this.debug) {
      console.log(`[DrizzlePortfolioDatabase] Updated item ID: ${id}`);
    }

    const updated = this.getItem(id);
    if (!updated) {
      throw new PortfolioError('Failed to update item');
    }
    return updated;
  }

  deleteItem(id: number): void {
    const existing = this.getItem(id);
    if (!existing) {
      throw new PortfolioItemNotFoundError(id);
    }

    this.db.delete(portfolioItems).where(eq(portfolioItems.id, id)).run();

    if (this.debug) {
      console.log(`[DrizzlePortfolioDatabase] Deleted item ID: ${id} (${existing.code})`);
    }
  }

  getItemByCode(portfolioId: number, code: string): PortfolioItem | null {
    const normalizedCode = normalizeStockCode(code);
    const result = this.db
      .select()
      .from(portfolioItems)
      .where(and(eq(portfolioItems.portfolioId, portfolioId), eq(portfolioItems.code, normalizedCode)))
      .get();
    return result ? this.mapPortfolioItemRow(result) : null;
  }

  updateItemByCode(portfolioId: number, code: string, input: UpdatePortfolioItemInput): PortfolioItem {
    const normalizedCode = normalizeStockCode(code);
    const existing = this.getItemByCode(portfolioId, normalizedCode);
    if (!existing) {
      throw new StockNotFoundInPortfolioError(normalizedCode, portfolioId);
    }

    const updateData: Record<string, unknown> = { updatedAt: sql`CURRENT_TIMESTAMP` };

    if (input.quantity !== undefined) {
      this.assertPositiveNumber(input.quantity, 'quantity');
      updateData.quantity = input.quantity;
    }
    if (input.purchasePrice !== undefined) {
      this.assertPositiveNumber(input.purchasePrice, 'purchase_price');
      updateData.purchasePrice = input.purchasePrice;
    }
    if (input.purchaseDate !== undefined) {
      updateData.purchaseDate = input.purchaseDate.toISOString().split('T')[0];
    }
    if (input.account !== undefined) updateData.account = input.account;
    if (input.notes !== undefined) updateData.notes = input.notes;

    if (Object.keys(updateData).length === 1) {
      return existing;
    }

    this.db
      .update(portfolioItems)
      .set(updateData)
      .where(and(eq(portfolioItems.portfolioId, portfolioId), eq(portfolioItems.code, normalizedCode)))
      .run();

    if (this.debug) {
      console.log(`[DrizzlePortfolioDatabase] Updated item: ${normalizedCode} in portfolio ${portfolioId}`);
    }

    const updated = this.getItemByCode(portfolioId, normalizedCode);
    if (!updated) {
      throw new PortfolioError('Failed to update item');
    }
    return updated;
  }

  deleteItemByCode(portfolioId: number, code: string): PortfolioItem {
    const normalizedCode = normalizeStockCode(code);
    const existing = this.getItemByCode(portfolioId, normalizedCode);
    if (!existing) {
      throw new StockNotFoundInPortfolioError(normalizedCode, portfolioId);
    }

    this.db
      .delete(portfolioItems)
      .where(and(eq(portfolioItems.portfolioId, portfolioId), eq(portfolioItems.code, normalizedCode)))
      .run();

    if (this.debug) {
      console.log(`[DrizzlePortfolioDatabase] Deleted item: ${normalizedCode} from portfolio ${portfolioId}`);
    }

    return existing;
  }

  // ===== AGGREGATIONS =====

  getPortfolioWithItems(id: number): PortfolioWithItems | null {
    const portfolio = this.getPortfolio(id);
    if (!portfolio) return null;

    const items = this.listItems(id);
    return { ...portfolio, items };
  }

  getPortfolioSummary(id: number): PortfolioSummary | null {
    const portfolio = this.getPortfolio(id);
    if (!portfolio) return null;

    const result = this.db
      .select({
        stockCount: sql<number>`COUNT(*)`,
        totalShares: sql<number>`COALESCE(SUM(quantity), 0)`,
      })
      .from(portfolioItems)
      .where(eq(portfolioItems.portfolioId, id))
      .get();

    return {
      ...portfolio,
      stockCount: result?.stockCount ?? 0,
      totalShares: result?.totalShares ?? 0,
    };
  }

  listPortfolioSummaries(): PortfolioSummary[] {
    const allPortfolios = this.listPortfolios();
    return allPortfolios.map((portfolio) => {
      const summary = this.getPortfolioSummary(portfolio.id);
      if (!summary) {
        throw new PortfolioError(`Failed to get summary for portfolio ${portfolio.id}`);
      }
      return summary;
    });
  }

  // ===== UTILITY =====

  close(): void {
    // Force WAL checkpoint before closing to ensure data durability
    try {
      this.sqlite.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    } catch {
      // Ignore checkpoint errors on close
    }
    this.sqlite.close();
    if (this.debug) {
      console.log('[DrizzlePortfolioDatabase] Database connection closed');
    }
  }

  getSchemaVersion(): string {
    return this.getMetadata('schema_version') ?? 'unknown';
  }

  // ===== MAPPING HELPERS =====

  private mapPortfolioRow(row: typeof portfolios.$inferSelect): Portfolio {
    return {
      id: row.id,
      name: row.name,
      description: row.description ?? undefined,
      createdAt: row.createdAt ? new Date(row.createdAt) : new Date(),
      updatedAt: row.updatedAt ? new Date(row.updatedAt) : new Date(),
    };
  }

  private mapPortfolioItemRow(row: typeof portfolioItems.$inferSelect): PortfolioItem {
    return {
      id: row.id,
      portfolioId: row.portfolioId,
      code: row.code, // Already normalized by stockCode column type
      companyName: row.companyName,
      quantity: row.quantity,
      purchasePrice: row.purchasePrice,
      purchaseDate: new Date(row.purchaseDate),
      account: row.account ?? undefined,
      notes: row.notes ?? undefined,
      createdAt: row.createdAt ? new Date(row.createdAt) : new Date(),
      updatedAt: row.updatedAt ? new Date(row.updatedAt) : new Date(),
    };
  }
}
