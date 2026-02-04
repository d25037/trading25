/**
 * Tests for DrizzlePortfolioDatabase
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  PortfolioItemNotFoundError,
  PortfolioNotFoundError,
  StockNotFoundInPortfolioError,
  ValidationError,
} from '../portfolio/types';
import { DrizzlePortfolioDatabase } from './drizzle-portfolio-database';

describe('DrizzlePortfolioDatabase', () => {
  let db: DrizzlePortfolioDatabase;
  let testDbPath: string;
  let testDir: string;

  beforeEach(() => {
    testDir = join(tmpdir(), `drizzle-portfolio-test-${Date.now()}`);
    mkdirSync(testDir, { recursive: true });
    testDbPath = join(testDir, 'test-portfolio.db');
    db = new DrizzlePortfolioDatabase(testDbPath);
  });

  afterEach(() => {
    db.close();
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('Portfolio CRUD', () => {
    test('creates a portfolio', () => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      expect(portfolio.name).toBe('Test Portfolio');
      expect(portfolio.id).toBeGreaterThan(0);
      expect(portfolio.createdAt).toBeInstanceOf(Date);
    });

    test('creates portfolio with description', () => {
      const portfolio = db.createPortfolio({
        name: 'Test Portfolio',
        description: 'A test description',
      });
      expect(portfolio.description).toBe('A test description');
    });

    test('throws on duplicate portfolio name', () => {
      db.createPortfolio({ name: 'Test Portfolio' });
      expect(() => db.createPortfolio({ name: 'Test Portfolio' })).toThrow(DuplicatePortfolioNameError);
    });

    test('gets portfolio by id', () => {
      const created = db.createPortfolio({ name: 'Test Portfolio' });
      const retrieved = db.getPortfolio(created.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.name).toBe('Test Portfolio');
    });

    test('gets portfolio by name', () => {
      db.createPortfolio({ name: 'Test Portfolio' });
      const retrieved = db.getPortfolioByName('Test Portfolio');
      expect(retrieved).not.toBeNull();
      expect(retrieved?.name).toBe('Test Portfolio');
    });

    test('returns null for non-existent portfolio', () => {
      expect(db.getPortfolio(999)).toBeNull();
      expect(db.getPortfolioByName('Non-existent')).toBeNull();
    });

    test('lists all portfolios', () => {
      db.createPortfolio({ name: 'Portfolio 1' });
      db.createPortfolio({ name: 'Portfolio 2' });
      const portfolios = db.listPortfolios();
      expect(portfolios.length).toBe(2);
    });

    test('updates portfolio', () => {
      const created = db.createPortfolio({ name: 'Original Name' });
      const updated = db.updatePortfolio(created.id, { name: 'New Name' });
      expect(updated.name).toBe('New Name');
    });

    test('throws on update non-existent portfolio', () => {
      expect(() => db.updatePortfolio(999, { name: 'New Name' })).toThrow(PortfolioNotFoundError);
    });

    test('deletes portfolio', () => {
      const created = db.createPortfolio({ name: 'To Delete' });
      db.deletePortfolio(created.id);
      expect(db.getPortfolio(created.id)).toBeNull();
    });

    test('throws on delete non-existent portfolio', () => {
      expect(() => db.deletePortfolio(999)).toThrow(PortfolioNotFoundError);
    });
  });

  describe('Portfolio Item CRUD', () => {
    let portfolioId: number;

    beforeEach(() => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      portfolioId = portfolio.id;
    });

    test('adds item to portfolio', () => {
      const item = db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      expect(item.code).toBe('7203');
      expect(item.quantity).toBe(100);
    });

    test('normalizes 5-digit code to 4-digit', () => {
      const item = db.addItem({
        portfolioId,
        code: '72030', // JQuants format
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      expect(item.code).toBe('7203'); // Normalized to 4-digit
    });

    test('throws on invalid stock code', () => {
      expect(() =>
        db.addItem({
          portfolioId,
          code: '12345', // Invalid: 5 digits not ending in 0
          companyName: 'Test',
          quantity: 100,
          purchasePrice: 100,
          purchaseDate: new Date(),
        })
      ).toThrow(InvalidStockCodeError);
    });

    test('throws on duplicate stock in portfolio', () => {
      db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      expect(() =>
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'Toyota Motor',
          quantity: 50,
          purchasePrice: 2600,
          purchaseDate: new Date('2024-02-01'),
        })
      ).toThrow(DuplicateStockError);
    });

    test('throws on invalid quantity', () => {
      expect(() =>
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'Toyota Motor',
          quantity: 0,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        })
      ).toThrow(ValidationError);
    });

    test('throws on invalid purchase price', () => {
      expect(() =>
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'Toyota Motor',
          quantity: 100,
          purchasePrice: -100,
          purchaseDate: new Date(),
        })
      ).toThrow(ValidationError);
    });

    test('gets item by id', () => {
      const created = db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      const retrieved = db.getItem(created.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.code).toBe('7203');
    });

    test('gets item by code', () => {
      db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      const retrieved = db.getItemByCode(portfolioId, '7203');
      expect(retrieved).not.toBeNull();
      expect(retrieved?.companyName).toBe('Toyota Motor');
    });

    test('lists items in portfolio', () => {
      db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      db.addItem({
        portfolioId,
        code: '6758',
        companyName: 'Sony Group',
        quantity: 50,
        purchasePrice: 12000,
        purchaseDate: new Date('2024-02-01'),
      });
      const items = db.listItems(portfolioId);
      expect(items.length).toBe(2);
    });

    test('updates item', () => {
      const created = db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      const updated = db.updateItem(created.id, { quantity: 200 });
      expect(updated.quantity).toBe(200);
    });

    test('throws on update non-existent item', () => {
      expect(() => db.updateItem(999, { quantity: 100 })).toThrow(PortfolioItemNotFoundError);
    });

    test('deletes item', () => {
      const created = db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      db.deleteItem(created.id);
      expect(db.getItem(created.id)).toBeNull();
    });

    test('deletes item by code', () => {
      db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      const deleted = db.deleteItemByCode(portfolioId, '7203');
      expect(deleted.code).toBe('7203');
      expect(db.getItemByCode(portfolioId, '7203')).toBeNull();
    });

    test('throws on delete non-existent item by code', () => {
      expect(() => db.deleteItemByCode(portfolioId, '9999')).toThrow(StockNotFoundInPortfolioError);
    });
  });

  describe('Aggregations', () => {
    test('gets portfolio with items', () => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      db.addItem({
        portfolioId: portfolio.id,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });

      const withItems = db.getPortfolioWithItems(portfolio.id);
      expect(withItems).not.toBeNull();
      expect(withItems?.items.length).toBe(1);
      expect(withItems?.items[0]?.code).toBe('7203');
    });

    test('gets portfolio summary', () => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      db.addItem({
        portfolioId: portfolio.id,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });
      db.addItem({
        portfolioId: portfolio.id,
        code: '6758',
        companyName: 'Sony Group',
        quantity: 50,
        purchasePrice: 12000,
        purchaseDate: new Date('2024-02-01'),
      });

      const summary = db.getPortfolioSummary(portfolio.id);
      expect(summary).not.toBeNull();
      expect(summary?.stockCount).toBe(2);
      expect(summary?.totalShares).toBe(150);
    });

    test('lists portfolio summaries', () => {
      const p1 = db.createPortfolio({ name: 'Portfolio 1' });
      const p2 = db.createPortfolio({ name: 'Portfolio 2' });

      db.addItem({
        portfolioId: p1.id,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });

      const summaries = db.listPortfolioSummaries();
      expect(summaries.length).toBe(2);

      const p1Summary = summaries.find((s) => s.id === p1.id);
      const p2Summary = summaries.find((s) => s.id === p2.id);
      expect(p1Summary?.stockCount).toBe(1);
      expect(p2Summary?.stockCount).toBe(0);
    });
  });

  describe('Stock Code Normalization', () => {
    let portfolioId: number;

    beforeEach(() => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      portfolioId = portfolio.id;
    });

    test('accepts and normalizes JQuants 5-digit codes', () => {
      const item = db.addItem({
        portfolioId,
        code: '72030', // JQuants format with trailing 0
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });

      expect(item.code).toBe('7203'); // Should be normalized
    });

    test('can retrieve item using both 4-digit and 5-digit codes', () => {
      db.addItem({
        portfolioId,
        code: '7203',
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });

      // Should find using 4-digit code
      const item4 = db.getItemByCode(portfolioId, '7203');
      expect(item4).not.toBeNull();

      // Should also find using 5-digit code (normalized internally)
      const item5 = db.getItemByCode(portfolioId, '72030');
      expect(item5).not.toBeNull();

      // Both should be the same item
      expect(item4?.id).toBe(item5?.id);
    });

    test('prevents duplicate when adding same stock with different code formats', () => {
      db.addItem({
        portfolioId,
        code: '7203', // 4-digit
        companyName: 'Toyota Motor',
        quantity: 100,
        purchasePrice: 2500,
        purchaseDate: new Date('2024-01-01'),
      });

      // Should throw when trying to add with 5-digit format
      expect(() =>
        db.addItem({
          portfolioId,
          code: '72030', // 5-digit - same stock
          companyName: 'Toyota Motor',
          quantity: 50,
          purchasePrice: 2600,
          purchaseDate: new Date('2024-02-01'),
        })
      ).toThrow(DuplicateStockError);
    });
  });

  describe('Utility', () => {
    test('returns schema version', () => {
      const version = db.getSchemaVersion();
      expect(version).toMatch(/^\d+\.\d+\.\d+$/);
    });

    test('validates stock codes', () => {
      expect(db.validateStockCode('7203')).toBe(true);
      expect(db.validateStockCode('285A')).toBe(true);
      expect(db.validateStockCode('72030')).toBe(false); // 5-digit
      expect(db.validateStockCode('123')).toBe(false); // 3-digit
    });
  });
});
