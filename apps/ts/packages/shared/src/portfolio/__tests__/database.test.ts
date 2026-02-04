/**
 * Portfolio Database Tests
 * Unit tests for PortfolioDatabase class
 */

import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PortfolioDatabase } from '../index';
import {
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  PortfolioItemNotFoundError,
  PortfolioNotFoundError,
  ValidationError,
} from '../types';

// Use unique path for each test run to avoid conflicts during parallel test execution
const getTestDbPath = () =>
  path.join(os.tmpdir(), `test-portfolio-${Date.now()}-${Math.random().toString(36).slice(2)}.db`);

describe('PortfolioDatabase', () => {
  let db: PortfolioDatabase;
  let testDbPath: string;

  beforeEach(() => {
    // Generate unique path for this test
    testDbPath = getTestDbPath();

    // Clean up any existing test database (shouldn't exist with unique names)
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }

    db = new PortfolioDatabase(testDbPath, false);
  });

  afterEach(() => {
    if (db) {
      db.close();
    }

    // Clean up test database
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  describe('Database Initialization', () => {
    it('should initialize database with schema version', () => {
      // Verify database is functional by checking schema version
      // v1.1.0: Drizzle ORM migration with automatic stock code normalization
      const version = db.getSchemaVersion();
      expect(version).toBe('1.1.0');
    });
  });

  describe('Stock Code Validation', () => {
    it('should validate traditional 4-digit stock codes', () => {
      expect(db.validateStockCode('7203')).toBe(true);
      expect(db.validateStockCode('0001')).toBe(true);
      expect(db.validateStockCode('9999')).toBe(true);
    });

    it('should validate alphanumeric stock codes (2024+ format)', () => {
      expect(db.validateStockCode('285A')).toBe(true); // Kioxia Holdings
      expect(db.validateStockCode('130A')).toBe(true); // Alphanumeric
      expect(db.validateStockCode('1A0B')).toBe(true); // Both 2nd and 4th are letters
    });

    it('should reject invalid stock codes', () => {
      expect(db.validateStockCode('123')).toBe(false); // Too short
      expect(db.validateStockCode('12345')).toBe(false); // Too long (5 chars)
      expect(db.validateStockCode('285A5')).toBe(false); // 5 chars (preferred stock - not supported)
      expect(db.validateStockCode('ABC1')).toBe(false); // 1st position must be digit
      expect(db.validateStockCode('72A3')).toBe(false); // 3rd position must be digit
      expect(db.validateStockCode('7203a')).toBe(false); // Lowercase not allowed
      expect(db.validateStockCode('')).toBe(false); // Empty
    });
  });

  describe('Portfolio CRUD Operations', () => {
    describe('Create Portfolio', () => {
      it('should create a portfolio with name only', () => {
        const portfolio = db.createPortfolio({ name: '退職金運用' });

        expect(portfolio.id).toBeDefined();
        expect(portfolio.name).toBe('退職金運用');
        expect(portfolio.description).toBeUndefined();
        expect(portfolio.createdAt).toBeInstanceOf(Date);
        expect(portfolio.updatedAt).toBeInstanceOf(Date);
      });

      it('should create a portfolio with name and description', () => {
        const portfolio = db.createPortfolio({
          name: '成長株',
          description: 'High growth technology stocks',
        });

        expect(portfolio.name).toBe('成長株');
        expect(portfolio.description).toBe('High growth technology stocks');
      });

      it('should throw error for duplicate portfolio name', () => {
        db.createPortfolio({ name: '退職金運用' });

        expect(() => {
          db.createPortfolio({ name: '退職金運用' });
        }).toThrow(DuplicatePortfolioNameError);
      });
    });

    describe('Get Portfolio', () => {
      it('should get portfolio by ID', () => {
        const created = db.createPortfolio({ name: '退職金運用' });
        const portfolio = db.getPortfolio(created.id);

        expect(portfolio).toBeDefined();
        expect(portfolio?.id).toBe(created.id);
        expect(portfolio?.name).toBe('退職金運用');
      });

      it('should return null for non-existent portfolio ID', () => {
        const portfolio = db.getPortfolio(999);
        expect(portfolio).toBeNull();
      });

      it('should get portfolio by name', () => {
        db.createPortfolio({ name: '成長株' });
        const portfolio = db.getPortfolioByName('成長株');

        expect(portfolio).toBeDefined();
        expect(portfolio?.name).toBe('成長株');
      });

      it('should return null for non-existent portfolio name', () => {
        const portfolio = db.getPortfolioByName('Non-existent');
        expect(portfolio).toBeNull();
      });
    });

    describe('List Portfolios', () => {
      it('should return empty array when no portfolios exist', () => {
        const portfolios = db.listPortfolios();
        expect(portfolios).toHaveLength(0);
      });

      it('should list all portfolios', () => {
        db.createPortfolio({ name: '退職金運用' });
        db.createPortfolio({ name: '成長株' });
        db.createPortfolio({ name: '配当株' });

        const portfolios = db.listPortfolios();
        expect(portfolios).toHaveLength(3);

        const names = portfolios.map((p) => p.name);
        expect(names).toContain('退職金運用');
        expect(names).toContain('成長株');
        expect(names).toContain('配当株');
      });
    });

    describe('Update Portfolio', () => {
      it('should update portfolio name', () => {
        const portfolio = db.createPortfolio({ name: 'Old Name' });
        const updated = db.updatePortfolio(portfolio.id, { name: 'New Name' });

        expect(updated.name).toBe('New Name');
        expect(updated.id).toBe(portfolio.id);
      });

      it('should update portfolio description', () => {
        const portfolio = db.createPortfolio({ name: '成長株' });
        const updated = db.updatePortfolio(portfolio.id, {
          description: 'New description',
        });

        expect(updated.description).toBe('New description');
      });

      it('should update both name and description', () => {
        const portfolio = db.createPortfolio({ name: 'Old Name' });
        const updated = db.updatePortfolio(portfolio.id, {
          name: 'New Name',
          description: 'New description',
        });

        expect(updated.name).toBe('New Name');
        expect(updated.description).toBe('New description');
      });

      it('should return unchanged portfolio if no updates provided', () => {
        const portfolio = db.createPortfolio({ name: '成長株' });
        const updated = db.updatePortfolio(portfolio.id, {});

        expect(updated.name).toBe(portfolio.name);
      });

      it('should throw error for non-existent portfolio', () => {
        expect(() => {
          db.updatePortfolio(999, { name: 'New Name' });
        }).toThrow(PortfolioNotFoundError);
      });

      it('should throw error for duplicate name', () => {
        const portfolio1 = db.createPortfolio({ name: '退職金運用' });
        db.createPortfolio({ name: '成長株' });

        expect(() => {
          db.updatePortfolio(portfolio1.id, { name: '成長株' });
        }).toThrow(DuplicatePortfolioNameError);
      });
    });

    describe('Delete Portfolio', () => {
      it('should delete portfolio', () => {
        const portfolio = db.createPortfolio({ name: '退職金運用' });
        db.deletePortfolio(portfolio.id);

        const deleted = db.getPortfolio(portfolio.id);
        expect(deleted).toBeNull();
      });

      it('should throw error for non-existent portfolio', () => {
        expect(() => {
          db.deletePortfolio(999);
        }).toThrow(PortfolioNotFoundError);
      });

      it('should cascade delete portfolio items', () => {
        const portfolio = db.createPortfolio({ name: '退職金運用' });
        db.addItem({
          portfolioId: portfolio.id,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date('2024-01-15'),
        });

        db.deletePortfolio(portfolio.id);

        const items = db.listItems(portfolio.id);
        expect(items).toHaveLength(0);
      });
    });
  });

  describe('Portfolio Item CRUD Operations', () => {
    let portfolioId: number;

    beforeEach(() => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      portfolioId = portfolio.id;
    });

    describe('Add Item', () => {
      it('should add item with all fields', () => {
        const item = db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date('2024-01-15'),
          account: 'SBI証券',
          notes: '配当株として購入',
        });

        expect(item.id).toBeDefined();
        expect(item.portfolioId).toBe(portfolioId);
        expect(item.code).toBe('7203');
        expect(item.companyName).toBe('トヨタ自動車');
        expect(item.quantity).toBe(100);
        expect(item.purchasePrice).toBe(2500);
        expect(item.purchaseDate).toBeInstanceOf(Date);
        expect(item.account).toBe('SBI証券');
        expect(item.notes).toBe('配当株として購入');
      });

      it('should add item without optional fields', () => {
        const item = db.addItem({
          portfolioId,
          code: '6758',
          companyName: 'ソニーグループ',
          quantity: 50,
          purchasePrice: 12000,
          purchaseDate: new Date('2024-02-01'),
        });

        expect(item.account).toBeUndefined();
        expect(item.notes).toBeUndefined();
      });

      it('should throw error for invalid stock code', () => {
        expect(() => {
          db.addItem({
            portfolioId,
            code: 'INVALID',
            companyName: 'Test Company',
            quantity: 100,
            purchasePrice: 1000,
            purchaseDate: new Date(),
          });
        }).toThrow(InvalidStockCodeError);
      });

      it('should throw error for non-positive quantity', () => {
        expect(() => {
          db.addItem({
            portfolioId,
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: 0,
            purchasePrice: 2500,
            purchaseDate: new Date(),
          });
        }).toThrow(ValidationError);

        expect(() => {
          db.addItem({
            portfolioId,
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: -10,
            purchasePrice: 2500,
            purchaseDate: new Date(),
          });
        }).toThrow(ValidationError);
      });

      it('should throw error for non-positive purchase price', () => {
        expect(() => {
          db.addItem({
            portfolioId,
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: 100,
            purchasePrice: 0,
            purchaseDate: new Date(),
          });
        }).toThrow(ValidationError);
      });

      it('should throw error for non-existent portfolio', () => {
        expect(() => {
          db.addItem({
            portfolioId: 999,
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: 100,
            purchasePrice: 2500,
            purchaseDate: new Date(),
          });
        }).toThrow(PortfolioNotFoundError);
      });

      it('should throw error for duplicate stock in same portfolio', () => {
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        });

        expect(() => {
          db.addItem({
            portfolioId,
            code: '7203',
            companyName: 'トヨタ自動車',
            quantity: 50,
            purchasePrice: 2600,
            purchaseDate: new Date(),
          });
        }).toThrow(DuplicateStockError);
      });
    });

    describe('Get Item', () => {
      it('should get item by ID', () => {
        const created = db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date('2024-01-15'),
        });

        const item = db.getItem(created.id);
        expect(item).toBeDefined();
        expect(item?.id).toBe(created.id);
        expect(item?.code).toBe('7203');
      });

      it('should return null for non-existent item', () => {
        const item = db.getItem(999);
        expect(item).toBeNull();
      });
    });

    describe('List Items', () => {
      it('should return empty array for portfolio with no items', () => {
        const items = db.listItems(portfolioId);
        expect(items).toHaveLength(0);
      });

      it('should list all items in portfolio', () => {
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date('2024-01-15'),
        });

        db.addItem({
          portfolioId,
          code: '6758',
          companyName: 'ソニーグループ',
          quantity: 50,
          purchasePrice: 12000,
          purchaseDate: new Date('2024-02-01'),
        });

        const items = db.listItems(portfolioId);
        expect(items).toHaveLength(2);

        const codes = items.map((item) => item.code);
        expect(codes).toContain('7203');
        expect(codes).toContain('6758');
      });
    });

    describe('Update Item', () => {
      let itemId: number;

      beforeEach(() => {
        const item = db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date('2024-01-15'),
          account: 'SBI証券',
        });
        itemId = item.id;
      });

      it('should update quantity', () => {
        const updated = db.updateItem(itemId, { quantity: 150 });
        expect(updated.quantity).toBe(150);
      });

      it('should update purchase price', () => {
        const updated = db.updateItem(itemId, { purchasePrice: 2600 });
        expect(updated.purchasePrice).toBe(2600);
      });

      it('should update purchase date', () => {
        const newDate = new Date('2024-02-01');
        const updated = db.updateItem(itemId, { purchaseDate: newDate });
        expect(updated.purchaseDate.toISOString()).toBe(newDate.toISOString());
      });

      it('should update account', () => {
        const updated = db.updateItem(itemId, { account: '楽天証券' });
        expect(updated.account).toBe('楽天証券');
      });

      it('should update notes', () => {
        const updated = db.updateItem(itemId, { notes: 'Updated notes' });
        expect(updated.notes).toBe('Updated notes');
      });

      it('should update multiple fields', () => {
        const updated = db.updateItem(itemId, {
          quantity: 200,
          purchasePrice: 2700,
          notes: 'Increased position',
        });

        expect(updated.quantity).toBe(200);
        expect(updated.purchasePrice).toBe(2700);
        expect(updated.notes).toBe('Increased position');
      });

      it('should return unchanged item if no updates provided', () => {
        const original = db.getItem(itemId);
        const updated = db.updateItem(itemId, {});

        expect(updated.quantity).toBe(original?.quantity ?? 0);
      });

      it('should throw error for non-existent item', () => {
        expect(() => {
          db.updateItem(999, { quantity: 100 });
        }).toThrow(PortfolioItemNotFoundError);
      });

      it('should throw error for non-positive quantity', () => {
        expect(() => {
          db.updateItem(itemId, { quantity: 0 });
        }).toThrow(ValidationError);
      });

      it('should throw error for non-positive purchase price', () => {
        expect(() => {
          db.updateItem(itemId, { purchasePrice: -100 });
        }).toThrow(ValidationError);
      });
    });

    describe('Delete Item', () => {
      it('should delete item', () => {
        const item = db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        });

        db.deleteItem(item.id);

        const deleted = db.getItem(item.id);
        expect(deleted).toBeNull();
      });

      it('should throw error for non-existent item', () => {
        expect(() => {
          db.deleteItem(999);
        }).toThrow(PortfolioItemNotFoundError);
      });
    });
  });

  describe('Aggregation Operations', () => {
    let portfolioId: number;

    beforeEach(() => {
      const portfolio = db.createPortfolio({ name: 'Test Portfolio' });
      portfolioId = portfolio.id;
    });

    describe('Get Portfolio With Items', () => {
      it('should get portfolio with all items', () => {
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        });

        db.addItem({
          portfolioId,
          code: '6758',
          companyName: 'ソニーグループ',
          quantity: 50,
          purchasePrice: 12000,
          purchaseDate: new Date(),
        });

        const result = db.getPortfolioWithItems(portfolioId);

        expect(result).toBeDefined();
        expect(result?.name).toBe('Test Portfolio');
        expect(result?.items).toHaveLength(2);
      });

      it('should get portfolio with empty items array', () => {
        const result = db.getPortfolioWithItems(portfolioId);

        expect(result).toBeDefined();
        expect(result?.items).toHaveLength(0);
      });

      it('should return null for non-existent portfolio', () => {
        const result = db.getPortfolioWithItems(999);
        expect(result).toBeNull();
      });
    });

    describe('Get Portfolio Summary', () => {
      it('should calculate summary statistics', () => {
        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        });

        db.addItem({
          portfolioId,
          code: '6758',
          companyName: 'ソニーグループ',
          quantity: 50,
          purchasePrice: 12000,
          purchaseDate: new Date(),
        });

        const summary = db.getPortfolioSummary(portfolioId);

        expect(summary).toBeDefined();
        expect(summary?.stockCount).toBe(2);
        expect(summary?.totalShares).toBe(150);
      });

      it('should return zero statistics for empty portfolio', () => {
        const summary = db.getPortfolioSummary(portfolioId);

        expect(summary).toBeDefined();
        expect(summary?.stockCount).toBe(0);
        expect(summary?.totalShares).toBe(0);
      });

      it('should return null for non-existent portfolio', () => {
        const summary = db.getPortfolioSummary(999);
        expect(summary).toBeNull();
      });
    });

    describe('List Portfolio Summaries', () => {
      it('should list summaries for all portfolios', () => {
        const portfolio2 = db.createPortfolio({ name: 'Portfolio 2' });

        db.addItem({
          portfolioId,
          code: '7203',
          companyName: 'トヨタ自動車',
          quantity: 100,
          purchasePrice: 2500,
          purchaseDate: new Date(),
        });

        db.addItem({
          portfolioId: portfolio2.id,
          code: '6758',
          companyName: 'ソニーグループ',
          quantity: 50,
          purchasePrice: 12000,
          purchaseDate: new Date(),
        });

        const summaries = db.listPortfolioSummaries();

        expect(summaries).toHaveLength(2);
        const testPortfolio = summaries.find((s) => s.name === 'Test Portfolio');
        expect(testPortfolio?.stockCount).toBe(1);
        expect(testPortfolio?.totalShares).toBe(100);
      });
    });
  });
});
