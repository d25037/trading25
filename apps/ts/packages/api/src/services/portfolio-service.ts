import { getMarketDbPath, getPortfolioDbPath } from '@trading25/shared';
import { MarketDataReader } from '@trading25/shared/market-sync';
import {
  type CreatePortfolioInput,
  type CreatePortfolioItemInput,
  DuplicatePortfolioNameError,
  DuplicateStockError,
  InvalidStockCodeError,
  type Portfolio,
  PortfolioDatabase,
  type PortfolioItem,
  PortfolioItemNotFoundError,
  PortfolioNameNotFoundError,
  PortfolioNotFoundError,
  type PortfolioSummary,
  type PortfolioWithItems,
  StockNotFoundInPortfolioError,
  type UpdatePortfolioInput,
  type UpdatePortfolioItemInput,
  ValidationError,
} from '@trading25/shared/portfolio';
import { logger } from '@trading25/shared/utils/logger';

/**
 * Portfolio Service
 * Business logic layer for portfolio management
 */
export class PortfolioService {
  private db: PortfolioDatabase;

  constructor() {
    const dbPath = getPortfolioDbPath();
    this.db = new PortfolioDatabase(dbPath, false);
    logger.debug('PortfolioService initialized', { dbPath });
  }

  /**
   * List all portfolios with summary statistics
   */
  async listPortfolios(): Promise<PortfolioSummary[]> {
    logger.debug('Listing all portfolios');
    return this.db.listPortfolioSummaries();
  }

  /**
   * Get portfolio by ID
   */
  async getPortfolio(id: number): Promise<Portfolio> {
    logger.debug('Getting portfolio by ID', { id });
    const portfolio = this.db.getPortfolio(id);
    if (!portfolio) {
      throw new PortfolioNotFoundError(id);
    }
    return portfolio;
  }

  /**
   * Get portfolio with all items
   */
  async getPortfolioWithItems(id: number): Promise<PortfolioWithItems> {
    logger.debug('Getting portfolio with items', { id });
    const portfolio = this.db.getPortfolioWithItems(id);
    if (!portfolio) {
      throw new PortfolioNotFoundError(id);
    }
    return portfolio;
  }

  /**
   * Get portfolio by name
   */
  async getPortfolioByName(name: string): Promise<Portfolio> {
    logger.debug('Getting portfolio by name', { name });
    const portfolio = this.db.getPortfolioByName(name);
    if (!portfolio) {
      throw new PortfolioNameNotFoundError(name);
    }
    return portfolio;
  }

  /**
   * Create a new portfolio
   */
  async createPortfolio(input: CreatePortfolioInput): Promise<Portfolio> {
    logger.debug('Creating portfolio', { name: input.name });
    try {
      return this.db.createPortfolio(input);
    } catch (error) {
      if (error instanceof DuplicatePortfolioNameError) {
        throw error;
      }
      logger.error('Failed to create portfolio', { error });
      throw error;
    }
  }

  /**
   * Update portfolio
   */
  async updatePortfolio(id: number, input: UpdatePortfolioInput): Promise<Portfolio> {
    logger.debug('Updating portfolio', { id, input });
    try {
      return this.db.updatePortfolio(id, input);
    } catch (error) {
      if (error instanceof PortfolioNotFoundError || error instanceof DuplicatePortfolioNameError) {
        throw error;
      }
      logger.error('Failed to update portfolio', { id, error });
      throw error;
    }
  }

  /**
   * Delete portfolio
   */
  async deletePortfolio(id: number): Promise<void> {
    logger.debug('Deleting portfolio', { id });
    try {
      this.db.deletePortfolio(id);
    } catch (error) {
      if (error instanceof PortfolioNotFoundError) {
        throw error;
      }
      logger.error('Failed to delete portfolio', { id, error });
      throw error;
    }
  }

  /**
   * Convert 4-digit stock code to 5-digit JQuants format
   * JQuants uses 5-digit codes, with 4-digit codes padded with trailing '0'
   */
  private toJQuantsCode(code: string): string {
    return code.length === 4 ? `${code}0` : code;
  }

  /**
   * Fetch company name from local market database
   * Falls back to stock code if database is not available or stock is not found
   */
  private fetchCompanyName(code: string): string {
    try {
      const marketDbPath = getMarketDbPath();
      const reader = new MarketDataReader(marketDbPath);

      try {
        // Convert 4-digit code to 5-digit JQuants format
        const jquantsCode = this.toJQuantsCode(code);
        const stockInfo = reader.getStockByCode(jquantsCode);

        if (!stockInfo) {
          logger.warn('Stock not found in market database', { code, jquantsCode });
          // Return the code as a fallback if stock is not found
          // This allows adding stocks that haven't been synced yet
          return code;
        }

        return stockInfo.companyName;
      } finally {
        reader.close();
      }
    } catch (error) {
      // If market database is not available, use the code as fallback
      logger.warn('Failed to fetch company name from market database', {
        code,
        error: error instanceof Error ? error.message : String(error),
      });
      return code;
    }
  }

  /**
   * Add item to portfolio
   */
  async addItem(
    portfolioId: number,
    input: Omit<CreatePortfolioItemInput, 'portfolioId' | 'companyName'> & { companyName?: string }
  ): Promise<PortfolioItem> {
    logger.debug('Adding item to portfolio', { portfolioId, code: input.code });
    try {
      // Fetch company name from local market database if not provided
      const companyName = input.companyName || this.fetchCompanyName(input.code);

      return this.db.addItem({
        ...input,
        companyName,
        portfolioId,
      });
    } catch (error) {
      if (
        error instanceof PortfolioNotFoundError ||
        error instanceof InvalidStockCodeError ||
        error instanceof DuplicateStockError ||
        error instanceof ValidationError
      ) {
        throw error;
      }
      logger.error('Failed to add item to portfolio', { portfolioId, error });
      throw error;
    }
  }

  /**
   * Get portfolio item by ID
   */
  async getItem(itemId: number): Promise<PortfolioItem> {
    logger.debug('Getting portfolio item', { itemId });
    const item = this.db.getItem(itemId);
    if (!item) {
      throw new PortfolioItemNotFoundError(itemId);
    }
    return item;
  }

  /**
   * Update portfolio item
   */
  async updateItem(itemId: number, input: UpdatePortfolioItemInput): Promise<PortfolioItem> {
    logger.debug('Updating portfolio item', { itemId, input });
    try {
      return this.db.updateItem(itemId, input);
    } catch (error) {
      if (error instanceof PortfolioItemNotFoundError || error instanceof ValidationError) {
        throw error;
      }
      logger.error('Failed to update portfolio item', { itemId, error });
      throw error;
    }
  }

  /**
   * Delete portfolio item
   */
  async deleteItem(itemId: number): Promise<void> {
    logger.debug('Deleting portfolio item', { itemId });
    try {
      this.db.deleteItem(itemId);
    } catch (error) {
      if (error instanceof PortfolioItemNotFoundError) {
        throw error;
      }
      logger.error('Failed to delete portfolio item', { itemId, error });
      throw error;
    }
  }

  /**
   * Update portfolio item by portfolio name and stock code
   * Returns the updated item
   */
  async updateItemByPortfolioNameAndCode(
    portfolioName: string,
    code: string,
    input: UpdatePortfolioItemInput
  ): Promise<PortfolioItem> {
    logger.debug('Updating item by portfolio name and code', { portfolioName, code, input });
    try {
      // Get portfolio by name (throws PortfolioNameNotFoundError if not found)
      const portfolio = await this.getPortfolioByName(portfolioName);

      // Update item by code (throws StockNotFoundInPortfolioError if not found)
      return this.db.updateItemByCode(portfolio.id, code, input);
    } catch (error) {
      if (
        error instanceof PortfolioNameNotFoundError ||
        error instanceof StockNotFoundInPortfolioError ||
        error instanceof ValidationError
      ) {
        throw error;
      }
      logger.error('Failed to update portfolio item by code', { portfolioName, code, error });
      throw error;
    }
  }

  /**
   * Delete portfolio item by portfolio name and stock code
   * Returns the deleted item for confirmation
   */
  async deleteItemByPortfolioNameAndCode(portfolioName: string, code: string): Promise<PortfolioItem> {
    logger.debug('Deleting item by portfolio name and code', { portfolioName, code });
    try {
      // Get portfolio by name (throws PortfolioNameNotFoundError if not found)
      const portfolio = await this.getPortfolioByName(portfolioName);

      // Delete item by code (throws StockNotFoundInPortfolioError if not found)
      return this.db.deleteItemByCode(portfolio.id, code);
    } catch (error) {
      if (error instanceof PortfolioNameNotFoundError || error instanceof StockNotFoundInPortfolioError) {
        throw error;
      }
      logger.error('Failed to delete portfolio item by code', { portfolioName, code, error });
      throw error;
    }
  }

  /**
   * Close database connection
   */
  close(): void {
    this.db.close();
    logger.debug('PortfolioService closed');
  }
}
