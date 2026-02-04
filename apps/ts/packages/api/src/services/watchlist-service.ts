import { getMarketDbPath, getPortfolioDbPath } from '@trading25/shared';
import { MarketDataReader } from '@trading25/shared/market-sync';
import { logger } from '@trading25/shared/utils/logger';
import {
  type CreateWatchlistInput,
  type CreateWatchlistItemInput,
  DuplicateWatchlistNameError,
  DuplicateWatchlistStockError,
  type UpdateWatchlistInput,
  type Watchlist,
  WatchlistDatabase,
  type WatchlistItem,
  WatchlistItemNotFoundError,
  WatchlistNameNotFoundError,
  WatchlistNotFoundError,
  type WatchlistSummary,
  type WatchlistWithItems,
} from '@trading25/shared/watchlist';

export class WatchlistService {
  private db: WatchlistDatabase;

  constructor() {
    const dbPath = getPortfolioDbPath();
    this.db = new WatchlistDatabase(dbPath, false);
    logger.debug('WatchlistService initialized', { dbPath });
  }

  async listWatchlists(): Promise<WatchlistSummary[]> {
    logger.debug('Listing all watchlists');
    return this.db.listWatchlistSummaries();
  }

  async getWatchlist(id: number): Promise<Watchlist> {
    logger.debug('Getting watchlist by ID', { id });
    const watchlist = this.db.getWatchlist(id);
    if (!watchlist) {
      throw new WatchlistNotFoundError(id);
    }
    return watchlist;
  }

  async getWatchlistWithItems(id: number): Promise<WatchlistWithItems> {
    logger.debug('Getting watchlist with items', { id });
    const watchlist = this.db.getWatchlistWithItems(id);
    if (!watchlist) {
      throw new WatchlistNotFoundError(id);
    }
    return watchlist;
  }

  async getWatchlistByName(name: string): Promise<Watchlist> {
    logger.debug('Getting watchlist by name', { name });
    const watchlist = this.db.getWatchlistByName(name);
    if (!watchlist) {
      throw new WatchlistNameNotFoundError(name);
    }
    return watchlist;
  }

  async createWatchlist(input: CreateWatchlistInput): Promise<Watchlist> {
    logger.debug('Creating watchlist', { name: input.name });
    try {
      return this.db.createWatchlist(input);
    } catch (error) {
      if (error instanceof DuplicateWatchlistNameError) {
        throw error;
      }
      logger.error('Failed to create watchlist', { error });
      throw error;
    }
  }

  async updateWatchlist(id: number, input: UpdateWatchlistInput): Promise<Watchlist> {
    logger.debug('Updating watchlist', { id, input });
    try {
      return this.db.updateWatchlist(id, input);
    } catch (error) {
      if (error instanceof WatchlistNotFoundError || error instanceof DuplicateWatchlistNameError) {
        throw error;
      }
      logger.error('Failed to update watchlist', { id, error });
      throw error;
    }
  }

  async deleteWatchlist(id: number): Promise<void> {
    logger.debug('Deleting watchlist', { id });
    try {
      this.db.deleteWatchlist(id);
    } catch (error) {
      if (error instanceof WatchlistNotFoundError) {
        throw error;
      }
      logger.error('Failed to delete watchlist', { id, error });
      throw error;
    }
  }

  private toJQuantsCode(code: string): string {
    return code.length === 4 ? `${code}0` : code;
  }

  private fetchCompanyName(code: string): string {
    try {
      const marketDbPath = getMarketDbPath();
      const reader = new MarketDataReader(marketDbPath);

      try {
        const jquantsCode = this.toJQuantsCode(code);
        const stockInfo = reader.getStockByCode(jquantsCode);

        if (!stockInfo) {
          logger.warn('Stock not found in market database', { code, jquantsCode });
          return code;
        }

        return stockInfo.companyName;
      } finally {
        reader.close();
      }
    } catch (error) {
      logger.warn('Failed to fetch company name from market database', {
        code,
        error: error instanceof Error ? error.message : String(error),
      });
      return code;
    }
  }

  async addItem(
    watchlistId: number,
    input: Omit<CreateWatchlistItemInput, 'watchlistId' | 'companyName'> & { companyName?: string }
  ): Promise<WatchlistItem> {
    logger.debug('Adding item to watchlist', { watchlistId, code: input.code });
    try {
      const companyName = input.companyName || this.fetchCompanyName(input.code);

      return this.db.addItem({
        ...input,
        companyName,
        watchlistId,
      });
    } catch (error) {
      if (error instanceof WatchlistNotFoundError || error instanceof DuplicateWatchlistStockError) {
        throw error;
      }
      logger.error('Failed to add item to watchlist', { watchlistId, error });
      throw error;
    }
  }

  async deleteItem(itemId: number): Promise<void> {
    logger.debug('Deleting watchlist item', { itemId });
    try {
      this.db.deleteItem(itemId);
    } catch (error) {
      if (error instanceof WatchlistItemNotFoundError) {
        throw error;
      }
      logger.error('Failed to delete watchlist item', { itemId, error });
      throw error;
    }
  }

  async deleteItemByWatchlistNameAndCode(watchlistName: string, code: string): Promise<WatchlistItem> {
    logger.debug('Deleting item by watchlist name and code', { watchlistName, code });
    try {
      const watchlist = await this.getWatchlistByName(watchlistName);
      return this.db.deleteItemByCode(watchlist.id, code);
    } catch (error) {
      if (error instanceof WatchlistNameNotFoundError) {
        throw error;
      }
      logger.error('Failed to delete watchlist item by code', { watchlistName, code, error });
      throw error;
    }
  }

  close(): void {
    this.db.close();
    logger.debug('WatchlistService closed');
  }
}
