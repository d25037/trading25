import { MarketDataReader } from '@trading25/shared/market-sync';
import { getMarketDbPath } from '@trading25/shared/utils/dataset-paths';
import { logger } from '@trading25/shared/utils/logger';
import type { SectorStocksResponse } from '../../schemas/sector-stocks';

interface SectorStocksQueryOptions {
  sector33Name?: string;
  sector17Name?: string;
  markets: string;
  lookbackDays: number;
  sortBy: 'tradingValue' | 'changePercentage' | 'code';
  sortOrder: 'asc' | 'desc';
  limit: number;
}

/**
 * Normalize middle dot characters to match database format.
 * Database uses halfwidth katakana middle dot (･ U+FF65) for some sector names.
 * Frontend may send fullwidth middle dot (・ U+30FB).
 */
function normalizeMiddleDot(text: string): string {
  // Replace fullwidth middle dot (・ U+30FB) with halfwidth (･ U+FF65)
  return text.replace(/・/g, '･');
}

export class SectorStocksService {
  private reader: MarketDataReader | null = null;
  private dbPath: string;

  constructor() {
    this.dbPath = getMarketDbPath();
  }

  private getReader(): MarketDataReader {
    if (!this.reader) {
      this.reader = new MarketDataReader(this.dbPath);
    }
    return this.reader;
  }

  async getStocks(options: SectorStocksQueryOptions): Promise<SectorStocksResponse> {
    logger.debug('Getting sector stocks', { options });

    const reader = this.getReader();

    // Parse market codes
    const marketCodes = options.markets.split(',').map((m) => m.trim());

    // Normalize sector names to match database format (handles middle dot variants)
    const normalizedSector33Name = options.sector33Name ? normalizeMiddleDot(options.sector33Name) : undefined;
    const normalizedSector17Name = options.sector17Name ? normalizeMiddleDot(options.sector17Name) : undefined;

    // Fetch stocks
    const stocks = reader.getStocksBySector({
      sector33Name: normalizedSector33Name,
      sector17Name: normalizedSector17Name,
      marketCodes,
      lookbackDays: options.lookbackDays,
      sortBy: options.sortBy,
      sortOrder: options.sortOrder,
      limit: options.limit,
    });

    logger.debug('Sector stocks fetched', { count: stocks.length });

    return {
      sector33Name: options.sector33Name,
      sector17Name: options.sector17Name,
      markets: marketCodes,
      lookbackDays: options.lookbackDays,
      sortBy: options.sortBy,
      sortOrder: options.sortOrder,
      stocks,
      lastUpdated: new Date().toISOString(),
    };
  }

  close(): void {
    if (this.reader) {
      this.reader.close();
      this.reader = null;
    }
  }
}
