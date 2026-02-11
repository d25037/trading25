import { logger } from '@trading25/shared/utils/logger';
import type { SectorStocksResponse } from '../../schemas/sector-stocks';
import { btGet } from '../bt-api-proxy';

interface SectorStocksQueryOptions {
  sector33Name?: string;
  sector17Name?: string;
  markets: string;
  lookbackDays: number;
  sortBy: 'tradingValue' | 'changePercentage' | 'code';
  sortOrder: 'asc' | 'desc';
  limit: number;
}

export class SectorStocksService {
  async getStocks(options: SectorStocksQueryOptions): Promise<SectorStocksResponse> {
    logger.debug('Proxying sector stocks request to apps/bt API', { options });

    return btGet<SectorStocksResponse>('/api/analytics/sector-stocks', {
      sector33Name: options.sector33Name,
      sector17Name: options.sector17Name,
      markets: options.markets,
      lookbackDays: options.lookbackDays,
      sortBy: options.sortBy,
      sortOrder: options.sortOrder,
      limit: options.limit,
    });
  }

  close(): void {
    // No local resources: this service is now a pure apps/bt proxy.
  }
}
