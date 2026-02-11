import { logger } from '@trading25/shared/utils/logger';
import type { MarketRankingResponse } from '../../schemas/market-ranking';
import { btGet } from '../bt-api-proxy';

interface RankingQueryOptions {
  date?: string;
  limit: number;
  markets: string;
  lookbackDays: number;
  periodDays: number;
}

export class MarketRankingService {
  async getRankings(options: RankingQueryOptions): Promise<MarketRankingResponse> {
    logger.debug('Proxying market rankings request to apps/bt API', { options });

    return btGet<MarketRankingResponse>('/api/analytics/ranking', {
      date: options.date,
      limit: options.limit,
      markets: options.markets,
      lookbackDays: options.lookbackDays,
      periodDays: options.periodDays,
    });
  }

  close(): void {
    // No local resources: this service is now a pure apps/bt proxy.
  }
}
