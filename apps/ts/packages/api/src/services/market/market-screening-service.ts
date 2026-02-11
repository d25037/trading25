import { logger } from '@trading25/shared/utils/logger';
import type { MarketScreeningResponse } from '../../schemas/market-screening';
import { btGet } from '../bt-api-proxy';

interface ScreeningQueryOptions {
  markets: string;
  rangeBreakFast: boolean;
  rangeBreakSlow: boolean;
  recentDays: number;
  referenceDate?: string;
  minBreakPercentage?: number;
  minVolumeRatio?: number;
  sortBy: 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage';
  order: 'asc' | 'desc';
  limit?: number;
}

export class MarketScreeningService {
  async runScreening(options: ScreeningQueryOptions): Promise<MarketScreeningResponse> {
    logger.debug('Proxying market screening request to apps/bt API', { options });

    return btGet<MarketScreeningResponse>('/api/analytics/screening', {
      markets: options.markets,
      rangeBreakFast: options.rangeBreakFast,
      rangeBreakSlow: options.rangeBreakSlow,
      recentDays: options.recentDays,
      date: options.referenceDate,
      minBreakPercentage: options.minBreakPercentage,
      minVolumeRatio: options.minVolumeRatio,
      sortBy: options.sortBy,
      order: options.order,
      limit: options.limit,
    });
  }

  close(): void {
    // No local resources: this service is now a pure apps/bt proxy.
  }
}
