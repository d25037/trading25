import { logger } from '@trading25/shared/utils/logger';
import type { FactorRegressionResponse } from '../schemas/factor-regression';
import { btGet } from './bt-api-proxy';

export class FactorRegressionService {
  async analyzeStock(params: { symbol: string; lookbackDays: number }): Promise<FactorRegressionResponse> {
    const { symbol, lookbackDays } = params;
    logger.debug('Proxying factor regression request to apps/bt API', { symbol, lookbackDays });

    return btGet<FactorRegressionResponse>(`/api/analytics/factor-regression/${encodeURIComponent(symbol)}`, {
      lookbackDays,
    });
  }

  close(): void {
    // No local resources: this service is now a pure apps/bt proxy.
  }
}
