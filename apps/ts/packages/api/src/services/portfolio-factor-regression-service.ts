import { logger } from '@trading25/shared/utils/logger';
import type { PortfolioFactorRegressionResponse } from '../schemas/portfolio-factor-regression';
import { btGet } from './bt-api-proxy';

export class PortfolioFactorRegressionService {
  async analyzePortfolio(params: {
    portfolioId: number;
    lookbackDays: number;
  }): Promise<PortfolioFactorRegressionResponse> {
    const { portfolioId, lookbackDays } = params;
    logger.debug('Proxying portfolio factor regression request to apps/bt API', {
      portfolioId,
      lookbackDays,
    });

    return btGet<PortfolioFactorRegressionResponse>(
      `/api/analytics/portfolio-factor-regression/${encodeURIComponent(String(portfolioId))}`,
      {
        lookbackDays,
      }
    );
  }

  close(): void {
    // No local resources: this service is now a pure apps/bt proxy.
  }
}
