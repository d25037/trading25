import { logger } from '@trading25/shared/utils/logger';
import type { ROEResponse } from '../schemas/roe';
import { btGet } from './bt-api-proxy';

interface ROEQueryOptions {
  code?: string;
  date?: string;
  annualize: boolean;
  preferConsolidated: boolean;
  minEquity: number;
  sortBy: 'roe' | 'code' | 'date';
  limit: number;
}

export class ROEDataService {
  async calculateROE(options: ROEQueryOptions): Promise<ROEResponse> {
    logger.debug('Proxying ROE request to apps/bt API', { options });

    return btGet<ROEResponse>('/api/analytics/roe', {
      code: options.code,
      date: options.date,
      annualize: options.annualize,
      preferConsolidated: options.preferConsolidated,
      minEquity: options.minEquity,
      sortBy: options.sortBy,
      limit: options.limit,
    });
  }
}
