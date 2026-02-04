import type { JQuantsDailyQuotesParams, JQuantsDailyQuotesResponse } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

export class DailyQuotesDataService extends BaseJQuantsService {
  async getDailyQuotes(params: JQuantsDailyQuotesParams): Promise<JQuantsDailyQuotesResponse> {
    return this.withTokenRefresh(async () => {
      logger.debug('Fetching daily quotes from JQuants API', { params });

      const client = this.getJQuantsClient();
      const data = await client.getDailyQuotes(params);

      logger.debug('Successfully fetched daily quotes', { count: data.data?.length || 0 });
      return data;
    });
  }
}
