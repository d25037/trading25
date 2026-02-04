import type { JQuantsWeeklyMarginInterestParams, JQuantsWeeklyMarginInterestResponse } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

export class MarginInterestDataService extends BaseJQuantsService {
  async getMarginInterest(params: JQuantsWeeklyMarginInterestParams): Promise<JQuantsWeeklyMarginInterestResponse> {
    return this.withTokenRefresh(async () => {
      logger.debug('Fetching margin interest from JQuants API', { params });

      const client = this.getJQuantsClient();
      const data = await client.getWeeklyMarginInterest(params);

      logger.debug('Successfully fetched margin interest', {
        count: data.data?.length || 0,
      });
      return data;
    });
  }
}
