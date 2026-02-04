import type { JQuantsTOPIXParams, JQuantsTOPIXResponse } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

export class TopixRawDataService extends BaseJQuantsService {
  async getTOPIX(params?: JQuantsTOPIXParams): Promise<JQuantsTOPIXResponse> {
    return this.withTokenRefresh(async () => {
      logger.debug('Fetching TOPIX from JQuants API', { params });

      const client = this.getJQuantsClient();
      const data = await client.getTOPIX(params);

      logger.debug('Successfully fetched TOPIX', { count: data.data?.length || 0 });
      return data;
    });
  }
}
