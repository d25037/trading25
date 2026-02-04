import type { JQuantsListedInfoParams, JQuantsListedInfoResponse } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

export class ListedInfoDataService extends BaseJQuantsService {
  async getListedInfo(params?: JQuantsListedInfoParams): Promise<JQuantsListedInfoResponse> {
    return this.withTokenRefresh(async () => {
      logger.debug('Fetching listed info from JQuants API', { params });

      const client = this.getJQuantsClient();
      const data = await client.getListedInfo(params);

      logger.debug('Successfully fetched listed info', { count: data.data?.length || 0 });
      return data;
    });
  }
}
