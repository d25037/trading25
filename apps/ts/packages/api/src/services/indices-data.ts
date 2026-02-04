import type { JQuantsIndicesParams, JQuantsIndicesResponse } from '@trading25/shared';
import { logger } from '@trading25/shared/utils/logger';
import { BaseJQuantsService } from './base-jquants-service';

export class IndicesDataService extends BaseJQuantsService {
  async getIndices(params?: JQuantsIndicesParams): Promise<JQuantsIndicesResponse> {
    return this.withTokenRefresh(async () => {
      logger.debug('Fetching indices from JQuants API', { params });

      const client = this.getJQuantsClient();
      const data = await client.getIndices(params || {});

      logger.debug('Successfully fetched indices', { count: data.data?.length || 0 });
      return data;
    });
  }
}
