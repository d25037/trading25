import type {
  JQuantsIndex,
  JQuantsIndicesParams,
  JQuantsIndicesResponse,
  JQuantsTOPIX,
  JQuantsTOPIXParams,
  JQuantsTOPIXResponse,
} from '../types/jquants';
import { BaseJQuantsClient } from '../base/BaseJQuantsClient';

export class IndexClient extends BaseJQuantsClient {
  /**
   * Get index data for specified index codes.
   * Automatically handles pagination to return all data.
   */
  override async getIndices(params: JQuantsIndicesParams): Promise<JQuantsIndicesResponse> {
    if (!params.code && !params.date && !params.from && !params.to) {
      throw new Error('At least one of code or date parameters is required');
    }

    return this.makePaginatedRequest<JQuantsIndicesResponse, JQuantsIndex, JQuantsIndicesParams>(
      '/indices/bars/daily',
      params
    );
  }

  /**
   * Get TOPIX index data.
   * Automatically handles pagination to return all data.
   */
  async getTOPIX(params?: JQuantsTOPIXParams): Promise<JQuantsTOPIXResponse> {
    return this.makePaginatedRequest<JQuantsTOPIXResponse, JQuantsTOPIX, JQuantsTOPIXParams>(
      '/indices/bars/daily/topix',
      params
    );
  }
}
