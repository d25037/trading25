import type { JQuantsStatement, JQuantsStatementsParams, JQuantsStatementsResponse } from '../../types/jquants';
import { BaseJQuantsClient } from '../base/BaseJQuantsClient';

export class StatementsClient extends BaseJQuantsClient {
  /**
   * Get financial statements data.
   * Automatically handles pagination to return all data.
   *
   * @param params Query parameters for financial statements
   * @returns Promise containing financial statements response
   * @throws Error if neither code nor date is provided
   *
   * @example
   * ```typescript
   * // Get statements for specific stock code
   * const statements = await client.getStatements({ code: '86970' });
   *
   * // Get statements disclosed on specific date
   * const statements = await client.getStatements({ date: '20230130' });
   *
   * // Get statements for specific stock and date
   * const statements = await client.getStatements({
   *   code: '86970',
   *   date: '20230130'
   * });
   * ```
   */
  async getStatements(params: JQuantsStatementsParams): Promise<JQuantsStatementsResponse> {
    if (!params.code && !params.date) {
      throw new Error('Either code or date parameter is required');
    }

    return this.makePaginatedRequest<JQuantsStatementsResponse, JQuantsStatement, JQuantsStatementsParams>(
      '/fins/summary',
      params
    );
  }
}
