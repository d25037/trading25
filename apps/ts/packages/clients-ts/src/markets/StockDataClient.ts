import type {
  JQuantsDailyQuote,
  JQuantsDailyQuotesParams,
  JQuantsDailyQuotesResponse,
  JQuantsListedInfo,
  JQuantsListedInfoParams,
  JQuantsListedInfoResponse,
  JQuantsWeeklyMarginInterest,
  JQuantsWeeklyMarginInterestParams,
  JQuantsWeeklyMarginInterestResponse,
} from '../types/jquants';
import { BaseJQuantsClient } from '../base/BaseJQuantsClient';

export class StockDataClient extends BaseJQuantsClient {
  /**
   * Get listed stock information.
   * Automatically handles pagination to return all data.
   */
  override async getListedInfo(params?: JQuantsListedInfoParams): Promise<JQuantsListedInfoResponse> {
    return this.makePaginatedRequest<JQuantsListedInfoResponse, JQuantsListedInfo, JQuantsListedInfoParams>(
      '/equities/master',
      params
    );
  }

  /**
   * Get daily stock quotes.
   * Automatically handles pagination to return all data.
   */
  override async getDailyQuotes(params: JQuantsDailyQuotesParams): Promise<JQuantsDailyQuotesResponse> {
    if (!params.code && !params.date && !params.from && !params.to) {
      throw new Error('At least one of code or date parameters is required');
    }

    return this.makePaginatedRequest<JQuantsDailyQuotesResponse, JQuantsDailyQuote, JQuantsDailyQuotesParams>(
      '/equities/bars/daily',
      params
    );
  }

  /**
   * Get weekly margin interest data.
   * Automatically handles pagination to return all data.
   */
  override async getWeeklyMarginInterest(
    params: JQuantsWeeklyMarginInterestParams
  ): Promise<JQuantsWeeklyMarginInterestResponse> {
    if (!params.code && !params.date && !params.from && !params.to) {
      throw new Error('At least one of code or date parameters is required');
    }

    return this.makePaginatedRequest<
      JQuantsWeeklyMarginInterestResponse,
      JQuantsWeeklyMarginInterest,
      JQuantsWeeklyMarginInterestParams
    >('/markets/margin-interest', params);
  }
}
