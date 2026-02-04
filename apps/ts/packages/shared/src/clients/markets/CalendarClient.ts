import type { JQuantsTradingCalendarParams, JQuantsTradingCalendarResponse } from '../../types/jquants';
import { BaseJQuantsClient } from '../base/BaseJQuantsClient';

export class CalendarClient extends BaseJQuantsClient {
  /**
   * Get trading calendar information
   */
  async getTradingCalendar(params?: JQuantsTradingCalendarParams): Promise<JQuantsTradingCalendarResponse> {
    return this.makeAuthenticatedRequest<JQuantsTradingCalendarResponse, JQuantsTradingCalendarParams>(
      'get',
      '/markets/calendar',
      params
    );
  }
}
