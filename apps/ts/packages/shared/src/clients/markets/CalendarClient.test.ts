import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockResponse } from '../../test-utils/fetch-mock';
import { mockJQuantsConfig, mockTradingCalendarResponse } from '../../test-utils/fixtures';
import { resetRateLimiter } from '../base/BaseJQuantsClient';
import { CalendarClient } from './CalendarClient';

describe('CalendarClient', () => {
  let client: CalendarClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    // Reset rate limiter and disable it for tests to avoid timeouts
    resetRateLimiter({ disable: true });
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
    client = new CalendarClient(mockJQuantsConfig);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    // Re-enable rate limiter after tests
    resetRateLimiter();
  });

  describe('getTradingCalendar', () => {
    it('should fetch trading calendar successfully without parameters', async () => {
      fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

      const result = await client.getTradingCalendar();

      expect(result).toEqual(mockTradingCalendarResponse);
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/markets/calendar'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should fetch trading calendar with holiday division parameter', async () => {
      fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

      const params = { hol_div: '1' };
      const result = await client.getTradingCalendar(params);

      expect(result).toEqual(mockTradingCalendarResponse);
      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('hol_div=1'), expect.any(Object));
    });

    it('should fetch trading calendar with date range parameters', async () => {
      fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

      const params = {
        hol_div: '0',
        from: '2025-01-01',
        to: '2025-01-31',
      };
      const result = await client.getTradingCalendar(params);

      expect(result).toEqual(mockTradingCalendarResponse);
      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should handle different date formats', async () => {
      fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

      const params = {
        from: '20250101', // YYYYMMDD format
        to: '20250131',
      };
      const result = await client.getTradingCalendar(params);

      expect(result).toEqual(mockTradingCalendarResponse);
      expect(fetchSpy).toHaveBeenCalled();
    });
  });
});
