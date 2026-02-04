import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockResponse } from '../../test-utils/fetch-mock';
import {
  mockDailyQuotesResponse,
  mockJQuantsConfig,
  mockListedInfoResponse,
  mockWeeklyMarginInterestResponse,
} from '../../test-utils/fixtures';
import { resetRateLimiter } from '../base/BaseJQuantsClient';
import { StockDataClient } from './StockDataClient';

describe('StockDataClient', () => {
  let client: StockDataClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    // Reset rate limiter and disable it for tests to avoid timeouts
    resetRateLimiter({ disable: true });
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
    client = new StockDataClient(mockJQuantsConfig);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    // Re-enable rate limiter after tests
    resetRateLimiter();
  });

  describe('getListedInfo', () => {
    it('should fetch listed info successfully', async () => {
      // Response without pagination_key to simulate last/only page
      const responseWithoutPagination = { data: mockListedInfoResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getListedInfo({ code: '7203' });

      // makePaginatedRequest returns data without pagination_key
      expect(result).toEqual({ data: mockListedInfoResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/equities/master'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should fetch listed info without parameters', async () => {
      const responseWithoutPagination = { data: mockListedInfoResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getListedInfo();

      expect(result).toEqual({ data: mockListedInfoResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/equities/master'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });
  });

  describe('getDailyQuotes', () => {
    it('should fetch daily quotes successfully', async () => {
      const responseWithoutPagination = { data: mockDailyQuotesResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { code: '7203', date: '2025-01-10' };
      const result = await client.getDailyQuotes(params);

      expect(result).toEqual({ data: mockDailyQuotesResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/equities/bars/daily'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should throw error when no parameters provided', async () => {
      await expect(client.getDailyQuotes({})).rejects.toThrow('At least one of code or date parameters is required');
    });

    it('should work with date range parameters', async () => {
      const responseWithoutPagination = { data: mockDailyQuotesResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { code: '7203', from: '2025-01-01', to: '2025-01-10' };
      const result = await client.getDailyQuotes(params);

      expect(result).toEqual({ data: mockDailyQuotesResponse.data });
      expect(fetchSpy).toHaveBeenCalled();
    });
  });

  describe('getWeeklyMarginInterest', () => {
    it('should fetch weekly margin interest successfully', async () => {
      const responseWithoutPagination = { data: mockWeeklyMarginInterestResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { code: '7203', date: '2025-01-10' };
      const result = await client.getWeeklyMarginInterest(params);

      expect(result).toEqual({ data: mockWeeklyMarginInterestResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/markets/margin-interest'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should throw error when no parameters provided', async () => {
      await expect(client.getWeeklyMarginInterest({})).rejects.toThrow(
        'At least one of code or date parameters is required'
      );
    });
  });
});
