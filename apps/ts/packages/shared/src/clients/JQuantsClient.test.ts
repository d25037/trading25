import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockErrorResponse, createMockResponse } from '../test-utils/fetch-mock';
import {
  mockDailyQuotesResponse,
  mockIndicesResponse,
  mockJQuantsConfig,
  mockListedInfoResponse,
  mockTOPIXResponse,
  mockTradingCalendarResponse,
  mockWeeklyMarginInterestResponse,
} from '../test-utils/fixtures';
import type { JQuantsConfig } from '../types/jquants';
import { resetRateLimiter } from './base/BaseJQuantsClient';
import { JQuantsClient } from './JQuantsClient';

// Helper to clear JQuants environment variables (delete is required to truly clear, not just set undefined)
function clearJQuantsEnv(): void {
  // biome-ignore lint/performance/noDelete: Required to truly clear env vars (setting undefined doesn't work)
  delete process.env.JQUANTS_API_KEY;
}

describe('JQuantsClient', () => {
  let client: JQuantsClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    // Reset rate limiter and disable it for tests to avoid timeouts
    resetRateLimiter({ disable: true });
    // Create fresh mock for each test
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    // Re-enable rate limiter after tests
    resetRateLimiter();
  });

  describe('constructor', () => {
    it('should initialize with provided config', () => {
      const config: JQuantsConfig = {
        apiKey: 'test_api_key',
      };

      client = new JQuantsClient(config);

      expect(client).toBeDefined();
    });

    it('should initialize with environment variables when no config provided', () => {
      const originalEnv = process.env;
      process.env = {
        ...originalEnv,
        JQUANTS_API_KEY: 'env_api_key',
      };

      client = new JQuantsClient();
      process.env = originalEnv;

      expect(client).toBeDefined();
    });
  });

  describe('authentication', () => {
    beforeEach(() => {
      // Clear environment variables to ensure clean test state
      clearJQuantsEnv();
    });

    it('should throw error when API key is missing', async () => {
      client = new JQuantsClient({ apiKey: '' });

      await expect(client.getListedInfo()).rejects.toThrow('API key is required for authentication');
    });
  });

  describe('getListedInfo', () => {
    beforeEach(() => {
      client = new JQuantsClient(mockJQuantsConfig);
    });

    it('should fetch listed info without parameters', async () => {
      // Response without pagination_key to simulate last/only page
      const responseWithoutPagination = { data: mockListedInfoResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      const result = await client.getListedInfo();

      expect(fetchSpy).toHaveBeenCalledWith(
        'https://api.jquants.com/v2/equities/master',
        expect.objectContaining({
          method: 'GET',
          headers: expect.objectContaining({
            'x-api-key': mockJQuantsConfig.apiKey,
          }),
        })
      );
      // makePaginatedRequest returns data without pagination_key
      expect(result).toEqual({ data: mockListedInfoResponse.data });
    });

    it('should fetch listed info with parameters', async () => {
      const responseWithoutPagination = { data: mockListedInfoResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      const params = { code: '7203', date: '2025-01-10' };
      await client.getListedInfo(params);

      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('code=7203'), expect.any(Object));
      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('date=2025-01-10'), expect.any(Object));
    });
  });

  describe('getDailyQuotes', () => {
    beforeEach(() => {
      client = new JQuantsClient(mockJQuantsConfig);
    });

    it('should fetch daily quotes with valid parameters', async () => {
      const responseWithoutPagination = { data: mockDailyQuotesResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      const params = { code: '7203' };
      const result = await client.getDailyQuotes(params);

      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/equities/bars/daily'),
        expect.objectContaining({
          method: 'GET',
          headers: expect.objectContaining({
            'x-api-key': mockJQuantsConfig.apiKey,
          }),
        })
      );
      expect(result).toEqual({ data: mockDailyQuotesResponse.data });
    });

    it('should throw error when no required parameters provided', async () => {
      await expect(client.getDailyQuotes({})).rejects.toThrow('At least one of code or date parameters is required');
    });

    it('should accept date parameter', async () => {
      const responseWithoutPagination = { data: mockDailyQuotesResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      await client.getDailyQuotes({ date: '2025-01-10' });

      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should accept from/to parameters', async () => {
      const responseWithoutPagination = { data: mockDailyQuotesResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      await client.getDailyQuotes({ from: '2025-01-01', to: '2025-01-10' });

      expect(fetchSpy).toHaveBeenCalled();
    });
  });

  describe('getWeeklyMarginInterest', () => {
    beforeEach(() => {
      client = new JQuantsClient(mockJQuantsConfig);
    });

    it('should fetch weekly margin interest with valid parameters', async () => {
      const responseWithoutPagination = { data: mockWeeklyMarginInterestResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(responseWithoutPagination));

      const params = { code: '7203' };
      const result = await client.getWeeklyMarginInterest(params);

      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/markets/margin-interest'),
        expect.objectContaining({
          method: 'GET',
          headers: expect.objectContaining({
            'x-api-key': mockJQuantsConfig.apiKey,
          }),
        })
      );
      expect(result).toEqual({ data: mockWeeklyMarginInterestResponse.data });
    });

    it('should throw error when no required parameters provided', async () => {
      await expect(client.getWeeklyMarginInterest({})).rejects.toThrow(
        'At least one of code or date parameters is required'
      );
    });
  });

  describe('error handling', () => {
    beforeEach(() => {
      client = new JQuantsClient(mockJQuantsConfig);
    });

    it('should handle API errors properly', async () => {
      fetchSpy.mockResolvedValueOnce(createMockErrorResponse('API Error', 400));

      await expect(client.getListedInfo()).rejects.toThrow('API Error');
    });

    it('should handle network errors', async () => {
      fetchSpy.mockRejectedValueOnce(new TypeError('Failed to fetch'));

      await expect(client.getListedInfo()).rejects.toThrow();
    });
  });

  describe('new API methods', () => {
    beforeEach(() => {
      client = new JQuantsClient(mockJQuantsConfig);
    });

    describe('getIndices', () => {
      it('should get indices data successfully', async () => {
        // Response without pagination_key to simulate last/only page
        const responseWithoutPagination = { data: mockIndicesResponse.data };
        fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

        const params = { code: '0028', date: '2025-01-10' };
        const result = await client.getIndices(params);

        // makePaginatedRequest returns data without pagination_key
        expect(result).toEqual({ data: mockIndicesResponse.data });
      });
    });

    describe('getTOPIX', () => {
      it('should get TOPIX data successfully', async () => {
        const responseWithoutPagination = { data: mockTOPIXResponse.data };
        fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

        const params = { from: '2025-01-01', to: '2025-01-10' };
        const result = await client.getTOPIX(params);

        expect(result).toEqual({ data: mockTOPIXResponse.data });
      });

      it('should get TOPIX data without parameters', async () => {
        const responseWithoutPagination = { data: mockTOPIXResponse.data };
        fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

        const result = await client.getTOPIX();

        expect(result).toEqual({ data: mockTOPIXResponse.data });
      });
    });

    describe('getTradingCalendar', () => {
      it('should get trading calendar data successfully', async () => {
        fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

        const params = { hol_div: '0', from: '2025-01-01', to: '2025-01-31' };
        const result = await client.getTradingCalendar(params);

        expect(result).toEqual(mockTradingCalendarResponse);
      });

      it('should get trading calendar without parameters', async () => {
        fetchSpy.mockResolvedValue(createMockResponse(mockTradingCalendarResponse));

        const result = await client.getTradingCalendar();

        expect(result).toEqual(mockTradingCalendarResponse);
      });
    });

    describe('specialized client access', () => {
      it('should provide access to stocks client', () => {
        expect(client.stocks).toBeDefined();
        expect(typeof client.stocks.getListedInfo).toBe('function');
      });

      it('should provide access to indices client', () => {
        expect(client.indices).toBeDefined();
        expect(typeof client.indices.getIndices).toBe('function');
        expect(typeof client.indices.getTOPIX).toBe('function');
      });

      it('should provide access to calendar client', () => {
        expect(client.calendar).toBeDefined();
        expect(typeof client.calendar.getTradingCalendar).toBe('function');
      });
    });
  });
});
