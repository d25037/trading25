import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockErrorResponse, createMockResponse } from '../../test-utils/fetch-mock';
import { mockJQuantsConfig, mockStatementsResponse } from '../../test-utils/fixtures';
import { resetRateLimiter } from '../base/BaseJQuantsClient';
import { StatementsClient } from './StatementsClient';

describe('StatementsClient', () => {
  let client: StatementsClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    // Reset rate limiter and disable it for tests to avoid timeouts
    resetRateLimiter({ disable: true });
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
    client = new StatementsClient(mockJQuantsConfig);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    // Re-enable rate limiter after tests
    resetRateLimiter();
  });

  describe('getStatements', () => {
    it('should fetch statements by code successfully', async () => {
      // Response without pagination_key to simulate last/only page
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getStatements({ code: '86970' });

      // makePaginatedRequest returns data without pagination_key
      expect(result).toEqual({ data: mockStatementsResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/fins/summary'),
        expect.objectContaining({
          method: 'GET',
          headers: expect.objectContaining({
            'x-api-key': mockJQuantsConfig.apiKey,
          }),
        })
      );
    });

    it('should fetch statements by date successfully', async () => {
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getStatements({ date: '20230130' });

      expect(result).toEqual({ data: mockStatementsResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('date=20230130'), expect.any(Object));
    });

    it('should fetch statements with both code and date successfully', async () => {
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getStatements({
        code: '86970',
        date: '20230130',
      });

      expect(result).toEqual({ data: mockStatementsResponse.data });
      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should handle pagination by fetching all pages', async () => {
      // First call returns data with pagination_key, second call returns without
      const page1 = { data: mockStatementsResponse.data, pagination_key: 'next_page_key' };
      const page2 = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(page1)).mockResolvedValueOnce(createMockResponse(page2));

      const result = await client.getStatements({ code: '86970' });

      // Result should combine data from both pages without pagination_key
      expect(result).toEqual({ data: [...mockStatementsResponse.data, ...mockStatementsResponse.data] });
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    });

    it('should throw error when neither code nor date is provided', async () => {
      await expect(client.getStatements({})).rejects.toThrow('Either code or date parameter is required');

      expect(fetchSpy).not.toHaveBeenCalled();
    });

    it('should handle API errors properly', async () => {
      fetchSpy.mockResolvedValue(createMockErrorResponse('Not found', 404));

      await expect(client.getStatements({ code: '86970' })).rejects.toThrow();
    });

    it('should handle different date formats', async () => {
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockImplementation((() =>
        Promise.resolve(createMockResponse(responseWithoutPagination))) as unknown as typeof fetch);

      // Test YYYY-MM-DD format
      await client.getStatements({ date: '2023-01-30' });
      expect(fetchSpy).toHaveBeenCalled();

      // Test YYYYMMDD format
      await client.getStatements({ date: '20230130' });
      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should handle 5-digit stock codes', async () => {
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      await client.getStatements({ code: '86970' });

      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('code=86970'), expect.any(Object));
    });

    it('should handle 4-digit stock codes', async () => {
      const responseWithoutPagination = { data: mockStatementsResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      await client.getStatements({ code: '7203' });

      expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining('code=7203'), expect.any(Object));
    });
  });
});
