import { afterEach, beforeEach, describe, expect, it, type Mock, spyOn } from 'bun:test';
import { createMockResponse } from '../../test-utils/fetch-mock';
import { mockIndicesResponse, mockJQuantsConfig, mockTOPIXResponse } from '../../test-utils/fixtures';
import { resetRateLimiter } from '../base/BaseJQuantsClient';
import { IndexClient } from './IndexClient';

describe('IndexClient', () => {
  let client: IndexClient;
  let fetchSpy: Mock<typeof fetch>;

  beforeEach(() => {
    // Reset rate limiter and disable it for tests to avoid timeouts
    resetRateLimiter({ disable: true });
    fetchSpy = spyOn(globalThis, 'fetch').mockImplementation((() =>
      Promise.resolve(createMockResponse({ data: [] }))) as unknown as typeof fetch);
    client = new IndexClient(mockJQuantsConfig);
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    // Re-enable rate limiter after tests
    resetRateLimiter();
  });

  describe('getIndices', () => {
    it('should fetch indices successfully with code', async () => {
      // Mock response without pagination_key to simulate last page
      const responseWithoutPagination = { data: mockIndicesResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { code: '0028', date: '2025-01-10' };
      const result = await client.getIndices(params);

      // makePaginatedRequest combines all data and removes pagination_key
      expect(result).toEqual({ data: mockIndicesResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/indices/bars/daily'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should fetch indices with date range', async () => {
      const responseWithoutPagination = { data: mockIndicesResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { code: '0028', from: '2025-01-01', to: '2025-01-10' };
      const result = await client.getIndices(params);

      expect(result).toEqual({ data: mockIndicesResponse.data });
      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should throw error when no parameters provided', async () => {
      await expect(client.getIndices({})).rejects.toThrow('At least one of code or date parameters is required');
    });

    it('should handle pagination by fetching all pages', async () => {
      // First call returns data with pagination_key, second call returns without
      const page1 = { data: mockIndicesResponse.data, pagination_key: 'next_key_123' };
      const page2 = { data: mockIndicesResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(page1)).mockResolvedValueOnce(createMockResponse(page2));

      const params = { code: '0028' };
      const result = await client.getIndices(params);

      // Result should combine data from both pages without pagination_key
      expect(result).toEqual({ data: [...mockIndicesResponse.data, ...mockIndicesResponse.data] });
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('getTOPIX', () => {
    it('should fetch TOPIX successfully without parameters', async () => {
      const responseWithoutPagination = { data: mockTOPIXResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const result = await client.getTOPIX();

      // makePaginatedRequest returns data without pagination_key
      expect(result).toEqual({ data: mockTOPIXResponse.data });
      expect(fetchSpy).toHaveBeenCalledWith(
        expect.stringContaining('/indices/bars/daily/topix'),
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should fetch TOPIX with date range parameters', async () => {
      const responseWithoutPagination = { data: mockTOPIXResponse.data };
      fetchSpy.mockResolvedValue(createMockResponse(responseWithoutPagination));

      const params = { from: '2025-01-01', to: '2025-01-10' };
      const result = await client.getTOPIX(params);

      expect(result).toEqual({ data: mockTOPIXResponse.data });
      expect(fetchSpy).toHaveBeenCalled();
    });

    it('should handle pagination by fetching all pages', async () => {
      // First call returns data with pagination_key, second call returns without
      const page1 = { data: mockTOPIXResponse.data, pagination_key: 'topix_key_456' };
      const page2 = { data: mockTOPIXResponse.data };
      fetchSpy.mockResolvedValueOnce(createMockResponse(page1)).mockResolvedValueOnce(createMockResponse(page2));

      const result = await client.getTOPIX();

      // Result should combine data from both pages without pagination_key
      expect(result).toEqual({ data: [...mockTOPIXResponse.data, ...mockTOPIXResponse.data] });
      expect(fetchSpy).toHaveBeenCalledTimes(2);
    });
  });
});
