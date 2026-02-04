import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

/**
 * Stock search result item from API
 */
export interface StockSearchResultItem {
  code: string;
  companyName: string;
  companyNameEnglish: string | null;
  marketCode: string;
  marketName: string;
  sector33Name: string;
}

/**
 * Stock search response from API
 */
interface StockSearchResponse {
  query: string;
  results: StockSearchResultItem[];
  count: number;
}

/**
 * Fetch stock search results from API
 */
function fetchStockSearch(query: string, limit = 20): Promise<StockSearchResponse> {
  return apiGet<StockSearchResponse>('/api/chart/stocks/search', { q: query, limit });
}

/**
 * Hook for searching stocks by code or company name
 *
 * @param query - Search query (stock code or company name)
 * @param options - Search options
 * @returns Query result with search results
 */
export function useStockSearch(
  query: string,
  options: {
    limit?: number;
    enabled?: boolean;
    debounceMs?: number;
  } = {}
) {
  const { limit = 20, enabled = true, debounceMs: _debounceMs = 300 } = options;

  // Only search if query has at least 1 character
  const shouldSearch = enabled && query.trim().length >= 1;

  logger.debug('useStockSearch called', { query, limit, shouldSearch });

  return useQuery({
    queryKey: ['stockSearch', query, limit],
    queryFn: () => {
      logger.debug('Fetching stock search results', { query, limit });
      return fetchStockSearch(query, limit);
    },
    enabled: shouldSearch,
    staleTime: 5 * 60 * 1000, // 5 minutes (search results rarely change)
    gcTime: 10 * 60 * 1000, // 10 minutes
    placeholderData: (previousData) => previousData, // Keep previous results while fetching new ones
  });
}
