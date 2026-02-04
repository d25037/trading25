import { useQuery } from '@tanstack/react-query';
import type { ApiStockDataResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

// Map frontend timeframe values to API timeframe values
const timeframeMap: Record<string, string> = {
  '1D': 'daily',
  '1W': 'weekly',
  '1M': 'monthly',
  '3M': '3months',
  '1Y': 'yearly',
};

function fetchStockData(symbol: string, timeframe: string): Promise<ApiStockDataResponse> {
  const apiTimeframe = timeframeMap[timeframe] || timeframe;
  return apiGet<ApiStockDataResponse>(`/api/chart/stocks/${symbol}`, { timeframe: apiTimeframe });
}

export function useStockData(symbol: string | null, timeframe: string) {
  logger.debug('useStockData called', { symbol, timeframe });

  return useQuery({
    queryKey: ['stockData', symbol, timeframe],
    queryFn: () => {
      logger.debug('Fetching stock data', { symbol, timeframe });
      if (!symbol) {
        throw new Error('Symbol is required');
      }
      return fetchStockData(symbol, timeframe);
    },
    enabled: !!symbol,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}

// Hook for fetching multiple symbols
export function useStockDataMultiple(symbols: string[], timeframe: string) {
  return useQuery({
    queryKey: ['stockDataMultiple', symbols, timeframe],
    queryFn: async () => {
      const results = await Promise.allSettled(symbols.map((symbol) => fetchStockData(symbol, timeframe)));

      return results.map((result, index) => ({
        symbol: symbols[index],
        success: result.status === 'fulfilled',
        data: result.status === 'fulfilled' ? result.value : null,
        error: result.status === 'rejected' ? result.reason : null,
      }));
    },
    enabled: symbols.length > 0,
  });
}
