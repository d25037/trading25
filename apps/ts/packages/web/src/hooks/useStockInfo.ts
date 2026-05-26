import { useQuery } from '@tanstack/react-query';
import type { StockInfoResponse } from '@trading25/contracts/types/api-response-types';
import { ApiError, apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

export type { StockInfoResponse };

export const stockInfoKeys = {
  detail: (symbol: string) => ['stock-info', symbol] as const,
};

function fetchStockInfo(symbol: string): Promise<StockInfoResponse> {
  return apiGet<StockInfoResponse>(`/api/market/stocks/${symbol}`);
}

export function useStockInfo(symbol: string | null) {
  logger.debug('useStockInfo called', { symbol });

  return useQuery({
    queryKey: stockInfoKeys.detail(symbol ?? ''),
    queryFn: () => {
      if (!symbol) {
        throw new Error('Symbol is required');
      }
      return fetchStockInfo(symbol);
    },
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000,
    gcTime: 15 * 60 * 1000,
    retry: (failureCount, error) => {
      if (error instanceof ApiError && error.isClientError()) {
        return false;
      }
      return failureCount < 2;
    },
  });
}
