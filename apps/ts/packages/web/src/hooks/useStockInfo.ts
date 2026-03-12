import { useQuery } from '@tanstack/react-query';
import { ApiError, apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

export interface StockInfoResponse {
  code: string;
  companyName: string;
  companyNameEnglish?: string;
  marketCode?: string;
  marketName?: string;
  sector17Code?: string;
  sector17Name?: string;
  sector33Code?: string;
  sector33Name?: string;
  scaleCategory?: string;
  listedDate?: string;
}

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
