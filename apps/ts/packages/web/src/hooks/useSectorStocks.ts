import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

export interface SectorStockItem {
  rank: number;
  code: string;
  companyName: string;
  marketCode: string;
  sector33Name: string;
  currentPrice: number;
  volume: number;
  tradingValue?: number;
  basePrice?: number;
  changeAmount?: number;
  changePercentage?: number;
  lookbackDays?: number;
}

export interface SectorStocksResponse {
  sector33Name?: string;
  sector17Name?: string;
  markets: string[];
  lookbackDays: number;
  sortBy: string;
  sortOrder: string;
  stocks: SectorStockItem[];
  lastUpdated: string;
}

export interface SectorStocksParams {
  sector33Name?: string;
  sector17Name?: string;
  markets?: string;
  lookbackDays?: number;
  sortBy?: 'tradingValue' | 'changePercentage' | 'code';
  sortOrder?: 'asc' | 'desc';
  limit?: number;
}

function fetchSectorStocks(params: SectorStocksParams): Promise<SectorStocksResponse> {
  return apiGet<SectorStocksResponse>('/api/analytics/sector-stocks', {
    sector33Name: params.sector33Name,
    sector17Name: params.sector17Name,
    markets: params.markets,
    lookbackDays: params.lookbackDays,
    sortBy: params.sortBy,
    sortOrder: params.sortOrder,
    limit: params.limit,
  });
}

export function useSectorStocks(params: SectorStocksParams, enabled = true) {
  return useQuery({
    queryKey: ['sector-stocks', params],
    queryFn: () => {
      logger.debug('Fetching sector stocks', { params });
      return fetchSectorStocks(params);
    },
    enabled: enabled && !!(params.sector33Name || params.sector17Name),
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}
