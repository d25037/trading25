import { useQuery } from '@tanstack/react-query';
import type { components } from '@trading25/contracts/clients/backtest/generated/bt-api-types';
import { analyticsClient } from '@/lib/analytics-client';
import { logger } from '@/utils/logger';

export type SectorStockItem = components['schemas']['SectorStockItem'];
export type SectorStocksResponse = components['schemas']['SectorStocksResponse'];

export interface SectorStocksParams {
  sector33Name?: string;
  sector17Name?: string;
  markets?: string;
  lookbackDays?: number;
  sortBy?: 'tradingValue' | 'changePercentage' | 'code' | 'per' | 'forwardPer' | 'pbr' | 'marketCap';
  sortOrder?: 'asc' | 'desc';
  limit?: number;
}

function fetchSectorStocks(params: SectorStocksParams): Promise<SectorStocksResponse> {
  return analyticsClient.getSectorStocks<SectorStocksResponse>({
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
