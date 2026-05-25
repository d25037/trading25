import { useQuery } from '@tanstack/react-query';
import type { SectorStockItem, SectorStocksParams, SectorStocksResponse } from '@trading25/api-clients/analytics';
import { analyticsClient } from '@/lib/analytics-client';
import { logger } from '@/utils/logger';

export type { SectorStockItem, SectorStocksParams, SectorStocksResponse };

function fetchSectorStocks(params: SectorStocksParams): Promise<SectorStocksResponse> {
  return analyticsClient.getSectorStocks({
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
