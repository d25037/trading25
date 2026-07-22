import { useQuery } from '@tanstack/react-query';
import type { MarketRankingParams } from '@trading25/api-clients/analytics';
import type { MarketRankingResponse } from '@trading25/contracts/types/api-response-types';
import { analyticsClient } from '@/lib/analytics-client';
import { logger } from '@/utils/logger';

function fetchRanking(params: MarketRankingParams): Promise<MarketRankingResponse> {
  return analyticsClient.getMarketRanking(params);
}

export function useRanking(params: MarketRankingParams, enabled = true) {
  return useQuery({
    queryKey: ['ranking', params],
    queryFn: () => {
      logger.debug('Fetching ranking data', { params });
      return fetchRanking(params);
    },
    enabled,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}
