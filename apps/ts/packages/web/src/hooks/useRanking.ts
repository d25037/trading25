import { useQuery } from '@tanstack/react-query';
import { analyticsClient } from '@/lib/analytics-client';
import type { MarketRankingResponse, RankingParams } from '@/types/ranking';
import { logger } from '@/utils/logger';

function fetchRanking(params: RankingParams): Promise<MarketRankingResponse> {
  return analyticsClient.getMarketRanking({
    date: params.date,
    limit: params.limit,
    markets: params.markets,
    lookbackDays: params.lookbackDays,
    periodDays: params.periodDays,
    sector33Name: params.sector33Name,
    sector17Name: params.sector17Name,
    includeValuation: params.includeValuation,
    forwardEpsDisclosedWithinDays: params.forwardEpsDisclosedWithinDays,
  });
}

export function useRanking(params: RankingParams, enabled = true) {
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
