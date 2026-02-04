import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { MarketRankingResponse, RankingParams } from '@/types/ranking';
import { logger } from '@/utils/logger';

function fetchRanking(params: RankingParams): Promise<MarketRankingResponse> {
  return apiGet<MarketRankingResponse>('/api/analytics/ranking', {
    date: params.date,
    limit: params.limit,
    markets: params.markets,
    lookbackDays: params.lookbackDays,
    periodDays: params.periodDays,
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
