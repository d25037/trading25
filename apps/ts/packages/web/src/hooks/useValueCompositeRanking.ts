import { useQuery } from '@tanstack/react-query';
import { analyticsClient } from '@/lib/analytics-client';
import type {
  ValueCompositeRankingParams,
  ValueCompositeRankingResponse,
} from '@/types/valueCompositeRanking';
import { logger } from '@/utils/logger';

function fetchValueCompositeRanking(params: ValueCompositeRankingParams): Promise<ValueCompositeRankingResponse> {
  const query = {
    date: params.date,
    limit: params.limit,
    markets: params.markets,
    scoreMethod: params.scoreMethod,
  };

  logger.debug('Fetching value composite ranking data', { query });
  return analyticsClient.getValueCompositeRanking(query);
}

export function useValueCompositeRanking(params: ValueCompositeRankingParams, enabled = true) {
  return useQuery({
    queryKey: ['value-composite-ranking', params],
    queryFn: () => fetchValueCompositeRanking(params),
    enabled,
    staleTime: 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}
