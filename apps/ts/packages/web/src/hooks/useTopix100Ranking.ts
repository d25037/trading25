import { useQuery } from '@tanstack/react-query';
import { analyticsClient } from '@/lib/analytics-client';
import type { Topix100RankingResponse } from '@/types/ranking';
import { logger } from '@/utils/logger';

function fetchTopix100Ranking(date?: string): Promise<Topix100RankingResponse> {
  return analyticsClient.getTopix100Ranking({ date });
}

export function useTopix100Ranking(date?: string, enabled = true) {
  return useQuery({
    queryKey: ['topix100-ranking', date],
    queryFn: () => {
      logger.debug('Fetching TOPIX100 ranking data', { date });
      return fetchTopix100Ranking(date);
    },
    enabled,
    staleTime: 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}
