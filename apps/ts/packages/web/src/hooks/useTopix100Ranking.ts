import { useQuery } from '@tanstack/react-query';
import { analyticsClient } from '@/lib/analytics-client';
import type { Topix100PriceSmaWindow, Topix100RankingMetric, Topix100RankingResponse } from '@/types/ranking';
import { logger } from '@/utils/logger';

function fetchTopix100Ranking(
  date?: string,
  metric?: Topix100RankingMetric,
  smaWindow?: Topix100PriceSmaWindow
): Promise<Topix100RankingResponse> {
  return analyticsClient.getTopix100Ranking({ date, metric, smaWindow });
}

export function useTopix100Ranking(
  date?: string,
  metric?: Topix100RankingMetric,
  smaWindow?: Topix100PriceSmaWindow,
  enabled = true
) {
  return useQuery({
    queryKey: ['topix100-ranking', date, metric, smaWindow],
    queryFn: () => {
      logger.debug('Fetching TOPIX100 ranking data', { date, metric, smaWindow });
      return fetchTopix100Ranking(date, metric, smaWindow);
    },
    enabled,
    staleTime: 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}
