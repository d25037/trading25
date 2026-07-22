import { useQuery } from '@tanstack/react-query';
import { analyticsClient } from '@/lib/analytics-client';
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import { logger } from '@/utils/logger';

function normalizeLookbackFyCount(value: number | undefined): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 3;
  return Math.min(20, Math.max(1, Math.trunc(value)));
}

function fetchFundamentalRanking(params: FundamentalRankingParams): Promise<MarketFundamentalRankingResponse> {
  const forecastAboveRecentFyActuals = params.forecastAboveRecentFyActuals ?? false;
  const query: FundamentalRankingParams = {
    limit: params.limit,
    markets: params.markets,
    metricKey: params.metricKey,
    forecastAboveRecentFyActuals,
    forecastLookbackFyCount: forecastAboveRecentFyActuals
      ? normalizeLookbackFyCount(params.forecastLookbackFyCount)
      : undefined,
  };

  logger.debug('Fetching fundamental ranking data', { query });
  return analyticsClient.getFundamentalRanking(query);
}

export function useFundamentalRanking(params: FundamentalRankingParams, enabled = true) {
  return useQuery({
    queryKey: ['fundamental-ranking', params],
    queryFn: () => fetchFundamentalRanking(params),
    enabled,
    staleTime: 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}
