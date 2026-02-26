import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import { logger } from '@/utils/logger';

function normalizeLookbackFyCount(value: number | undefined): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 3;
  return Math.min(20, Math.max(1, Math.trunc(value)));
}

function fetchFundamentalRanking(params: FundamentalRankingParams): Promise<MarketFundamentalRankingResponse> {
  const forecastAboveRecentFyActuals = params.forecastAboveRecentFyActuals ?? params.forecastAboveAllActuals ?? false;
  const forecastLookbackFyCount = normalizeLookbackFyCount(params.forecastLookbackFyCount);

  return apiGet<MarketFundamentalRankingResponse>('/api/analytics/fundamental-ranking', {
    limit: params.limit,
    markets: params.markets,
    forecastAboveRecentFyActuals,
    forecastLookbackFyCount,
  });
}

export function useFundamentalRanking(params: FundamentalRankingParams, enabled = true) {
  return useQuery({
    queryKey: ['fundamental-ranking', params],
    queryFn: () => {
      logger.debug('Fetching fundamental ranking data', { params });
      return fetchFundamentalRanking(params);
    },
    enabled,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}
