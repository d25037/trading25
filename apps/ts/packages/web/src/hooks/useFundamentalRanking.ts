import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { FundamentalRankingParams, MarketFundamentalRankingResponse } from '@/types/fundamentalRanking';
import { logger } from '@/utils/logger';

function fetchFundamentalRanking(params: FundamentalRankingParams): Promise<MarketFundamentalRankingResponse> {
  return apiGet<MarketFundamentalRankingResponse>('/api/analytics/fundamental-ranking', {
    limit: params.limit,
    markets: params.markets,
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
