import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { MarketScreeningResponse, ScreeningParams } from '@/types/screening';
import { logger } from '@/utils/logger';

function fetchScreening(params: ScreeningParams): Promise<MarketScreeningResponse> {
  return apiGet<MarketScreeningResponse>('/api/analytics/screening', {
    markets: params.markets,
    strategies: params.strategies,
    recentDays: params.recentDays,
    date: params.date,
    backtestMetric: params.backtestMetric,
    sortBy: params.sortBy,
    order: params.order,
    limit: params.limit,
  });
}

export function useScreening(params: ScreeningParams, enabled = true) {
  return useQuery({
    queryKey: ['screening', params],
    queryFn: () => {
      logger.debug('Fetching screening data', { params });
      return fetchScreening(params);
    },
    enabled,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}
