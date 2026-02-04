import { useQuery } from '@tanstack/react-query';
import type { ApiMarginPressureIndicatorsResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

function fetchMarginPressureIndicators(symbol: string, period: number): Promise<ApiMarginPressureIndicatorsResponse> {
  return apiGet<ApiMarginPressureIndicatorsResponse>(`/api/analytics/stocks/${symbol}/margin-pressure`, {
    period,
  });
}

export function useMarginPressureIndicators(symbol: string | null, period = 15) {
  return useQuery({
    queryKey: ['margin-pressure-indicators', symbol, period],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchMarginPressureIndicators(symbol, period);
    },
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes cache
    retry: 3,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
  });
}
