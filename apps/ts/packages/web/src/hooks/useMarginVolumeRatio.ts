import { useQuery } from '@tanstack/react-query';
import type { ApiMarginVolumeRatioResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

function fetchMarginVolumeRatio(symbol: string): Promise<ApiMarginVolumeRatioResponse> {
  return apiGet<ApiMarginVolumeRatioResponse>(`/api/analytics/stocks/${symbol}/margin-ratio`);
}

export function useMarginVolumeRatio(symbol: string | null) {
  return useQuery({
    queryKey: ['margin-volume-ratio', symbol],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchMarginVolumeRatio(symbol);
    },
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000, // 5分間データを新鮮とみなす
    gcTime: 10 * 60 * 1000, // 10分間キャッシュ保持
    retry: 3,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
  });
}
