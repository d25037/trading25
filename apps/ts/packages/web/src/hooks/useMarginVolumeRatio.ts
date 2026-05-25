import { useQuery } from '@tanstack/react-query';
import type { MarginVolumeRatioResponse } from '@trading25/api-clients/analytics';
import { analyticsClient } from '@/lib/analytics-client';

function fetchMarginVolumeRatio(symbol: string): Promise<MarginVolumeRatioResponse> {
  return analyticsClient.getMarginVolumeRatio({ symbol });
}

export function useMarginVolumeRatio(symbol: string | null) {
  return useQuery({
    queryKey: ['margin-volume-ratio', symbol],
    queryFn: () => fetchMarginVolumeRatio(symbol as string),
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000, // 5分間データを新鮮とみなす
    gcTime: 10 * 60 * 1000, // 10分間キャッシュ保持
    retry: 3,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
  });
}
