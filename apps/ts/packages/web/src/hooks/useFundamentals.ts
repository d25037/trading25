import { useQuery } from '@tanstack/react-query';
import type { ApiFundamentalsResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

function fetchFundamentals(symbol: string): Promise<ApiFundamentalsResponse> {
  return apiGet<ApiFundamentalsResponse>(`/api/analytics/fundamentals/${symbol}`);
}

interface UseFundamentalsOptions {
  enabled?: boolean;
}

export function useFundamentals(symbol: string | null, options: UseFundamentalsOptions = {}) {
  const { enabled = true } = options;

  return useQuery({
    queryKey: ['fundamentals', symbol],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchFundamentals(symbol);
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes (financial data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
