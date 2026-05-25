import { useQuery } from '@tanstack/react-query';
import type { ValueCompositeScoreResponse } from '@trading25/contracts/types/api-response-types';
import { analyticsClient } from '@/lib/analytics-client';

interface ValueCompositeScoreParams {
  symbol: string;
  date?: string;
  forwardEpsMode?: ValueCompositeScoreResponse['forwardEpsMode'];
}

function fetchValueCompositeScore(params: ValueCompositeScoreParams): Promise<ValueCompositeScoreResponse> {
  return analyticsClient.getValueCompositeScore(params);
}

interface UseValueCompositeScoreOptions {
  enabled?: boolean;
  date?: string;
  forwardEpsMode?: ValueCompositeScoreParams['forwardEpsMode'];
}

export function useValueCompositeScore(symbol: string | null, options: UseValueCompositeScoreOptions = {}) {
  const { enabled = true, date, forwardEpsMode = 'latest' } = options;

  return useQuery({
    queryKey: ['value-composite-score', symbol, date ?? null, forwardEpsMode],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchValueCompositeScore({ symbol, date, forwardEpsMode });
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    retry: 2,
  });
}
