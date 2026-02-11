import { useQuery } from '@tanstack/react-query';
import type { ApiFundamentalsResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

function normalizeTradingValuePeriod(period: number): number {
  if (!Number.isFinite(period)) return 15;
  return Math.max(1, Math.trunc(period));
}

function fetchFundamentals(symbol: string, tradingValuePeriod: number): Promise<ApiFundamentalsResponse> {
  return apiGet<ApiFundamentalsResponse>(`/api/analytics/fundamentals/${symbol}`, {
    tradingValuePeriod,
  });
}

interface UseFundamentalsOptions {
  enabled?: boolean;
  tradingValuePeriod?: number;
}

const FUNDAMENTALS_QUERY_KEY_VERSION = 'v2';

export function useFundamentals(symbol: string | null, options: UseFundamentalsOptions = {}) {
  const { enabled = true, tradingValuePeriod = 15 } = options;
  const normalizedTradingValuePeriod = normalizeTradingValuePeriod(tradingValuePeriod);

  return useQuery({
    queryKey: ['fundamentals', FUNDAMENTALS_QUERY_KEY_VERSION, symbol, normalizedTradingValuePeriod],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchFundamentals(symbol, normalizedTradingValuePeriod);
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes (financial data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
