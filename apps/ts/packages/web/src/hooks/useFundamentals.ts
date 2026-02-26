import { useQuery } from '@tanstack/react-query';
import type { ApiFundamentalsResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

function normalizeTradingValuePeriod(period: number): number {
  if (!Number.isFinite(period)) return 15;
  return Math.max(1, Math.trunc(period));
}

function normalizeForecastEpsLookbackFyCount(count: number): number {
  if (!Number.isFinite(count)) return 3;
  return Math.min(20, Math.max(1, Math.trunc(count)));
}

function fetchFundamentals(
  symbol: string,
  tradingValuePeriod: number,
  forecastEpsLookbackFyCount: number
): Promise<ApiFundamentalsResponse> {
  return apiGet<ApiFundamentalsResponse>(`/api/analytics/fundamentals/${symbol}`, {
    tradingValuePeriod,
    forecastEpsLookbackFyCount,
  });
}

interface UseFundamentalsOptions {
  enabled?: boolean;
  tradingValuePeriod?: number;
  forecastEpsLookbackFyCount?: number;
}

const FUNDAMENTALS_QUERY_KEY_VERSION = 'v2';

export function useFundamentals(symbol: string | null, options: UseFundamentalsOptions = {}) {
  const { enabled = true, tradingValuePeriod = 15, forecastEpsLookbackFyCount = 3 } = options;
  const normalizedTradingValuePeriod = normalizeTradingValuePeriod(tradingValuePeriod);
  const normalizedForecastEpsLookbackFyCount = normalizeForecastEpsLookbackFyCount(
    forecastEpsLookbackFyCount
  );

  return useQuery({
    queryKey: [
      'fundamentals',
      FUNDAMENTALS_QUERY_KEY_VERSION,
      symbol,
      normalizedTradingValuePeriod,
      normalizedForecastEpsLookbackFyCount,
    ],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchFundamentals(
        symbol,
        normalizedTradingValuePeriod,
        normalizedForecastEpsLookbackFyCount
      );
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes (financial data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
