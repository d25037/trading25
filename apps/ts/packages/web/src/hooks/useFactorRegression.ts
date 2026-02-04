import { useQuery } from '@tanstack/react-query';
import type { ApiFactorRegressionResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

interface FactorRegressionOptions {
  lookbackDays?: number;
}

function fetchFactorRegression(
  symbol: string,
  options: FactorRegressionOptions = {}
): Promise<ApiFactorRegressionResponse> {
  return apiGet<ApiFactorRegressionResponse>(`/api/analytics/factor-regression/${symbol}`, {
    lookbackDays: options.lookbackDays,
  });
}

export function useFactorRegression(symbol: string | null, options: FactorRegressionOptions = {}) {
  const { lookbackDays = 252 } = options;

  return useQuery({
    queryKey: ['factor-regression', symbol, lookbackDays],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchFactorRegression(symbol, { lookbackDays });
    },
    enabled: !!symbol,
    staleTime: 10 * 60 * 1000, // 10 minutes (analysis data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
