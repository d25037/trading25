import { useQuery } from '@tanstack/react-query';
import type { FactorRegressionResponse } from '@trading25/contracts/types/api-response-types';
import { analyticsClient } from '@/lib/analytics-client';

interface FactorRegressionOptions {
  lookbackDays?: number;
  enabled?: boolean;
}

function fetchFactorRegression(
  symbol: string,
  options: FactorRegressionOptions = {}
): Promise<FactorRegressionResponse> {
  return analyticsClient.getFactorRegression({
    symbol,
    lookbackDays: options.lookbackDays,
  });
}

export function useFactorRegression(symbol: string | null, options: FactorRegressionOptions = {}) {
  const { lookbackDays = 252, enabled = true } = options;

  return useQuery({
    queryKey: ['factor-regression', symbol, lookbackDays],
    queryFn: () => {
      if (!symbol) throw new Error('Symbol is required');
      return fetchFactorRegression(symbol, { lookbackDays });
    },
    enabled: !!symbol && enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes (analysis data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
