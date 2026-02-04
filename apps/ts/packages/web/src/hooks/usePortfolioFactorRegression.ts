import { useQuery } from '@tanstack/react-query';
import type { ApiPortfolioFactorRegressionResponse } from '@trading25/shared/types/api-types';
import { apiGet } from '@/lib/api-client';

interface PortfolioFactorRegressionOptions {
  lookbackDays?: number;
}

function fetchPortfolioFactorRegression(
  portfolioId: number,
  options: PortfolioFactorRegressionOptions = {}
): Promise<ApiPortfolioFactorRegressionResponse> {
  return apiGet<ApiPortfolioFactorRegressionResponse>(`/api/analytics/portfolio-factor-regression/${portfolioId}`, {
    lookbackDays: options.lookbackDays,
  });
}

export function usePortfolioFactorRegression(
  portfolioId: number | null,
  options: PortfolioFactorRegressionOptions = {}
) {
  const { lookbackDays = 252 } = options;

  return useQuery({
    queryKey: ['portfolio-factor-regression', portfolioId, lookbackDays],
    queryFn: () => {
      if (portfolioId === null) throw new Error('Portfolio ID is required');
      return fetchPortfolioFactorRegression(portfolioId, { lookbackDays });
    },
    enabled: portfolioId !== null,
    staleTime: 10 * 60 * 1000, // 10 minutes (analysis data changes infrequently)
    gcTime: 30 * 60 * 1000, // 30 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
