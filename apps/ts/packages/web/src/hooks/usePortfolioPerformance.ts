import { useQuery } from '@tanstack/react-query';
import type {
  PortfolioBenchmarkMetrics,
  PortfolioBenchmarkPoint,
  PortfolioHoldingPerformance,
  PortfolioPerformancePoint,
  PortfolioPerformanceResponse,
  PortfolioPerformanceSummary,
} from '@trading25/contracts/types/api-response-types';
import { apiGet } from '@/lib/api-client';

export type PortfolioSummary = PortfolioPerformanceSummary;
export type HoldingPerformance = PortfolioHoldingPerformance;
export type PerformanceDataPoint = PortfolioPerformancePoint;
export type BenchmarkMetrics = PortfolioBenchmarkMetrics;
export type BenchmarkDataPoint = PortfolioBenchmarkPoint;
export type { PortfolioPerformanceResponse };

function fetchPortfolioPerformance(
  portfolioId: number,
  benchmarkCode = '0000',
  lookbackDays = 252
): Promise<PortfolioPerformanceResponse> {
  return apiGet<PortfolioPerformanceResponse>(`/api/portfolio/${portfolioId}/performance`, {
    benchmarkCode,
    lookbackDays,
  });
}

interface UsePortfolioPerformanceOptions {
  benchmarkCode?: string;
  lookbackDays?: number;
}

export function usePortfolioPerformance(portfolioId: number | null, options: UsePortfolioPerformanceOptions = {}) {
  const { benchmarkCode = '0000', lookbackDays = 252 } = options;

  return useQuery({
    queryKey: ['portfolio-performance', portfolioId, benchmarkCode, lookbackDays],
    queryFn: () => {
      if (!portfolioId) throw new Error('Portfolio ID is required');
      return fetchPortfolioPerformance(portfolioId, benchmarkCode, lookbackDays);
    },
    enabled: portfolioId !== null,
    staleTime: 1 * 60 * 1000, // 1 minute (market data changes frequently)
    gcTime: 5 * 60 * 1000, // 5 minutes cache
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  });
}
