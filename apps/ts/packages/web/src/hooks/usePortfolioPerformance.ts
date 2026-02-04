import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';

/**
 * Portfolio summary metrics
 */
export interface PortfolioSummary {
  totalCost: number;
  currentValue: number;
  totalPnL: number;
  returnRate: number;
}

/**
 * Individual holding performance
 */
export interface HoldingPerformance {
  code: string;
  companyName: string;
  quantity: number;
  purchasePrice: number;
  currentPrice: number;
  cost: number;
  marketValue: number;
  pnl: number;
  returnRate: number;
  weight: number;
  purchaseDate: string;
  account?: string;
}

/**
 * Performance time series data point
 */
export interface PerformanceDataPoint {
  date: string;
  dailyReturn: number;
  cumulativeReturn: number;
}

/**
 * Benchmark metrics
 */
export interface BenchmarkMetrics {
  code: string;
  name: string;
  beta: number;
  alpha: number;
  correlation: number;
  rSquared: number;
  benchmarkReturn: number;
  relativeReturn: number;
}

/**
 * Benchmark time series data point
 */
export interface BenchmarkDataPoint {
  date: string;
  portfolioReturn: number;
  benchmarkReturn: number;
}

/**
 * Portfolio performance response
 */
export interface PortfolioPerformanceResponse {
  portfolioId: number;
  portfolioName: string;
  portfolioDescription?: string;
  summary: PortfolioSummary;
  holdings: HoldingPerformance[];
  timeSeries: PerformanceDataPoint[];
  benchmark: BenchmarkMetrics | null;
  benchmarkTimeSeries: BenchmarkDataPoint[] | null;
  analysisDate: string;
  dateRange: { from: string; to: string } | null;
  dataPoints: number;
  warnings: string[];
}

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
