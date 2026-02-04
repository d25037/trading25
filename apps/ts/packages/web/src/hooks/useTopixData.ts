import { useQuery } from '@tanstack/react-query';
import type { JQuantsTOPIXResponse } from '@trading25/shared';
import { apiGet } from '@/lib/api-client';
import { logger } from '@/utils/logger';

interface UseTopixDataOptions {
  from?: string;
  to?: string;
  date?: string;
  enabled?: boolean;
}

const CACHE_TIME = 30 * 60 * 1000; // 30 minutes
const STALE_TIME = 15 * 60 * 1000; // 15 minutes

async function fetchTopixData(options: UseTopixDataOptions = {}): Promise<JQuantsTOPIXResponse> {
  const { from, to, date } = options;

  logger.debug('Fetching TOPIX data', { from, to, date });

  const data = await apiGet<JQuantsTOPIXResponse>('/api/chart/indices/topix', { from, to, date });
  logger.debug('TOPIX data fetched', { dataPoints: data.data?.length || 0 });

  return data;
}

export function useTopixData(options: UseTopixDataOptions = {}) {
  const { from, to, date, enabled = true } = options;

  const queryKey = ['topix', { from, to, date }];

  return useQuery({
    queryKey,
    queryFn: () => fetchTopixData({ from, to, date }),
    enabled,
    staleTime: STALE_TIME,
    gcTime: CACHE_TIME, // React Query v5 uses gcTime instead of cacheTime
    retry: (failureCount, error) => {
      // Don't retry on client errors (4xx)
      if (error instanceof Error && error.message.includes('HTTP 4')) {
        return false;
      }
      // Retry up to 3 times for server errors
      return failureCount < 3;
    },
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
  });
}

// Hook for getting all TOPIX data (no date filters)
export function useAllTopixData(enabled = true) {
  return useTopixData({ enabled });
}

// Hook for getting TOPIX data within a date range
export function useTopixDateRange(from: string, to: string, enabled = true) {
  return useTopixData({ from, to, enabled });
}
