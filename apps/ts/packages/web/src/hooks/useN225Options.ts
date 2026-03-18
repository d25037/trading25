import { useQuery } from '@tanstack/react-query';
import { ApiError, apiGet } from '@/lib/api-client';
import type { N225OptionsExplorerResponse } from '@/types/options225';
import { logger } from '@/utils/logger';

interface UseN225OptionsOptions {
  date?: string;
}

const CACHE_TIME = 30 * 60 * 1000;
const STALE_TIME = 5 * 60 * 1000;

async function fetchN225Options(options: UseN225OptionsOptions = {}): Promise<N225OptionsExplorerResponse> {
  const { date } = options;
  logger.debug('Fetching N225 options data', { date });
  const data = await apiGet<N225OptionsExplorerResponse>('/api/jquants/options/225', { date });
  logger.debug('N225 options data fetched', { resolvedDate: data.resolvedDate, itemCount: data.items.length });
  return data;
}

export function useN225Options(options: UseN225OptionsOptions = {}) {
  const { date } = options;

  return useQuery({
    queryKey: ['n225-options', { date }],
    queryFn: () => fetchN225Options({ date }),
    staleTime: STALE_TIME,
    gcTime: CACHE_TIME,
    retry: (failureCount, error) => {
      if (error instanceof ApiError && error.isClientError()) {
        return false;
      }
      return failureCount < 2;
    },
  });
}
