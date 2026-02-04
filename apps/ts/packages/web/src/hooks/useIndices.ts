import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { IndexDataResponse, IndicesListResponse } from '@/types/indices';
import { logger } from '@/utils/logger';

function fetchIndicesList(): Promise<IndicesListResponse> {
  return apiGet<IndicesListResponse>('/api/chart/indices');
}

function fetchIndexData(code: string): Promise<IndexDataResponse> {
  return apiGet<IndexDataResponse>(`/api/chart/indices/${code}`);
}

export function useIndicesList() {
  return useQuery({
    queryKey: ['indices-list'],
    queryFn: () => {
      logger.debug('Fetching indices list');
      return fetchIndicesList();
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 30 * 60 * 1000, // 30 minutes
  });
}

export function useIndexData(code: string | null) {
  return useQuery({
    queryKey: ['index-data', code],
    queryFn: () => {
      if (!code) {
        throw new Error('Index code is required');
      }
      logger.debug('Fetching index data', { code });
      return fetchIndexData(code);
    },
    enabled: !!code,
    staleTime: 60 * 1000, // 1 minute
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}
