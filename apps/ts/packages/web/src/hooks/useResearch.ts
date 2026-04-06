import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type { ResearchCatalogResponse, ResearchDetailResponse } from '@/types/research';
import { logger } from '@/utils/logger';

function fetchResearchCatalog(): Promise<ResearchCatalogResponse> {
  logger.debug('Fetching research catalog');
  return apiGet<ResearchCatalogResponse>('/api/analytics/research');
}

function fetchResearchDetail(experimentId: string, runId?: string | null): Promise<ResearchDetailResponse> {
  logger.debug('Fetching research detail', { experimentId, runId: runId ?? null });
  return apiGet<ResearchDetailResponse>('/api/analytics/research/detail', {
    experimentId,
    runId: runId ?? undefined,
  });
}

export function useResearchCatalog() {
  return useQuery({
    queryKey: ['research-catalog'],
    queryFn: fetchResearchCatalog,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
  });
}

export function useResearchDetail(experimentId: string | null, runId?: string | null) {
  return useQuery({
    queryKey: ['research-detail', experimentId, runId ?? null],
    queryFn: () => {
      if (!experimentId) {
        throw new Error('experimentId is required');
      }
      return fetchResearchDetail(experimentId, runId);
    },
    enabled: Boolean(experimentId),
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
  });
}
