import { useQuery } from '@tanstack/react-query';
import { apiGet } from '@/lib/api-client';
import type {
  ApiPublishedResearchSummary,
  ApiResearchCatalogItem,
  ApiResearchCatalogResponse,
  ApiResearchDetailResponse,
  ApiResearchRunReference,
  PublishedResearchSummary,
  ResearchCatalogItem,
  ResearchCatalogResponse,
  ResearchDetailResponse,
  ResearchHighlight,
  ResearchRunReference,
} from '@/types/research';
import { logger } from '@/utils/logger';

function normalizeResearchCatalogItem(item: ApiResearchCatalogItem): ResearchCatalogItem {
  return {
    ...item,
    tags: item.tags ?? [],
  };
}

function normalizeResearchRunReference(item: ApiResearchRunReference): ResearchRunReference {
  return {
    ...item,
    isLatest: item.isLatest ?? false,
  };
}

function normalizeResearchSummary(summary: ApiPublishedResearchSummary | null | undefined): PublishedResearchSummary | null {
  if (!summary) {
    return null;
  }

  return {
    ...summary,
    tags: summary.tags ?? [],
    method: summary.method ?? [],
    resultBullets: summary.resultBullets ?? [],
    considerations: summary.considerations ?? [],
    selectedParameters: summary.selectedParameters ?? [],
    highlights: (summary.highlights ?? []).map(
      (item): ResearchHighlight => ({
        ...item,
        tone: item.tone ?? 'neutral',
      })
    ),
    tableHighlights: summary.tableHighlights ?? [],
  };
}

function normalizeResearchCatalogResponse(response: ApiResearchCatalogResponse): ResearchCatalogResponse {
  return {
    lastUpdated: response.lastUpdated,
    items: (response.items ?? []).map(normalizeResearchCatalogItem),
  };
}

function normalizeResearchDetailResponse(response: ApiResearchDetailResponse): ResearchDetailResponse {
  return {
    ...response,
    item: normalizeResearchCatalogItem(response.item),
    summary: normalizeResearchSummary(response.summary),
    outputTables: response.outputTables ?? [],
    availableRuns: (response.availableRuns ?? []).map(normalizeResearchRunReference),
    resultMetadata: response.resultMetadata ?? {},
  };
}

function fetchResearchCatalog(): Promise<ResearchCatalogResponse> {
  logger.debug('Fetching research catalog');
  return apiGet<ApiResearchCatalogResponse>('/api/analytics/research').then(normalizeResearchCatalogResponse);
}

function fetchResearchDetail(experimentId: string, runId?: string | null): Promise<ResearchDetailResponse> {
  logger.debug('Fetching research detail', { experimentId, runId: runId ?? null });
  return apiGet<ApiResearchDetailResponse>('/api/analytics/research/detail', {
    experimentId,
    runId: runId ?? undefined,
  }).then(normalizeResearchDetailResponse);
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
