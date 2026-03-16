import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, apiDelete, apiGet, apiPost } from '@/lib/api-client';
import type {
  CancelDatasetJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateRequest,
  DatasetDeleteResponse,
  DatasetInfoResponse,
  DatasetListItem,
  DatasetJobResponse,
  DatasetListResponse,
} from '@/types/dataset';
import { logger } from '@/utils/logger';

export const datasetKeys = {
  all: ['dataset'] as const,
  list: () => [...datasetKeys.all, 'list'] as const,
  info: (name: string) => [...datasetKeys.all, 'info', name] as const,
  job: (jobId: string) => [...datasetKeys.all, 'job', jobId] as const,
};

interface LegacyDatasetInfoResponse {
  name: string;
  path: string;
  fileSize: number;
  lastModified: string;
  snapshot: {
    preset?: string | null;
    createdAt?: string | null;
    totalStocks?: number;
    stocksWithQuotes?: number;
    dateRange?: {
      from?: string;
      to?: string;
      min?: string;
      max?: string;
    } | null;
    validation?: {
      isValid?: boolean;
      errors?: string[];
      warnings?: string[];
    };
  };
}

interface LegacyDatasetListItem {
  name: string;
  path?: string;
  fileSize: number;
  lastModified: string;
  preset?: string | null;
  createdAt?: string | null;
  backend?: DatasetListItem['backend'];
  hasCompatibilityArtifact?: boolean;
}

type DatasetStorage = DatasetInfoResponse['storage'];
type LegacySnapshot = LegacyDatasetInfoResponse['snapshot'];
type LegacyValidation = NonNullable<LegacySnapshot['validation']>;
type DatasetJobResultPayload = NonNullable<DatasetJobResponse['result']> & {
  warnings?: string[] | null;
  errors?: string[] | null;
};
type DatasetJobResponsePayload = Omit<DatasetJobResponse, 'result'> & {
  result?: DatasetJobResultPayload | null;
};

function inferLegacyStorage(path: string): DatasetStorage {
  const isLegacySqlite = path.endsWith('.db');
  return {
    backend: isLegacySqlite ? 'sqlite-legacy' : 'duckdb-parquet',
    primaryPath: path,
    duckdbPath: null,
    compatibilityDbPath: null,
    manifestPath: null,
    hasCompatibilityArtifact: false,
  };
}

function isDatasetInfoResponse(value: DatasetInfoResponse | LegacyDatasetInfoResponse): value is DatasetInfoResponse {
  return 'stats' in value && 'validation' in value;
}

function normalizeStorage(path: string, storage: DatasetStorage | null | undefined): DatasetStorage {
  // Keep web resilient during rolling deploys and when viewing historical dataset payloads.
  const fallback = inferLegacyStorage(path);
  return {
    ...fallback,
    ...storage,
    compatibilityDbPath: storage?.compatibilityDbPath ?? fallback.compatibilityDbPath,
    manifestPath: storage?.manifestPath ?? fallback.manifestPath,
    hasCompatibilityArtifact: storage?.hasCompatibilityArtifact ?? fallback.hasCompatibilityArtifact,
  };
}

function normalizeLegacyValidation(snapshot: LegacySnapshot): LegacyValidation {
  return snapshot.validation ?? {
    isValid: true,
    errors: [],
    warnings: [],
  };
}

function normalizeLegacyDatasetInfoResponse(value: LegacyDatasetInfoResponse): DatasetInfoResponse {
  const snapshot = value.snapshot ?? {};
  const validation = normalizeLegacyValidation(snapshot);
  const dateRange = snapshot.dateRange ?? {};
  const totalStocks = snapshot.totalStocks ?? 0;
  const stocksWithQuotes = snapshot.stocksWithQuotes ?? 0;

  return {
    name: value.name,
    path: value.path,
    fileSize: value.fileSize,
    lastModified: value.lastModified,
    storage: inferLegacyStorage(value.path),
    snapshot: {
      preset: snapshot.preset ?? null,
      createdAt: snapshot.createdAt ?? null,
    },
    stats: {
      totalStocks,
      totalQuotes: 0,
      dateRange: {
        from: dateRange.from ?? dateRange.min ?? '-',
        to: dateRange.to ?? dateRange.max ?? '-',
      },
      hasMarginData: false,
      hasTOPIXData: !(validation.warnings ?? []).includes('No TOPIX data'),
      hasSectorData: false,
      hasStatementsData: false,
      statementsFieldCoverage: null,
    },
    validation: {
      isValid: validation.isValid ?? true,
      errors: validation.errors ?? [],
      warnings: validation.warnings ?? [],
      details: {
        dataCoverage: {
          totalStocks,
          stocksWithQuotes,
          stocksWithStatements: 0,
          stocksWithMargin: 0,
        },
      },
    },
  };
}

function normalizeDatasetInfoResponse(value: DatasetInfoResponse | LegacyDatasetInfoResponse): DatasetInfoResponse {
  if (isDatasetInfoResponse(value)) {
    return {
      ...value,
      storage: normalizeStorage(value.path, value.storage),
    };
  }

  return normalizeLegacyDatasetInfoResponse(value);
}

function normalizeDatasetListItem(value: DatasetListItem | LegacyDatasetListItem): DatasetListItem {
  const legacyPath = 'path' in value ? value.path : undefined;
  const inferredStorage = inferLegacyStorage(legacyPath ?? value.name);
  return {
    ...value,
    preset: value.preset ?? null,
    createdAt: value.createdAt ?? null,
    backend: value.backend ?? inferredStorage.backend,
    hasCompatibilityArtifact: value.hasCompatibilityArtifact ?? inferredStorage.hasCompatibilityArtifact,
  };
}

function normalizeDatasetJobResponse(value: DatasetJobResponsePayload): DatasetJobResponse {
  if (value.result == null) {
    const { result: _result, ...rest } = value;
    return rest;
  }

  return {
    ...value,
    result: {
      ...value.result,
      warnings: value.result.warnings ?? [],
      errors: value.result.errors ?? [],
    },
  };
}

function fetchDatasets(): Promise<DatasetListResponse> {
  return apiGet<DatasetListResponse | LegacyDatasetListItem[]>('/api/dataset').then((items) =>
    items.map(normalizeDatasetListItem)
  );
}

function fetchDatasetInfo(name: string): Promise<DatasetInfoResponse> {
  return apiGet<DatasetInfoResponse | LegacyDatasetInfoResponse>(`/api/dataset/${encodeURIComponent(name)}/info`).then(
    normalizeDatasetInfoResponse
  );
}

function fetchJobStatus(jobId: string): Promise<DatasetJobResponse> {
  return apiGet<DatasetJobResponsePayload>(`/api/dataset/jobs/${encodeURIComponent(jobId)}`).then(
    normalizeDatasetJobResponse
  );
}

function createDataset(request: DatasetCreateRequest): Promise<DatasetCreateJobResponse> {
  return apiPost<DatasetCreateJobResponse>('/api/dataset', request);
}

function deleteDataset(name: string): Promise<DatasetDeleteResponse> {
  return apiDelete<DatasetDeleteResponse>(`/api/dataset/${encodeURIComponent(name)}`);
}

function cancelJob(jobId: string): Promise<CancelDatasetJobResponse> {
  return apiDelete<CancelDatasetJobResponse>(`/api/dataset/jobs/${encodeURIComponent(jobId)}`);
}

export function useDatasets() {
  return useQuery({
    queryKey: datasetKeys.list(),
    queryFn: () => {
      logger.debug('Fetching datasets list');
      return fetchDatasets();
    },
    staleTime: 30 * 1000,
  });
}

export function useDatasetInfo(name: string | null) {
  return useQuery({
    queryKey: datasetKeys.info(name ?? ''),
    queryFn: () => {
      if (!name) throw new Error('Dataset name required');
      logger.debug('Fetching dataset info', { name });
      return fetchDatasetInfo(name);
    },
    enabled: !!name,
    staleTime: 5 * 60 * 1000,
  });
}

export function useDatasetJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: datasetKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling dataset job status', { jobId });
      return fetchJobStatus(jobId);
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 2000;
      return false;
    },
    retry: (failureCount, error) => {
      if (error instanceof ApiError && error.status === 404) return false;
      return failureCount < 2;
    },
    staleTime: 0,
  });
}

export function useCreateDataset() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: createDataset,
    onSuccess: (data) => {
      logger.debug('Dataset creation started', { jobId: data.jobId });
      queryClient.invalidateQueries({ queryKey: datasetKeys.list() });
    },
    onError: (error) => {
      logger.error('Failed to create dataset', { error: error.message });
    },
  });
}

export function useDeleteDataset() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: deleteDataset,
    onSuccess: (data) => {
      logger.debug('Dataset deleted', { name: data.name });
      queryClient.invalidateQueries({ queryKey: datasetKeys.list() });
    },
    onError: (error) => {
      logger.error('Failed to delete dataset', { error: error.message });
    },
  });
}

export function useCancelDatasetJob() {
  return useMutation({
    mutationFn: cancelJob,
    onError: (error) => {
      logger.error('Failed to cancel dataset job', { error: error.message });
    },
  });
}
