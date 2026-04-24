import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, apiDelete, apiGet, apiPost } from '@/lib/api-client';
import type {
  CancelDatasetJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateRequest,
  DatasetDeleteResponse,
  DatasetInfoResponse,
  DatasetJobResponse,
  DatasetListItem,
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
  backend?: string;
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

const UNSUPPORTED_LEGACY_DATASET_ERROR =
  'Unsupported legacy dataset snapshot; recreate it as dataset.duckdb + parquet/ + manifest.v2.json.';

function isUnsupportedLegacyDatasetPath(path: string): boolean {
  return path.endsWith('.db');
}

function normalizeDuckDbStorage(path: string, storage: DatasetStorage | null | undefined): DatasetStorage {
  return {
    backend: 'duckdb-parquet',
    primaryPath: storage?.primaryPath ?? path,
    duckdbPath: storage?.duckdbPath ?? null,
    manifestPath: storage?.manifestPath ?? null,
  };
}

function isDatasetInfoResponse(value: DatasetInfoResponse | LegacyDatasetInfoResponse): value is DatasetInfoResponse {
  return 'stats' in value && 'validation' in value;
}

function markUnsupportedLegacyValidation(validation: DatasetInfoResponse['validation']): DatasetInfoResponse['validation'] {
  return {
    ...validation,
    isValid: false,
    errors: [UNSUPPORTED_LEGACY_DATASET_ERROR, ...(validation.errors ?? [])],
    warnings: validation.warnings ?? [],
  };
}

function normalizeLegacyValidation(snapshot: LegacySnapshot): LegacyValidation {
  return (
    snapshot.validation ?? {
      isValid: true,
      errors: [],
      warnings: [],
    }
  );
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
    storage: normalizeDuckDbStorage(value.path, null),
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
      isValid: false,
      errors: [UNSUPPORTED_LEGACY_DATASET_ERROR, ...(validation.errors ?? [])],
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
    const backend = typeof value.storage?.backend === 'string' ? value.storage.backend : 'duckdb-parquet';
    const isUnsupported = backend !== 'duckdb-parquet' || isUnsupportedLegacyDatasetPath(value.path);
    return {
      ...value,
      storage: normalizeDuckDbStorage(value.path, value.storage),
      validation: isUnsupported ? markUnsupportedLegacyValidation(value.validation) : value.validation,
    };
  }

  return normalizeLegacyDatasetInfoResponse(value);
}

function normalizeDatasetListItem(value: DatasetListItem | LegacyDatasetListItem): DatasetListItem | null {
  const legacyPath = 'path' in value ? value.path : undefined;
  const path = legacyPath ?? value.name;
  if (value.backend && value.backend !== 'duckdb-parquet') {
    logger.debug('Skipping unsupported dataset backend', { name: value.name, backend: value.backend });
    return null;
  }
  if (isUnsupportedLegacyDatasetPath(path)) {
    logger.debug('Skipping unsupported legacy dataset snapshot', { name: value.name, path });
    return null;
  }
  return {
    ...value,
    path,
    preset: value.preset ?? null,
    createdAt: value.createdAt ?? null,
    backend: 'duckdb-parquet',
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
    items.map(normalizeDatasetListItem).filter((item): item is DatasetListItem => item !== null)
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
