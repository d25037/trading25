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
} from '@trading25/contracts/types/api-response-types';
import { resolveActiveJobRefetchInterval } from '@trading25/api-clients/base/job-status';
import { logger } from '@/utils/logger';

export const datasetKeys = {
  all: ['dataset'] as const,
  list: () => [...datasetKeys.all, 'list'] as const,
  info: (name: string) => [...datasetKeys.all, 'info', name] as const,
  job: (jobId: string) => [...datasetKeys.all, 'job', jobId] as const,
};

type DatasetJobResultPayload = NonNullable<DatasetJobResponse['result']> & {
  warnings?: string[] | null;
  errors?: string[] | null;
};
type DatasetJobResponsePayload = Omit<DatasetJobResponse, 'result'> & {
  result?: DatasetJobResultPayload | null;
};
type DatasetQueryOptions = {
  enabled?: boolean;
};

function normalizeDatasetListItem(value: DatasetListItem): DatasetListItem | null {
  if (value.backend !== 'duckdb-parquet') {
    logger.debug('Skipping unsupported dataset backend', { name: value.name, backend: value.backend });
    return null;
  }
  return {
    ...value,
    preset: value.preset ?? null,
    createdAt: value.createdAt ?? null,
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
  return apiGet<DatasetListResponse>('/api/dataset').then((items) =>
    items.map(normalizeDatasetListItem).filter((item): item is DatasetListItem => item !== null)
  );
}

function fetchDatasetInfo(name: string): Promise<DatasetInfoResponse> {
  return apiGet<DatasetInfoResponse>(`/api/dataset/${encodeURIComponent(name)}/info`);
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

export function useDatasets(options: DatasetQueryOptions = {}) {
  return useQuery({
    queryKey: datasetKeys.list(),
    queryFn: () => {
      logger.debug('Fetching datasets list');
      return fetchDatasets();
    },
    enabled: options.enabled ?? true,
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
      return resolveActiveJobRefetchInterval(query.state.data?.status);
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
