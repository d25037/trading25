import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import type {
  CancelDatasetJobResponse,
  DatasetCreateJobResponse,
  DatasetCreateRequest,
  DatasetDeleteResponse,
  DatasetInfoResponse,
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

function fetchDatasets(): Promise<DatasetListResponse> {
  return apiGet<DatasetListResponse>('/api/dataset');
}

function fetchDatasetInfo(name: string): Promise<DatasetInfoResponse> {
  return apiGet<DatasetInfoResponse>(`/api/dataset/${encodeURIComponent(name)}/info`);
}

function fetchJobStatus(jobId: string): Promise<DatasetJobResponse> {
  return apiGet<DatasetJobResponse>(`/api/dataset/jobs/${encodeURIComponent(jobId)}`);
}

function createDataset(request: DatasetCreateRequest): Promise<DatasetCreateJobResponse> {
  return apiPost<DatasetCreateJobResponse>('/api/dataset', request);
}

function resumeDataset(request: DatasetCreateRequest): Promise<DatasetCreateJobResponse> {
  return apiPost<DatasetCreateJobResponse>('/api/dataset/resume', request);
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

export function useResumeDataset() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: resumeDataset,
    onSuccess: (data) => {
      logger.debug('Dataset resume started', { jobId: data.jobId });
      queryClient.invalidateQueries({ queryKey: datasetKeys.list() });
    },
    onError: (error) => {
      logger.error('Failed to resume dataset', { error: error.message });
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
