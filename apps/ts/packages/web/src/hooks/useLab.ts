import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiGet, apiPost } from '@/lib/api-client';
import type {
  LabEvolveRequest,
  LabGenerateRequest,
  LabImproveRequest,
  LabJobResponse,
  LabOptimizeRequest,
} from '@/types/backtest';
import { logger } from '@/utils/logger';

export const labKeys = {
  all: ['lab'] as const,
  job: (jobId: string) => [...labKeys.all, 'job', jobId] as const,
};

function fetchLabJobStatus(jobId: string): Promise<LabJobResponse> {
  return apiGet<LabJobResponse>(`/bt/api/lab/jobs/${encodeURIComponent(jobId)}`);
}

function postLabGenerate(request: LabGenerateRequest): Promise<LabJobResponse> {
  return apiPost<LabJobResponse>('/bt/api/lab/generate', request);
}

function postLabEvolve(request: LabEvolveRequest): Promise<LabJobResponse> {
  return apiPost<LabJobResponse>('/bt/api/lab/evolve', request);
}

function postLabOptimize(request: LabOptimizeRequest): Promise<LabJobResponse> {
  return apiPost<LabJobResponse>('/bt/api/lab/optimize', request);
}

function postLabImprove(request: LabImproveRequest): Promise<LabJobResponse> {
  return apiPost<LabJobResponse>('/bt/api/lab/improve', request);
}

function cancelLabJob(jobId: string): Promise<LabJobResponse> {
  return apiPost<LabJobResponse>(`/bt/api/lab/jobs/${encodeURIComponent(jobId)}/cancel`);
}

export function useLabGenerate() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabGenerate,
    onSuccess: (data) => {
      logger.debug('Lab generate started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab generate', { error: error.message });
    },
  });
}

export function useLabEvolve() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabEvolve,
    onSuccess: (data) => {
      logger.debug('Lab evolve started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab evolve', { error: error.message });
    },
  });
}

export function useLabOptimize() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabOptimize,
    onSuccess: (data) => {
      logger.debug('Lab optimize started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab optimize', { error: error.message });
    },
  });
}

export function useLabImprove() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabImprove,
    onSuccess: (data) => {
      logger.debug('Lab improve started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab improve', { error: error.message });
    },
  });
}

export function useLabJobStatus(jobId: string | null, sseConnected = false) {
  return useQuery({
    queryKey: labKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling lab job status', { jobId });
      return fetchLabJobStatus(jobId);
    },
    enabled: !!jobId && !sseConnected,
    refetchInterval: (query) => {
      if (sseConnected) return false;
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 2000;
      return false;
    },
    staleTime: 0,
  });
}

export function useCancelLabJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: cancelLabJob,
    onMutate: async (jobId) => {
      await queryClient.cancelQueries({ queryKey: labKeys.job(jobId) });
    },
    onSuccess: (data) => {
      logger.debug('Lab job cancelled', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to cancel lab job', { error: error.message });
    },
  });
}
