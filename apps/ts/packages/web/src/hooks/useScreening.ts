import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, apiGet, apiPost } from '@/lib/api-client';
import type {
  MarketScreeningResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningParams,
} from '@/types/screening';
import { logger } from '@/utils/logger';

export const screeningKeys = {
  all: ['screening'] as const,
  job: (jobId: string) => [...screeningKeys.all, 'job', jobId] as const,
  result: (jobId: string) => [...screeningKeys.all, 'result', jobId] as const,
};

function runScreeningJob(request: ScreeningJobRequest): Promise<ScreeningJobResponse> {
  return apiPost<ScreeningJobResponse>('/api/analytics/screening/jobs', request);
}

function fetchScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
  return apiGet<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}`);
}

function fetchScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
  return apiGet<MarketScreeningResponse>(`/api/analytics/screening/result/${encodeURIComponent(jobId)}`);
}

function cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
  return apiPost<ScreeningJobResponse>(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}/cancel`);
}

function toJobRequest(params: ScreeningParams): ScreeningJobRequest {
  return {
    markets: params.markets,
    strategies: params.strategies,
    recentDays: params.recentDays,
    date: params.date,
    sortBy: params.sortBy,
    order: params.order,
    limit: params.limit,
  };
}

export function useRunScreeningJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (params: ScreeningParams) => runScreeningJob(toJobRequest(params)),
    onSuccess: (data) => {
      logger.debug('Screening job started', { jobId: data.job_id, status: data.status });
      queryClient.invalidateQueries({ queryKey: screeningKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start screening job', { error: error.message });
    },
  });
}

export function useScreeningJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: screeningKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling screening job status', { jobId });
      return fetchScreeningJobStatus(jobId);
    },
    enabled: !!jobId,
    retry: (failureCount, error) => {
      if (error instanceof ApiError && error.status === 404) return false;
      return failureCount < 2;
    },
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 2000;
      return false;
    },
    staleTime: 0,
  });
}

export function useScreeningResult(jobId: string | null, enabled = true) {
  return useQuery({
    queryKey: screeningKeys.result(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Fetching screening result', { jobId });
      return fetchScreeningResult(jobId);
    },
    enabled: !!jobId && enabled,
    staleTime: Infinity,
  });
}

export function useCancelScreeningJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: cancelScreeningJob,
    onMutate: async (jobId) => {
      await queryClient.cancelQueries({ queryKey: screeningKeys.job(jobId) });
    },
    onSuccess: (data) => {
      logger.debug('Screening job cancelled', { jobId: data.job_id, status: data.status });
      queryClient.invalidateQueries({ queryKey: screeningKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to cancel screening job', { error: error.message });
    },
  });
}
