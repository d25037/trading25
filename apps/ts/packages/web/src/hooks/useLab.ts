import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  LabEvolveRequest,
  LabEvolveResponse,
  LabGenerateRequest,
  LabGenerateResponse,
  LabImproveRequest,
  LabImproveResponse,
  LabJobCancelResponse,
  LabJobCancelPathParams,
  LabJobStatusResponse,
  LabJobStatusPathParams,
  LabJobsQuery,
  LabJobsResponse,
  LabOptimizeRecommendationQuery,
  LabOptimizeRecommendationResponse,
  LabOptimizeRequest,
  LabOptimizeResponse,
} from '@trading25/api-clients/backtest';
import { isActiveJobStatus, resolveActiveJobRefetchInterval } from '@trading25/api-clients/base/job-status';
import { apiGet, apiPost } from '@/lib/api-client';
import { logger } from '@/utils/logger';

export const labKeys = {
  all: ['lab'] as const,
  jobsAll: () => [...labKeys.all, 'jobs'] as const,
  jobs: (limit?: LabJobsQuery['limit']) => [...labKeys.jobsAll(), limit] as const,
  job: (jobId: LabJobStatusPathParams['job_id']) => [...labKeys.all, 'job', jobId] as const,
  optimizeRecommendation: (
    strategyName: LabOptimizeRecommendationQuery['strategy_name'],
    targetScope: NonNullable<LabOptimizeRecommendationQuery['target_scope']>,
    allowedCategories: NonNullable<LabOptimizeRecommendationQuery['allowed_categories']>
  ) =>
    [...labKeys.all, 'optimize-recommendation', strategyName, targetScope, allowedCategories] as const,
};

function fetchLabJobs(limit: NonNullable<LabJobsQuery['limit']> = 50): Promise<LabJobsResponse> {
  return apiGet<LabJobsResponse>(`/api/lab/jobs?limit=${limit}`);
}

function fetchLabJobStatus(jobId: LabJobStatusPathParams['job_id']): Promise<LabJobStatusResponse> {
  return apiGet<LabJobStatusResponse>(`/api/lab/jobs/${encodeURIComponent(jobId)}`);
}

function postLabGenerate(request: LabGenerateRequest): Promise<LabGenerateResponse> {
  return apiPost<LabGenerateResponse>('/api/lab/generate', request);
}

function postLabEvolve(request: LabEvolveRequest): Promise<LabEvolveResponse> {
  return apiPost<LabEvolveResponse>('/api/lab/evolve', request);
}

function postLabOptimize(request: LabOptimizeRequest): Promise<LabOptimizeResponse> {
  return apiPost<LabOptimizeResponse>('/api/lab/optimize', request);
}

function postLabImprove(request: LabImproveRequest): Promise<LabImproveResponse> {
  return apiPost<LabImproveResponse>('/api/lab/improve', request);
}

function fetchLabOptimizeRecommendation(
  strategyName: LabOptimizeRecommendationQuery['strategy_name'],
  targetScope: NonNullable<LabOptimizeRecommendationQuery['target_scope']> = 'both',
  allowedCategories: NonNullable<LabOptimizeRecommendationQuery['allowed_categories']> = []
): Promise<LabOptimizeRecommendationResponse> {
  const query = new URLSearchParams();
  query.set('strategy_name', strategyName);
  query.set('target_scope', targetScope);
  for (const category of allowedCategories) {
    query.append('allowed_categories', category);
  }
  return apiGet<LabOptimizeRecommendationResponse>(`/api/lab/optimize/recommendation?${query.toString()}`);
}

function cancelLabJob(jobId: LabJobCancelPathParams['job_id']): Promise<LabJobCancelResponse> {
  return apiPost<LabJobCancelResponse>(`/api/lab/jobs/${encodeURIComponent(jobId)}/cancel`);
}

export function useLabGenerate() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabGenerate,
    onSuccess: (data) => {
      logger.debug('Lab generate started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.jobsAll() });
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
      queryClient.invalidateQueries({ queryKey: labKeys.jobsAll() });
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
      queryClient.invalidateQueries({ queryKey: labKeys.jobsAll() });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab optimize', { error: error.message });
    },
  });
}

export function useLabOptimizeRecommendation(
  strategyName: LabOptimizeRecommendationQuery['strategy_name'] | null,
  targetScope: NonNullable<LabOptimizeRecommendationQuery['target_scope']> = 'both',
  allowedCategories: NonNullable<LabOptimizeRecommendationQuery['allowed_categories']> = []
) {
  return useQuery({
    queryKey: strategyName
      ? labKeys.optimizeRecommendation(strategyName, targetScope, allowedCategories)
      : [...labKeys.all, 'optimize-recommendation', 'none'],
    queryFn: () => {
      if (!strategyName) throw new Error('Strategy name required');
      return fetchLabOptimizeRecommendation(strategyName, targetScope, allowedCategories);
    },
    enabled: !!strategyName,
    staleTime: 30 * 1000,
  });
}

export function useLabImprove() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: postLabImprove,
    onSuccess: (data) => {
      logger.debug('Lab improve started', { jobId: data.job_id });
      queryClient.invalidateQueries({ queryKey: labKeys.jobsAll() });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to start lab improve', { error: error.message });
    },
  });
}

export function useLabJobStatus(jobId: LabJobStatusPathParams['job_id'] | null, sseConnected = false) {
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
      return resolveActiveJobRefetchInterval(query.state.data?.status);
    },
    staleTime: 0,
  });
}

export function useLabJobs(limit: NonNullable<LabJobsQuery['limit']> = 50) {
  return useQuery({
    queryKey: labKeys.jobs(limit),
    queryFn: () => {
      logger.debug('Fetching lab jobs', { limit });
      return fetchLabJobs(limit);
    },
    refetchInterval: (query) => {
      const jobs = query.state.data;
      if (!jobs) return false;
      if (jobs.some((job) => isActiveJobStatus(job.status))) return 2000;
      return false;
    },
    staleTime: 10 * 1000,
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
      queryClient.invalidateQueries({ queryKey: labKeys.jobsAll() });
      queryClient.invalidateQueries({ queryKey: labKeys.job(data.job_id) });
    },
    onError: (error) => {
      logger.error('Failed to cancel lab job', { error: error.message });
    },
  });
}
