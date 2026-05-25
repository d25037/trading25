import { type QueryClient, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import { useCallback } from 'react';
import { analyticsClient } from '@/lib/analytics-client';
import type {
  MarketScreeningResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
  ScreeningParams,
} from '@/types/screening';
import { logger } from '@/utils/logger';
import { type SseStreamControls, useSseStream } from './useSseStream';

export const screeningKeys = {
  all: ['screening'] as const,
  job: (jobId: string) => [...screeningKeys.all, 'job', jobId] as const,
  result: (jobId: string) => [...screeningKeys.all, 'result', jobId] as const,
};

interface ScreeningJobSSEState {
  isConnected: boolean;
}

const MAX_SSE_RETRIES = 3;
const TERMINAL_SCREENING_STATUSES: ScreeningJobResponse['status'][] = ['completed', 'failed', 'cancelled'];
const SCREENING_SSE_EVENTS = ['snapshot', 'job'] as const;

function isTerminalScreeningStatus(status: unknown): status is ScreeningJobResponse['status'] {
  return TERMINAL_SCREENING_STATUSES.includes(status as ScreeningJobResponse['status']);
}

function runScreeningJob(request: ScreeningJobRequest): Promise<ScreeningJobResponse> {
  return analyticsClient.createScreeningJob(request);
}

function fetchScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
  return analyticsClient.getScreeningJobStatus(jobId);
}

function fetchScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
  return analyticsClient.getScreeningResult(jobId);
}

function cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
  return analyticsClient.cancelScreeningJob(jobId);
}

function toJobRequest(params: ScreeningParams): ScreeningJobRequest {
  return {
    entry_decidability: params.entry_decidability,
    markets: params.markets,
    strategies: params.strategies,
    recentDays: params.recentDays,
    date: params.date,
    sortBy: params.sortBy,
    order: params.order,
    limit: params.limit,
  };
}

function parseScreeningJobPayload(rawData: string, jobId: string | null): ScreeningJobResponse | null {
  try {
    const payload = JSON.parse(rawData) as ScreeningJobResponse;
    if (typeof payload.job_id !== 'string' || typeof payload.status !== 'string') {
      return null;
    }
    return payload;
  } catch (error) {
    logger.error('Failed to parse screening SSE payload', { error: String(error), jobId });
    return null;
  }
}

function updateScreeningJobCache(queryClient: QueryClient, payload: ScreeningJobResponse): void {
  queryClient.setQueryData(screeningKeys.job(payload.job_id), payload);
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

export function useScreeningJobSSE(jobId: string | null): ScreeningJobSSEState {
  const queryClient = useQueryClient();
  const streamUrl = jobId ? `/api/analytics/screening/jobs/${encodeURIComponent(jobId)}/stream` : null;

  const handleJobEvent = useCallback(
    (rawData: string, controls: SseStreamControls) => {
      const payload = parseScreeningJobPayload(rawData, jobId);
      if (!payload) {
        return;
      }

      updateScreeningJobCache(queryClient, payload);
      if (isTerminalScreeningStatus(payload.status)) {
        controls.close();
      }
    },
    [jobId, queryClient]
  );

  return useSseStream({
    url: streamUrl,
    eventNames: SCREENING_SSE_EVENTS,
    onMessage: handleJobEvent,
    onEvent: (_eventName, rawData, controls) => handleJobEvent(rawData, controls),
    maxRetries: MAX_SSE_RETRIES,
    onMaxRetriesExceeded: () => logger.error('Screening SSE max retries exceeded', { jobId }),
  });
}

export function useScreeningJobStatus(jobId: string | null, sseConnected = false) {
  return useQuery({
    queryKey: screeningKeys.job(jobId ?? ''),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling screening job status', { jobId });
      return fetchScreeningJobStatus(jobId);
    },
    enabled: !!jobId && !sseConnected,
    retry: (failureCount, error) => {
      if (error instanceof HttpRequestError && error.kind === 'http' && error.status === 404) return false;
      return failureCount < 2;
    },
    refetchInterval: (query) => {
      if (sseConnected) return false;
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
