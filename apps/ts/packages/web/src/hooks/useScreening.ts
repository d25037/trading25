import { type QueryClient, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import type {
  MarketScreeningResponse,
  ScreeningJobRequest,
  ScreeningJobResponse,
} from '@trading25/contracts/types/api-response-types';
import { useCallback } from 'react';
import { analyticsClient } from '@/lib/analytics-client';
import type { ScreeningParams } from '@/types/screening';
import { isTerminalJobStatus, resolveActiveJobRefetchInterval } from '@trading25/api-clients/base/job-status';
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
const SCREENING_SSE_EVENTS = ['snapshot', 'job'] as const;

function runScreeningJob(request: ScreeningJobRequest): Promise<ScreeningJobResponse> {
  return analyticsClient.createScreeningJob(toClientScreeningJobRequest(request)).then(normalizeScreeningJobResponse);
}

function fetchScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
  return analyticsClient.getScreeningJobStatus(jobId).then(normalizeScreeningJobResponse);
}

function fetchScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
  return analyticsClient.getScreeningResult(jobId).then(normalizeScreeningResult);
}

function cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
  return analyticsClient.cancelScreeningJob(jobId).then(normalizeScreeningJobResponse);
}

function toClientScreeningJobRequest(
  request: ScreeningJobRequest
): Parameters<typeof analyticsClient.createScreeningJob>[0] {
  return {
    ...request,
    date: request.date ?? undefined,
    limit: request.limit ?? undefined,
    markets: request.markets ?? undefined,
    strategies: request.strategies ?? undefined,
  };
}

function normalizeScreeningJobResponse(
  response: Awaited<ReturnType<typeof analyticsClient.getScreeningJobStatus>>
): ScreeningJobResponse {
  return {
    ...response,
    entry_decidability: response.entry_decidability ?? 'pre_open_decidable',
  };
}

function normalizeScreeningResult(
  response: Awaited<ReturnType<typeof analyticsClient.getScreeningResult>>
): MarketScreeningResponse {
  return {
    ...response,
    entry_decidability: response.entry_decidability ?? 'pre_open_decidable',
  };
}

function toJobRequest(params: ScreeningParams): ScreeningJobRequest {
  return {
    entry_decidability: params.entry_decidability ?? 'pre_open_decidable',
    markets: params.markets,
    strategies: params.strategies,
    recentDays: params.recentDays ?? 10,
    date: params.date,
    sortBy: params.sortBy ?? 'matchedDate',
    order: params.order ?? 'desc',
    limit: params.limit,
  };
}

function parseScreeningJobPayload(rawData: string, jobId: string | null): ScreeningJobResponse | null {
  try {
    const payload: unknown = JSON.parse(rawData);
    if (!isScreeningJobPayload(payload)) {
      return null;
    }
    return normalizeScreeningJobResponse(payload);
  } catch (error) {
    logger.error('Failed to parse screening SSE payload', { error: String(error), jobId });
    return null;
  }
}

function isScreeningJobPayload(
  value: unknown
): value is Awaited<ReturnType<typeof analyticsClient.getScreeningJobStatus>> {
  if (!isUnknownRecord(value)) return false;
  const record = value;
  const hasRequiredStrings = ['job_id', 'status', 'created_at', 'markets', 'sortBy', 'order'].every(
    (key) => typeof record[key] === 'string'
  );
  const entryDecidability = record.entry_decidability;
  const hasValidEntryDecidability =
    entryDecidability === undefined ||
    entryDecidability === 'pre_open_decidable' ||
    entryDecidability === 'requires_same_session_observation';
  return hasRequiredStrings && typeof record.recentDays === 'number' && hasValidEntryDecidability;
}

function isUnknownRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
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
      if (isTerminalJobStatus(payload.status)) {
        controls.close();
      }
    },
    [jobId, queryClient]
  );

  return useSseStream({
    url: streamUrl,
    eventNames: SCREENING_SSE_EVENTS,
    onAnyMessage: handleJobEvent,
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
      return resolveActiveJobRefetchInterval(query.state.data?.status);
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
