import { type QueryClient, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import { useCallback, useEffect, useRef, useState } from 'react';
import { analyticsClient } from '@/lib/analytics-client';
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

interface ScreeningJobSSEState {
  isConnected: boolean;
}

const MAX_SSE_RETRIES = 3;
const TERMINAL_SCREENING_STATUSES: ScreeningJobResponse['status'][] = ['completed', 'failed', 'cancelled'];

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

function closeScreeningJobStream(
  cleanup: () => void,
  setIsConnected: (isConnected: boolean) => void,
  disposed: boolean
): void {
  cleanup();
  if (!disposed) {
    setIsConnected(false);
  }
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
  const [isConnected, setIsConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryCountRef = useRef(0);

  const cleanup = useCallback(() => {
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!jobId) {
      cleanup();
      retryCountRef.current = 0;
      setIsConnected(false);
      return;
    }

    let disposed = false;
    retryCountRef.current = 0;
    setIsConnected(false);

    const connect = () => {
      cleanup();

      const es = new EventSource(`/api/analytics/screening/jobs/${encodeURIComponent(jobId)}/stream`);
      eventSourceRef.current = es;

      const handleJobEvent = (rawData: string) => {
        const payload = parseScreeningJobPayload(rawData, jobId);
        if (!payload) {
          return;
        }

        updateScreeningJobCache(queryClient, payload);
        if (isTerminalScreeningStatus(payload.status)) {
          closeScreeningJobStream(cleanup, setIsConnected, disposed);
        }
      };

      es.onopen = () => {
        retryCountRef.current = 0;
        if (!disposed) {
          setIsConnected(true);
        }
      };

      es.onmessage = (event) => {
        handleJobEvent(event.data);
      };
      es.addEventListener('snapshot', (event) => {
        handleJobEvent((event as MessageEvent<string>).data);
      });
      es.addEventListener('job', (event) => {
        handleJobEvent((event as MessageEvent<string>).data);
      });

      es.onerror = () => {
        cleanup();
        if (!disposed) {
          setIsConnected(false);
        }
        retryCountRef.current += 1;

        if (retryCountRef.current > MAX_SSE_RETRIES) {
          logger.error('Screening SSE max retries exceeded', { jobId });
          return;
        }

        reconnectTimerRef.current = setTimeout(connect, retryCountRef.current * 1000);
      };
    };

    connect();

    return () => {
      disposed = true;
      cleanup();
    };
  }, [cleanup, jobId, queryClient]);

  return { isConnected };
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
