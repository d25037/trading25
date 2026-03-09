import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState } from 'react';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import type {
  CancelJobResponse,
  CreateSyncJobResponse,
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  RefreshStocksRequest,
  StartSyncRequest,
  SyncFetchDetail,
  SyncFetchDetailsResponse,
  SyncJobResponse,
} from '@/types/sync';
import { logger } from '@/utils/logger';

export const syncKeys = {
  active: () => ['sync-job-active'] as const,
  job: (jobId: string | null) => ['sync-job', jobId] as const,
  fetchDetails: (jobId: string | null) => ['sync-job-fetch-details', jobId] as const,
  stats: (isSyncRunning: boolean) => ['db-stats', isSyncRunning ? 'running' : 'idle'] as const,
  validation: (isSyncRunning: boolean) => ['db-validation', isSyncRunning ? 'running' : 'idle'] as const,
};

interface SnapshotPollingOptions {
  isSyncRunning?: boolean;
}

interface SnapshotQueryTiming {
  refetchInterval: number;
  staleTime: number;
}

interface SyncSSEState {
  isConnected: boolean;
}

interface SyncSnapshotPayload {
  job?: SyncJobResponse;
  fetchDetails?: SyncFetchDetailsResponse | null;
}

interface SyncFetchDetailStreamPayload {
  jobId: string;
  status: SyncJobResponse['status'];
  mode: SyncJobResponse['mode'];
  detail: SyncFetchDetail | null;
}

const MAX_SSE_RETRIES = 3;
const SNAPSHOT_POLL_INTERVAL_RUNNING_MS = 2_000;
const SNAPSHOT_POLL_INTERVAL_IDLE_MS = 30_000;
const SNAPSHOT_STALE_TIME_RUNNING_MS = 0;
const SNAPSHOT_STALE_TIME_IDLE_MS = 5_000;
const TERMINAL_SYNC_STATUSES: SyncJobResponse['status'][] = ['completed', 'failed', 'cancelled'];

// Fetch functions
function startSync(request: StartSyncRequest): Promise<CreateSyncJobResponse> {
  return apiPost<CreateSyncJobResponse>('/api/db/sync', request);
}

function fetchJobStatus(jobId: string): Promise<SyncJobResponse> {
  return apiGet<SyncJobResponse>(`/api/db/sync/jobs/${jobId}`);
}

function fetchActiveJobStatus(): Promise<SyncJobResponse | null> {
  return apiGet<SyncJobResponse | null>('/api/db/sync/jobs/active');
}

function fetchSyncFetchDetails(jobId: string): Promise<SyncFetchDetailsResponse> {
  return apiGet<SyncFetchDetailsResponse>(`/api/db/sync/jobs/${jobId}/fetch-details`);
}

function cancelJob(jobId: string): Promise<CancelJobResponse> {
  return apiDelete<CancelJobResponse>(`/api/db/sync/jobs/${jobId}`);
}

function fetchDbStats(): Promise<MarketStatsResponse> {
  return apiGet<MarketStatsResponse>('/api/db/stats');
}

function fetchDbValidation(): Promise<MarketValidationResponse> {
  return apiGet<MarketValidationResponse>('/api/db/validate');
}

function refreshStocks(request: RefreshStocksRequest): Promise<MarketRefreshResponse> {
  return apiPost<MarketRefreshResponse>('/api/db/stocks/refresh', request);
}

function resolveSnapshotQueryTiming(isSyncRunning: boolean): SnapshotQueryTiming {
  if (isSyncRunning) {
    return {
      refetchInterval: SNAPSHOT_POLL_INTERVAL_RUNNING_MS,
      staleTime: SNAPSHOT_STALE_TIME_RUNNING_MS,
    };
  }

  return {
    refetchInterval: SNAPSHOT_POLL_INTERVAL_IDLE_MS,
    staleTime: SNAPSHOT_STALE_TIME_IDLE_MS,
  };
}

function isTerminalSyncStatus(status: string | null | undefined): status is SyncJobResponse['status'] {
  return TERMINAL_SYNC_STATUSES.includes(status as SyncJobResponse['status']);
}

function getFetchDetailKey(detail: SyncFetchDetail): string {
  return `${detail.timestamp}-${detail.stage}-${detail.endpoint}-${detail.eventType}`;
}

function mergeFetchDetails(
  previous: SyncFetchDetailsResponse | undefined,
  payload: SyncFetchDetailStreamPayload
): SyncFetchDetailsResponse {
  const detail = payload.detail;
  if (!detail) {
    return (
      previous ?? {
        jobId: payload.jobId,
        status: payload.status,
        mode: payload.mode,
        items: [],
      }
    );
  }

  const existingItems = previous?.items ?? [];
  const nextItems = existingItems.some((item) => getFetchDetailKey(item) === getFetchDetailKey(detail))
    ? existingItems
    : [...existingItems, detail];

  return {
    jobId: payload.jobId,
    status: payload.status,
    mode: payload.mode,
    latest: detail,
    items: nextItems,
  };
}

function syncFetchDetailsStatus(
  previous: SyncFetchDetailsResponse | undefined,
  payload: SyncJobResponse
): SyncFetchDetailsResponse | undefined {
  if (!previous) {
    return previous;
  }
  return {
    ...previous,
    status: payload.status,
    mode: payload.mode,
  };
}

// Hooks
export function useStartSync() {
  return useMutation({
    mutationFn: startSync,
    onSuccess: (data) => {
      logger.debug('Sync job started', { jobId: data.jobId, mode: data.mode });
    },
    onError: (error) => {
      logger.error('Failed to start sync', { error: error.message });
    },
  });
}

export function useSyncSSE(jobId: string | null): SyncSSEState {
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

      const es = new EventSource(`/api/db/sync/jobs/${encodeURIComponent(jobId)}/stream`);
      eventSourceRef.current = es;

      const handleSnapshotEvent = (rawData: string) => {
        try {
          const payload = JSON.parse(rawData) as SyncSnapshotPayload;
          if (payload.job?.jobId) {
            queryClient.setQueryData(syncKeys.job(payload.job.jobId), payload.job);
          }
          if (payload.fetchDetails?.jobId) {
            queryClient.setQueryData(syncKeys.fetchDetails(payload.fetchDetails.jobId), payload.fetchDetails);
          }

          if (isTerminalSyncStatus(payload.job?.status)) {
            cleanup();
            if (!disposed) {
              setIsConnected(false);
            }
          }
        } catch (error) {
          logger.error('Failed to parse sync SSE snapshot', { error: String(error), jobId });
        }
      };

      const handleJobEvent = (rawData: string) => {
        try {
          const payload = JSON.parse(rawData) as SyncJobResponse;
          if (typeof payload.jobId !== 'string' || typeof payload.status !== 'string') {
            return;
          }

          queryClient.setQueryData(syncKeys.job(payload.jobId), payload);
          queryClient.setQueryData(
            syncKeys.fetchDetails(payload.jobId),
            (previous: SyncFetchDetailsResponse | undefined) => syncFetchDetailsStatus(previous, payload)
          );

          if (isTerminalSyncStatus(payload.status)) {
            cleanup();
            if (!disposed) {
              setIsConnected(false);
            }
          }
        } catch (error) {
          logger.error('Failed to parse sync SSE job event', { error: String(error), jobId });
        }
      };

      const handleFetchDetailEvent = (rawData: string) => {
        try {
          const payload = JSON.parse(rawData) as SyncFetchDetailStreamPayload;
          if (typeof payload.jobId !== 'string') {
            return;
          }

          queryClient.setQueryData(
            syncKeys.fetchDetails(payload.jobId),
            (previous: SyncFetchDetailsResponse | undefined) => mergeFetchDetails(previous, payload)
          );
        } catch (error) {
          logger.error('Failed to parse sync SSE fetch-detail event', { error: String(error), jobId });
        }
      };

      es.onopen = () => {
        retryCountRef.current = 0;
        if (!disposed) {
          setIsConnected(true);
        }
      };

      es.addEventListener('snapshot', (event) => {
        handleSnapshotEvent((event as MessageEvent<string>).data);
      });
      es.addEventListener('job', (event) => {
        handleJobEvent((event as MessageEvent<string>).data);
      });
      es.addEventListener('fetch-detail', (event) => {
        handleFetchDetailEvent((event as MessageEvent<string>).data);
      });

      es.onerror = () => {
        cleanup();
        if (!disposed) {
          setIsConnected(false);
        }
        retryCountRef.current += 1;

        if (retryCountRef.current > MAX_SSE_RETRIES) {
          logger.error('Sync SSE max retries exceeded', { jobId });
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

export function useSyncJobStatus(jobId: string | null, sseConnected = false) {
  return useQuery({
    queryKey: syncKeys.job(jobId),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling sync job status', { jobId });
      return fetchJobStatus(jobId);
    },
    enabled: !!jobId && !sseConnected,
    refetchInterval: (query) => {
      if (sseConnected) {
        return false;
      }
      const status = query.state.data?.status;
      if (isTerminalSyncStatus(status)) {
        return false;
      }
      return 1000;
    },
    staleTime: 0,
  });
}

export function useSyncFetchDetails(jobId: string | null, sseConnected = false) {
  return useQuery({
    queryKey: syncKeys.fetchDetails(jobId),
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling sync fetch details', { jobId });
      return fetchSyncFetchDetails(jobId);
    },
    enabled: !!jobId && !sseConnected,
    refetchInterval: (query) => {
      if (sseConnected) {
        return false;
      }
      const status = query.state.data?.status;
      if (isTerminalSyncStatus(status)) {
        return false;
      }
      return 1000;
    },
    staleTime: 0,
  });
}

export function useActiveSyncJob(enabled = true) {
  return useQuery({
    queryKey: syncKeys.active(),
    queryFn: fetchActiveJobStatus,
    enabled,
    staleTime: 0,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status === 'pending' || status === 'running') {
        return 1000;
      }
      return 5000;
    },
  });
}

export function useCancelSync() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: cancelJob,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: syncKeys.job(data.jobId) });
      logger.debug('Sync job cancelled', { jobId: data.jobId });
    },
    onError: (error) => {
      logger.error('Failed to cancel sync', { error: error.message });
    },
  });
}

export function useDbStats(options?: SnapshotPollingOptions) {
  const isSyncRunning = options?.isSyncRunning ?? false;
  const timing = resolveSnapshotQueryTiming(isSyncRunning);
  return useQuery({
    queryKey: syncKeys.stats(isSyncRunning),
    queryFn: fetchDbStats,
    refetchInterval: timing.refetchInterval,
    staleTime: timing.staleTime,
    refetchIntervalInBackground: true,
  });
}

export function useDbValidation(options?: SnapshotPollingOptions) {
  const isSyncRunning = options?.isSyncRunning ?? false;
  const timing = resolveSnapshotQueryTiming(isSyncRunning);
  return useQuery({
    queryKey: syncKeys.validation(isSyncRunning),
    queryFn: fetchDbValidation,
    refetchInterval: timing.refetchInterval,
    staleTime: timing.staleTime,
    refetchIntervalInBackground: true,
  });
}

export function useRefreshStocks() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: refreshStocks,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['db-stats'] });
      queryClient.invalidateQueries({ queryKey: ['db-validation'] });
      logger.debug('Stock refresh completed', {
        totalStocks: data.totalStocks,
        successCount: data.successCount,
        failedCount: data.failedCount,
      });
    },
    onError: (error) => {
      logger.error('Failed to refresh stocks', { error: error.message });
    },
  });
}
