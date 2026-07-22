import { type QueryClient, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type {
  CancelJobResponse,
  CreateSyncJobResponse,
  MarketRefreshResponse,
  MarketStatsResponse,
  MarketValidationResponse,
  StartSyncRequest,
  SyncFetchDetail,
  SyncFetchDetailsResponse,
  SyncJobResponse,
} from '@trading25/contracts/types/api-response-types';
import { useCallback } from 'react';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import { isActiveJobStatus, isTerminalJobStatus } from '@trading25/api-clients/base/job-status';
import { logger } from '@/utils/logger';
import { type SseStreamControls, useSseStream } from './useSseStream';

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

interface RefreshStocksRequest {
  codes: string[];
}

type StartSyncInput = Omit<StartSyncRequest, 'enforceBulkForStockData' | 'resetBeforeSync'> &
  Partial<Pick<StartSyncRequest, 'enforceBulkForStockData' | 'resetBeforeSync'>>;

const MAX_SSE_RETRIES = 3;
const SNAPSHOT_POLL_INTERVAL_RUNNING_MS = 2_000;
const SNAPSHOT_POLL_INTERVAL_IDLE_MS = 30_000;
const SNAPSHOT_STALE_TIME_RUNNING_MS = 0;
const SNAPSHOT_STALE_TIME_IDLE_MS = 5_000;
const SYNC_SSE_EVENTS = ['snapshot', 'job', 'fetch-detail'] as const;

// Fetch functions
function startSync(request: StartSyncInput): Promise<CreateSyncJobResponse> {
  const wireRequest: StartSyncRequest = {
    ...request,
    enforceBulkForStockData: request.enforceBulkForStockData ?? false,
    resetBeforeSync: request.mode === 'initial',
  };
  return apiPost<CreateSyncJobResponse>('/api/db/sync', wireRequest);
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
  return isTerminalJobStatus(status);
}

function resolveSyncJobPollInterval(status: string | null | undefined, sseConnected = false): false | 1000 {
  if (sseConnected || isTerminalSyncStatus(status)) {
    return false;
  }
  return 1000;
}

function resolveActiveSyncJobPollInterval(status: string | null | undefined): 1000 | 5000 {
  return isActiveJobStatus(status) ? 1000 : 5000;
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

function parseSyncEventPayload<T>(rawData: string, eventName: string, jobId: string | null): T | null {
  try {
    return JSON.parse(rawData) as T;
  } catch (error) {
    logger.error(`Failed to parse sync SSE ${eventName}`, { error: String(error), jobId });
    return null;
  }
}

function updateSnapshotCache(queryClient: QueryClient, payload: SyncSnapshotPayload): boolean {
  if (payload.job?.jobId) {
    queryClient.setQueryData(syncKeys.job(payload.job.jobId), payload.job);
  }
  if (payload.fetchDetails?.jobId) {
    queryClient.setQueryData(syncKeys.fetchDetails(payload.fetchDetails.jobId), payload.fetchDetails);
  }

  return isTerminalSyncStatus(payload.job?.status);
}

function updateJobCache(queryClient: QueryClient, payload: SyncJobResponse): boolean {
  if (typeof payload.jobId !== 'string' || typeof payload.status !== 'string') {
    return false;
  }

  queryClient.setQueryData(syncKeys.job(payload.jobId), payload);
  queryClient.setQueryData(syncKeys.fetchDetails(payload.jobId), (previous: SyncFetchDetailsResponse | undefined) =>
    syncFetchDetailsStatus(previous, payload)
  );

  return isTerminalSyncStatus(payload.status);
}

function updateFetchDetailCache(queryClient: QueryClient, payload: SyncFetchDetailStreamPayload): void {
  if (typeof payload.jobId !== 'string') {
    return;
  }

  queryClient.setQueryData(syncKeys.fetchDetails(payload.jobId), (previous: SyncFetchDetailsResponse | undefined) =>
    mergeFetchDetails(previous, payload)
  );
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
  const streamUrl = jobId ? `/api/db/sync/jobs/${encodeURIComponent(jobId)}/stream` : null;

  const handleSnapshotEvent = useCallback(
    (rawData: string, controls: SseStreamControls) => {
      const payload = parseSyncEventPayload<SyncSnapshotPayload>(rawData, 'snapshot', jobId);
      if (!payload) {
        return;
      }
      if (updateSnapshotCache(queryClient, payload)) {
        controls.close();
      }
    },
    [jobId, queryClient]
  );

  const handleJobEvent = useCallback(
    (rawData: string, controls: SseStreamControls) => {
      const payload = parseSyncEventPayload<SyncJobResponse>(rawData, 'job event', jobId);
      if (!payload) {
        return;
      }
      if (updateJobCache(queryClient, payload)) {
        controls.close();
      }
    },
    [jobId, queryClient]
  );

  const handleFetchDetailEvent = useCallback(
    (rawData: string) => {
      const payload = parseSyncEventPayload<SyncFetchDetailStreamPayload>(rawData, 'fetch-detail event', jobId);
      if (!payload) {
        return;
      }
      updateFetchDetailCache(queryClient, payload);
    },
    [jobId, queryClient]
  );

  return useSseStream({
    url: streamUrl,
    eventNames: SYNC_SSE_EVENTS,
    onEvent: (eventName, rawData, controls) => {
      if (eventName === 'snapshot') {
        handleSnapshotEvent(rawData, controls);
        return;
      }
      if (eventName === 'job') {
        handleJobEvent(rawData, controls);
        return;
      }
      handleFetchDetailEvent(rawData);
    },
    maxRetries: MAX_SSE_RETRIES,
    onMaxRetriesExceeded: () => logger.error('Sync SSE max retries exceeded', { jobId }),
  });
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
      return resolveSyncJobPollInterval(query.state.data?.status, sseConnected);
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
      return resolveSyncJobPollInterval(query.state.data?.status, sseConnected);
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
      return resolveActiveSyncJobPollInterval(query.state.data?.status);
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
