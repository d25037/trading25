import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiDelete, apiGet, apiPost } from '@/lib/api-client';
import type { CancelJobResponse, CreateSyncJobResponse, SyncJobResponse, SyncMode } from '@/types/sync';
import { logger } from '@/utils/logger';

// Fetch functions
function startSync(mode: SyncMode): Promise<CreateSyncJobResponse> {
  return apiPost<CreateSyncJobResponse>('/api/db/sync', { mode });
}

function fetchJobStatus(jobId: string): Promise<SyncJobResponse> {
  return apiGet<SyncJobResponse>(`/api/db/sync/jobs/${jobId}`);
}

function cancelJob(jobId: string): Promise<CancelJobResponse> {
  return apiDelete<CancelJobResponse>(`/api/db/sync/jobs/${jobId}`);
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

export function useSyncJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: ['sync-job', jobId],
    queryFn: () => {
      if (!jobId) throw new Error('Job ID required');
      logger.debug('Polling sync job status', { jobId });
      return fetchJobStatus(jobId);
    },
    enabled: !!jobId,
    refetchInterval: (query) => {
      // Poll every 1s while running, stop when completed/failed/cancelled
      const status = query.state.data?.status;
      if (status === 'running' || status === 'pending') return 1000;
      return false;
    },
    staleTime: 0, // Always fetch fresh data
  });
}

export function useCancelSync() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: cancelJob,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['sync-job', data.jobId] });
      logger.debug('Sync job cancelled', { jobId: data.jobId });
    },
    onError: (error) => {
      logger.error('Failed to cancel sync', { error: error.message });
    },
  });
}
