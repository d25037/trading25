import { useCallback, useEffect, useState } from 'react';
import type { JobStatus } from '@/types/backtest';
import { isTerminalJobStatus } from '@/utils/jobStatus';
import { logger } from '@/utils/logger';
import { type SseStreamControls, useSseStream } from './useSseStream';

interface LabSSEState {
  progress: number | null;
  message: string | null;
  status: JobStatus | null;
  isConnected: boolean;
}

const MAX_RETRIES = 3;
const STATUS_EVENTS: JobStatus[] = ['pending', 'running', 'completed', 'failed', 'cancelled'];
const INITIAL_LAB_SSE_STATE: Omit<LabSSEState, 'isConnected'> = {
  progress: null,
  message: null,
  status: null,
};

interface LabSSEPayload {
  status?: unknown;
  progress?: number | null;
  message?: string | null;
}

function isJobStatus(value: unknown): value is JobStatus {
  return typeof value === 'string' && STATUS_EVENTS.includes(value as JobStatus);
}

export function useLabSSE(jobId: string | null): LabSSEState {
  const [eventState, setEventState] = useState(INITIAL_LAB_SSE_STATE);
  const streamUrl = jobId ? `/api/lab/jobs/${encodeURIComponent(jobId)}/stream` : null;

  useEffect(() => {
    if (!jobId) {
      setEventState(INITIAL_LAB_SSE_STATE);
    }
  }, [jobId]);

  const handleStatusEvent = useCallback((rawData: string, controls: SseStreamControls) => {
    try {
      const data = JSON.parse(rawData) as LabSSEPayload;
      if (!isJobStatus(data.status)) return;

      const status = data.status;
      setEventState({
        progress: data.progress ?? null,
        message: data.message ?? null,
        status,
      });

      if (isTerminalJobStatus(status)) {
        controls.close();
      }
    } catch (e) {
      logger.error('Lab SSE parse error', { error: String(e) });
    }
  }, []);

  const { isConnected } = useSseStream({
    url: streamUrl,
    eventNames: STATUS_EVENTS,
    onAnyMessage: handleStatusEvent,
    maxRetries: MAX_RETRIES,
    onMaxRetriesExceeded: () => logger.error('Lab SSE max retries exceeded', { jobId }),
  });

  return { ...eventState, isConnected };
}
