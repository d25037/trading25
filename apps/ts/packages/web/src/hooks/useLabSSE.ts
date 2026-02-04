import { useCallback, useEffect, useRef, useState } from 'react';
import type { JobStatus } from '@/types/backtest';
import { logger } from '@/utils/logger';

interface LabSSEState {
  progress: number | null;
  message: string | null;
  status: JobStatus | null;
  isConnected: boolean;
}

const MAX_RETRIES = 3;
const TERMINAL_STATUSES: JobStatus[] = ['completed', 'failed', 'cancelled'];

export function useLabSSE(jobId: string | null): LabSSEState {
  const [state, setState] = useState<LabSSEState>({
    progress: null,
    message: null,
    status: null,
    isConnected: false,
  });
  const retryCountRef = useRef(0);
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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
      setState({ progress: null, message: null, status: null, isConnected: false });
      retryCountRef.current = 0;
      return;
    }

    const connect = () => {
      cleanup();

      const url = `/bt/api/lab/jobs/${encodeURIComponent(jobId)}/stream`;
      const es = new EventSource(url);
      eventSourceRef.current = es;

      es.onopen = () => {
        logger.debug('Lab SSE connected', { jobId });
        retryCountRef.current = 0;
        setState((prev) => ({ ...prev, isConnected: true }));
      };

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          const status = data.status as JobStatus;
          setState({
            progress: data.progress ?? null,
            message: data.message ?? null,
            status,
            isConnected: true,
          });

          if (TERMINAL_STATUSES.includes(status)) {
            cleanup();
            setState((prev) => ({ ...prev, isConnected: false }));
          }
        } catch (e) {
          logger.error('Lab SSE parse error', { error: String(e) });
        }
      };

      es.onerror = () => {
        cleanup();
        retryCountRef.current += 1;

        if (retryCountRef.current <= MAX_RETRIES) {
          logger.debug('Lab SSE reconnecting', { attempt: retryCountRef.current, jobId });
          reconnectTimerRef.current = setTimeout(connect, 1000 * retryCountRef.current);
        } else {
          logger.error('Lab SSE max retries exceeded', { jobId });
          setState((prev) => ({ ...prev, isConnected: false }));
        }
      };
    };

    connect();

    return cleanup;
  }, [jobId, cleanup]);

  return state;
}
