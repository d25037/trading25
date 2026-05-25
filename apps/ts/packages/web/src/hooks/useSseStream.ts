import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

export interface SseStreamControls {
  close: () => void;
}

export type SseMessageHandler = (rawData: string, controls: SseStreamControls) => void;
export type SseNamedEventHandler = (eventName: string, rawData: string, controls: SseStreamControls) => void;

export interface UseSseStreamOptions {
  url: string | null;
  eventNames?: readonly string[];
  onAnyMessage?: SseMessageHandler;
  onMessage?: SseMessageHandler;
  onEvent?: SseNamedEventHandler;
  maxRetries?: number;
  retryDelayMs?: (attempt: number) => number;
  onMaxRetriesExceeded?: () => void;
}

export interface SseStreamState {
  isConnected: boolean;
}

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = (attempt: number) => attempt * 1000;
const EVENT_NAME_SEPARATOR = '\u0000';

export function useSseStream({
  url,
  eventNames = [],
  onAnyMessage,
  onMessage,
  onEvent,
  maxRetries = DEFAULT_MAX_RETRIES,
  retryDelayMs = DEFAULT_RETRY_DELAY_MS,
  onMaxRetriesExceeded,
}: UseSseStreamOptions): SseStreamState {
  const [isConnected, setIsConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryCountRef = useRef(0);
  const handlersRef = useRef({
    onAnyMessage,
    onMessage,
    onEvent,
    onMaxRetriesExceeded,
  });

  handlersRef.current = {
    onAnyMessage,
    onMessage,
    onEvent,
    onMaxRetriesExceeded,
  };

  const eventNamesKey = useMemo(() => eventNames.join(EVENT_NAME_SEPARATOR), [eventNames]);

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
    if (!url) {
      cleanup();
      retryCountRef.current = 0;
      setIsConnected(false);
      return;
    }

    let disposed = false;
    const currentEventNames = eventNamesKey ? eventNamesKey.split(EVENT_NAME_SEPARATOR) : [];

    const close = () => {
      cleanup();
      if (!disposed) {
        setIsConnected(false);
      }
    };
    const controls: SseStreamControls = { close };

    retryCountRef.current = 0;
    setIsConnected(false);

    const connect = () => {
      cleanup();

      const es = new EventSource(url);
      eventSourceRef.current = es;

      if (handlersRef.current.onMessage) {
        es.onmessage = (event) => {
          handlersRef.current.onAnyMessage?.(event.data, controls);
          handlersRef.current.onMessage?.(event.data, controls);
        };
      } else if (handlersRef.current.onAnyMessage) {
        es.onmessage = (event) => {
          handlersRef.current.onAnyMessage?.(event.data, controls);
        };
      }

      for (const eventName of currentEventNames) {
        es.addEventListener(eventName, (event) => {
          const rawData = (event as MessageEvent<string>).data;
          handlersRef.current.onAnyMessage?.(rawData, controls);
          handlersRef.current.onEvent?.(eventName, rawData, controls);
        });
      }

      es.onopen = () => {
        retryCountRef.current = 0;
        if (!disposed) {
          setIsConnected(true);
        }
      };

      es.onerror = () => {
        close();
        retryCountRef.current += 1;

        if (retryCountRef.current > maxRetries) {
          handlersRef.current.onMaxRetriesExceeded?.();
          return;
        }

        reconnectTimerRef.current = setTimeout(connect, retryDelayMs(retryCountRef.current));
      };
    };

    connect();

    return () => {
      disposed = true;
      cleanup();
    };
  }, [cleanup, eventNamesKey, maxRetries, retryDelayMs, url]);

  return { isConnected };
}
