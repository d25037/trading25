import { normalizeShikihoCode } from './contract';

type TimerHandle = number;

interface ObserverHandle {
  disconnect(): void;
}

export interface CaptureControllerOptions {
  capture(code: string): void | Promise<void>;
  getCode(): string | null;
  observe?(callback: () => void): ObserverHandle;
  quietPeriodMs?: number;
  initialMaxWaitMs?: number;
  setTimeout?(callback: () => void, delay: number): TimerHandle;
  clearTimeout?(handle: TimerHandle): void;
}

function observeDocument(callback: () => void): ObserverHandle {
  const observer = new MutationObserver(callback);
  const root = document.documentElement;
  if (root !== null) observer.observe(root, { childList: true, subtree: true, characterData: true });
  return observer;
}

export function createCaptureController(options: CaptureControllerOptions): { start(): void; stop(): void } {
  const quietPeriodMs = options.quietPeriodMs ?? 500;
  const initialMaxWaitMs = options.initialMaxWaitMs ?? 10_000;
  const scheduleTimeout = options.setTimeout ?? ((callback, delay) => window.setTimeout(callback, delay));
  const cancelTimeout = options.clearTimeout ?? ((handle) => window.clearTimeout(handle));
  const observe = options.observe ?? observeDocument;
  let observer: ObserverHandle | null = null;
  let quietTimer: TimerHandle | null = null;
  let initialMaxTimer: TimerHandle | null = null;
  let started = false;
  let initialCapturePending = true;
  let scheduledCode: string | null = null;

  function clearQuietTimer(): void {
    if (quietTimer === null) return;
    cancelTimeout(quietTimer);
    quietTimer = null;
  }

  function clearInitialMaxTimer(): void {
    if (initialMaxTimer === null) return;
    cancelTimeout(initialMaxTimer);
    initialMaxTimer = null;
  }

  function captureScheduledCode(): void {
    if (!started) return;
    clearQuietTimer();
    const currentCode = normalizeShikihoCode(options.getCode());
    if (currentCode !== scheduledCode) scheduledCode = currentCode;
    if (initialCapturePending) {
      initialCapturePending = false;
      clearInitialMaxTimer();
    }
    if (scheduledCode !== null) void options.capture(scheduledCode);
  }

  function scheduleCapture(): void {
    if (!started) return;
    scheduledCode = normalizeShikihoCode(options.getCode());
    clearQuietTimer();
    quietTimer = scheduleTimeout(captureScheduledCode, quietPeriodMs);
  }

  return {
    start(): void {
      if (started) return;
      started = true;
      initialCapturePending = true;
      observer = observe(scheduleCapture);
      scheduleCapture();
      initialMaxTimer = scheduleTimeout(captureScheduledCode, initialMaxWaitMs);
    },

    stop(): void {
      if (!started) return;
      started = false;
      observer?.disconnect();
      observer = null;
      clearQuietTimer();
      clearInitialMaxTimer();
      scheduledCode = null;
    },
  };
}
