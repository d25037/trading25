import {
  normalizeShikihoCode,
  parseShikihoCaptureProgress,
  parseShikihoCaptureTrace,
  type ShikihoCaptureProgressV1,
  type ShikihoCaptureTraceV1,
  type ShikihoTraceMode,
} from './contract';

export interface ListenerEvent<TListener extends (...args: never[]) => void> {
  addListener(listener: TListener): void;
  removeListener(listener: TListener): void;
}

export interface ProgressPort {
  postMessage(message: unknown): void;
  onMessage: ListenerEvent<(message: unknown) => void>;
  onDisconnect: ListenerEvent<() => void>;
}

export interface ActiveCaptureAttempt {
  attemptId: string;
  tabId: number;
  code: string;
  mode: ShikihoTraceMode;
  startedAtMs: number;
}

export interface CaptureProgressBroker {
  registerAttempt(input: ActiveCaptureAttempt): void;
  recordReceiverAttempt(attemptId: string, elapsedMs: number): void;
  acceptContentProgress(progress: ShikihoCaptureProgressV1, senderTabId: number): Promise<boolean>;
  finishAttempt(attemptId: string, trace: ShikihoCaptureTraceV1): Promise<void>;
  abandonAttempt(attemptId: string): void;
  attachPort(port: ProgressPort): () => void;
}

export interface CaptureProgressBrokerDeps {
  saveTrace(trace: ShikihoCaptureTraceV1): Promise<void>;
}

interface AttemptState extends ActiveCaptureAttempt {
  lastSequence: number;
  receiverAttempts: number;
  receiverReadyMs: number | null;
}

type MessageListener = (message: unknown) => void;
type DisconnectListener = () => void;

function isAttempt(input: ActiveCaptureAttempt): boolean {
  return (
    input.attemptId.length > 0 &&
    input.attemptId.length <= 256 &&
    Number.isInteger(input.tabId) &&
    input.tabId >= 0 &&
    normalizeShikihoCode(input.code) === input.code &&
    Number.isFinite(input.startedAtMs) &&
    !Number.isNaN(new Date(input.startedAtMs).getTime())
  );
}

function parseSubscription(message: unknown): string | null {
  if (typeof message !== 'object' || message === null || Array.isArray(message)) return null;
  const record = message as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (keys.length !== 2 || keys[0] !== 'code' || keys[1] !== 'type') return null;
  if (record.type !== 'subscribe_capture_progress') return null;
  return normalizeShikihoCode(record.code) === record.code ? (record.code as string) : null;
}

function mergeAttemptTrace(trace: ShikihoCaptureTraceV1, attempt: AttemptState): ShikihoCaptureTraceV1 {
  return {
    ...trace,
    mode: attempt.mode,
    startedAt: new Date(attempt.startedAtMs).toISOString(),
    receiverAttempts: attempt.receiverAttempts,
    receiverReadyMs: attempt.receiverReadyMs,
    timings: {
      ...trace.timings,
      receiverMs: attempt.receiverReadyMs ?? trace.timings.receiverMs,
    },
  };
}

export function createCaptureProgressBroker(deps: CaptureProgressBrokerDeps): CaptureProgressBroker {
  const attempts = new Map<string, AttemptState>();
  const subscriptions = new Map<ProgressPort, string>();
  const cleanups = new Map<ProgressPort, () => void>();

  function registerAttempt(input: ActiveCaptureAttempt): void {
    if (!isAttempt(input)) return;
    attempts.set(input.attemptId, {
      ...input,
      lastSequence: 0,
      receiverAttempts: 0,
      receiverReadyMs: null,
    });
  }

  function recordReceiverAttempt(attemptId: string, elapsedMs: number): void {
    const attempt = attempts.get(attemptId);
    if (
      attempt === undefined ||
      !Number.isFinite(elapsedMs) ||
      elapsedMs < 0 ||
      elapsedMs > Number.MAX_SAFE_INTEGER ||
      attempt.receiverAttempts >= Number.MAX_SAFE_INTEGER
    ) {
      return;
    }
    attempt.receiverAttempts += 1;
    attempt.receiverReadyMs = elapsedMs;
  }

  function abandonAttempt(attemptId: string): void {
    attempts.delete(attemptId);
  }

  function attachPort(port: ProgressPort): () => void {
    const existingCleanup = cleanups.get(port);
    if (existingCleanup !== undefined) return existingCleanup;
    let attached = true;
    const messageListener: MessageListener = (message) => {
      const code = parseSubscription(message);
      if (code !== null) subscriptions.set(port, code);
    };
    const cleanup = () => {
      if (!attached) return;
      attached = false;
      subscriptions.delete(port);
      cleanups.delete(port);
      port.onMessage.removeListener(messageListener);
      port.onDisconnect.removeListener(disconnectListener);
    };
    const disconnectListener: DisconnectListener = cleanup;
    cleanups.set(port, cleanup);
    port.onMessage.addListener(messageListener);
    port.onDisconnect.addListener(disconnectListener);
    return cleanup;
  }

  async function acceptContentProgress(input: ShikihoCaptureProgressV1, senderTabId: number): Promise<boolean> {
    const progress = parseShikihoCaptureProgress(input);
    if (progress === null) return false;
    const attempt = attempts.get(progress.attemptId);
    if (
      attempt === undefined ||
      attempt.tabId !== senderTabId ||
      attempt.code !== progress.code ||
      progress.sequence <= attempt.lastSequence
    ) {
      return false;
    }
    const trustedProgress = parseShikihoCaptureProgress({
      ...progress,
      trace: mergeAttemptTrace(progress.trace, attempt),
    });
    if (trustedProgress === null) return false;
    attempt.lastSequence = progress.sequence;
    for (const [port, code] of subscriptions) {
      if (code !== progress.code) continue;
      try {
        port.postMessage({ type: 'capture_progress', progress: trustedProgress });
      } catch {
        cleanups.get(port)?.();
      }
    }
    return true;
  }

  async function finishAttempt(attemptId: string, input: ShikihoCaptureTraceV1): Promise<void> {
    const attempt = attempts.get(attemptId);
    if (attempt === undefined) return;
    attempts.delete(attemptId);
    const trace = parseShikihoCaptureTrace(input);
    if (trace === null || trace.attemptId !== attemptId || trace.code !== attempt.code) return;
    const trustedTrace = parseShikihoCaptureTrace(mergeAttemptTrace(trace, attempt));
    if (trustedTrace !== null) await deps.saveTrace(trustedTrace);
  }

  return {
    registerAttempt,
    recordReceiverAttempt,
    acceptContentProgress,
    finishAttempt,
    attachPort,
    abandonAttempt,
  };
}
