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
  latestTrace: ShikihoCaptureTraceV1 | null;
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

function isTerminalTrace(trace: ShikihoCaptureTraceV1): boolean {
  if (trace.phase === 'complete') return trace.outcome === 'success' || trace.outcome === 'partial';
  if (trace.phase === 'timeout') return trace.outcome === 'timeout';
  if (trace.phase === 'error') {
    return trace.outcome === 'login_required' || trace.outcome === 'page_changed' || trace.outcome === 'error';
  }
  return false;
}

function maxNullable(left: number | null, right: number | null): number | null {
  if (left === null) return right;
  if (right === null) return left;
  return Math.max(left, right);
}

function mergeTerminalWithLatest(
  terminal: ShikihoCaptureTraceV1,
  latest: ShikihoCaptureTraceV1 | null
): ShikihoCaptureTraceV1 {
  if (latest === null) return terminal;
  const base = terminal.outcome === 'timeout' || terminal.outcome === 'error' ? latest : terminal;
  return {
    ...base,
    phase: terminal.phase,
    outcome: terminal.outcome,
    waitEndReason: terminal.waitEndReason,
    updatedAt: terminal.updatedAt,
    dom: {
      ...base.dom,
      firstSampleMs:
        terminal.dom.firstSampleMs === null
          ? latest.dom.firstSampleMs
          : latest.dom.firstSampleMs === null
            ? terminal.dom.firstSampleMs
            : Math.min(terminal.dom.firstSampleMs, latest.dom.firstSampleMs),
      mutationBatches: Math.max(terminal.dom.mutationBatches, latest.dom.mutationBatches),
      meaningfulChanges: Math.max(terminal.dom.meaningfulChanges, latest.dom.meaningfulChanges),
      samples: Math.max(terminal.dom.samples, latest.dom.samples),
      firstSeenMs: Object.fromEntries(
        Object.keys(terminal.dom.firstSeenMs).map((field) => {
          const key = field as keyof typeof terminal.dom.firstSeenMs;
          const terminalValue = terminal.dom.firstSeenMs[key];
          const latestValue = latest.dom.firstSeenMs[key];
          return [
            key,
            terminalValue === null
              ? latestValue
              : latestValue === null
                ? terminalValue
                : Math.min(terminalValue, latestValue),
          ];
        })
      ) as unknown as ShikihoCaptureTraceV1['dom']['firstSeenMs'],
    },
    extraction: {
      samples: Math.max(terminal.extraction.samples, latest.extraction.samples),
      lastMs: maxNullable(terminal.extraction.lastMs, latest.extraction.lastMs),
      maxMs: maxNullable(terminal.extraction.maxMs, latest.extraction.maxMs),
      totalMs: Math.max(terminal.extraction.totalMs, latest.extraction.totalMs),
    },
    timings: {
      probeMs: Math.max(terminal.timings.probeMs, latest.timings.probeMs),
      acquisitionMs: Math.max(terminal.timings.acquisitionMs, latest.timings.acquisitionMs),
      receiverMs: Math.max(terminal.timings.receiverMs, latest.timings.receiverMs),
      domObservationMs: Math.max(terminal.timings.domObservationMs, latest.timings.domObservationMs),
      storageMs: Math.max(terminal.timings.storageMs, latest.timings.storageMs),
      totalMs: Math.max(terminal.timings.totalMs, latest.timings.totalMs),
    },
  };
}

export function createCaptureProgressBroker(deps: CaptureProgressBrokerDeps): CaptureProgressBroker {
  const attempts = new Map<string, AttemptState>();
  const subscriptions = new Map<ProgressPort, string>();
  const cleanups = new Map<ProgressPort, () => void>();

  function registerAttempt(input: ActiveCaptureAttempt): void {
    if (!isAttempt(input) || attempts.has(input.attemptId)) return;
    attempts.set(input.attemptId, {
      ...input,
      lastSequence: 0,
      receiverAttempts: 0,
      receiverReadyMs: null,
      latestTrace: null,
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
    attempt.latestTrace = trustedProgress.trace;
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
    const trace = parseShikihoCaptureTrace(input);
    if (trace === null || trace.attemptId !== attemptId || trace.code !== attempt.code || !isTerminalTrace(trace)) {
      return;
    }
    const trustedTrace = parseShikihoCaptureTrace(
      mergeAttemptTrace(mergeTerminalWithLatest(trace, attempt.latestTrace), attempt)
    );
    if (trustedTrace === null) return;
    attempts.delete(attemptId);
    await deps.saveTrace(trustedTrace);
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
