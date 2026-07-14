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
  probeMs?: number;
  acquisitionMs?: number;
}

export interface ActiveCaptureAcquisition {
  attemptId: string;
  code: string;
  startedAtMs: number;
}

export interface CaptureProgressBroker {
  registerAcquisition(input: ActiveCaptureAcquisition): void;
  updateAcquisition(attemptId: string, trace: ShikihoCaptureTraceV1): void;
  finishAcquisition(attemptId: string, trace: ShikihoCaptureTraceV1): Promise<void>;
  registerAttempt(input: ActiveCaptureAttempt): void;
  recordReceiverAttempt(attemptId: string, elapsedMs: number, receiverMs?: number): void;
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
  receiverMs: number;
  latestTrace: ShikihoCaptureTraceV1 | null;
}

interface AcquisitionState extends ActiveCaptureAcquisition {
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
  const probeMs = attempt.probeMs ?? trace.timings.probeMs;
  const acquisitionMs = attempt.acquisitionMs ?? trace.timings.acquisitionMs;
  const receiverMs = Math.max(attempt.receiverMs, trace.timings.receiverMs);
  return {
    ...trace,
    mode: attempt.mode,
    startedAt: new Date(attempt.startedAtMs).toISOString(),
    receiverAttempts: attempt.receiverAttempts,
    receiverReadyMs: attempt.receiverReadyMs,
    timings: {
      ...trace.timings,
      probeMs,
      acquisitionMs,
      receiverMs,
      totalMs: Math.max(trace.timings.totalMs, probeMs, acquisitionMs, receiverMs, attempt.receiverReadyMs ?? 0),
    },
  };
}

function isTerminalTrace(trace: ShikihoCaptureTraceV1): boolean {
  return (
    (trace.phase === 'complete' && trace.outcome === 'success' && trace.waitEndReason === 'field_stable') ||
    (trace.phase === 'complete' && trace.outcome === 'partial' && trace.waitEndReason === 'deadline') ||
    (trace.phase === 'timeout' && trace.outcome === 'timeout' && trace.waitEndReason === 'deadline') ||
    (trace.phase === 'error' && trace.outcome === 'login_required' && trace.waitEndReason === 'login_confirmed') ||
    (trace.phase === 'error' && trace.outcome === 'page_changed' && trace.waitEndReason === 'deadline') ||
    (trace.phase === 'error' &&
      trace.outcome === 'error' &&
      (trace.waitEndReason === 'error' || trace.waitEndReason === 'invalid_response'))
  );
}

function maxNullable(left: number | null, right: number | null): number | null {
  if (left === null) return right;
  if (right === null) return left;
  return Math.max(left, right);
}

function maxReadyState(left: DocumentReadyState | null, right: DocumentReadyState | null): DocumentReadyState | null {
  const states: Array<DocumentReadyState | null> = [null, 'loading', 'interactive', 'complete'];
  return states[Math.max(states.indexOf(left), states.indexOf(right))] ?? null;
}

function mergeMonotonicMetadata(
  previous: ShikihoCaptureTraceV1 | null,
  next: ShikihoCaptureTraceV1
): ShikihoCaptureTraceV1 {
  if (previous === null) return next;
  const presentFields = [...new Set([...previous.dom.presentFields, ...next.dom.presentFields])];
  const present = new Set(presentFields);
  const missingFields = [...new Set([...previous.dom.missingFields, ...next.dom.missingFields])].filter(
    (field) => !present.has(field)
  );
  return {
    ...next,
    documentReadyState: maxReadyState(previous.documentReadyState, next.documentReadyState),
    navigation: {
      responseStartMs: maxNullable(previous.navigation.responseStartMs, next.navigation.responseStartMs),
      domInteractiveMs: maxNullable(previous.navigation.domInteractiveMs, next.navigation.domInteractiveMs),
      domContentLoadedMs: maxNullable(previous.navigation.domContentLoadedMs, next.navigation.domContentLoadedMs),
      loadEndMs: maxNullable(previous.navigation.loadEndMs, next.navigation.loadEndMs),
    },
    dom: {
      ...next.dom,
      firstSampleMs:
        previous.dom.firstSampleMs === null
          ? next.dom.firstSampleMs
          : next.dom.firstSampleMs === null
            ? previous.dom.firstSampleMs
            : Math.min(previous.dom.firstSampleMs, next.dom.firstSampleMs),
      mutationBatches: Math.max(previous.dom.mutationBatches, next.dom.mutationBatches),
      meaningfulChanges: Math.max(previous.dom.meaningfulChanges, next.dom.meaningfulChanges),
      samples: Math.max(previous.dom.samples, next.dom.samples),
      presentFields,
      missingFields,
      firstSeenMs: Object.fromEntries(
        Object.keys(next.dom.firstSeenMs).map((field) => {
          const key = field as keyof typeof next.dom.firstSeenMs;
          const previousValue = previous.dom.firstSeenMs[key];
          const nextValue = next.dom.firstSeenMs[key];
          return [
            key,
            previousValue === null
              ? nextValue
              : nextValue === null
                ? previousValue
                : Math.min(previousValue, nextValue),
          ];
        })
      ) as unknown as ShikihoCaptureTraceV1['dom']['firstSeenMs'],
    },
    extraction: {
      samples: Math.max(previous.extraction.samples, next.extraction.samples),
      lastMs: maxNullable(previous.extraction.lastMs, next.extraction.lastMs),
      maxMs: maxNullable(previous.extraction.maxMs, next.extraction.maxMs),
      totalMs: Math.max(previous.extraction.totalMs, next.extraction.totalMs),
    },
    timings: {
      probeMs: Math.max(previous.timings.probeMs, next.timings.probeMs),
      acquisitionMs: Math.max(previous.timings.acquisitionMs, next.timings.acquisitionMs),
      receiverMs: Math.max(previous.timings.receiverMs, next.timings.receiverMs),
      domObservationMs: Math.max(previous.timings.domObservationMs, next.timings.domObservationMs),
      storageMs: Math.max(previous.timings.storageMs, next.timings.storageMs),
      totalMs: Math.max(previous.timings.totalMs, next.timings.totalMs),
    },
  };
}

function mergeTerminalWithLatest(
  terminal: ShikihoCaptureTraceV1,
  latest: ShikihoCaptureTraceV1 | null
): ShikihoCaptureTraceV1 {
  const merged = mergeMonotonicMetadata(latest, terminal);
  return {
    ...merged,
    phase: terminal.phase,
    outcome: terminal.outcome,
    waitEndReason: terminal.waitEndReason,
    updatedAt: terminal.updatedAt,
  };
}

export function createCaptureProgressBroker(deps: CaptureProgressBrokerDeps): CaptureProgressBroker {
  const attempts = new Map<string, AttemptState>();
  const acquisitions = new Map<string, AcquisitionState>();
  const subscriptions = new Map<ProgressPort, string>();
  const cleanups = new Map<ProgressPort, () => void>();

  function registerAcquisition(input: ActiveCaptureAcquisition): void {
    if (
      input.attemptId.length === 0 ||
      input.attemptId.length > 256 ||
      normalizeShikihoCode(input.code) !== input.code ||
      !Number.isFinite(input.startedAtMs) ||
      acquisitions.has(input.attemptId) ||
      attempts.has(input.attemptId)
    )
      return;
    acquisitions.set(input.attemptId, { ...input, latestTrace: null });
  }

  function updateAcquisition(attemptId: string, input: ShikihoCaptureTraceV1): void {
    const acquisition = acquisitions.get(attemptId);
    const trace = parseShikihoCaptureTrace(input);
    if (
      acquisition === undefined ||
      trace === null ||
      trace.mode !== 'acquisition_unbound' ||
      trace.attemptId !== attemptId ||
      trace.code !== acquisition.code ||
      trace.startedAt !== new Date(acquisition.startedAtMs).toISOString() ||
      trace.outcome !== null
    )
      return;
    acquisition.latestTrace = trace;
  }

  async function finishAcquisition(attemptId: string, input: ShikihoCaptureTraceV1): Promise<void> {
    const acquisition = acquisitions.get(attemptId);
    const trace = parseShikihoCaptureTrace(input);
    if (
      acquisition === undefined ||
      trace === null ||
      trace.mode !== 'acquisition_unbound' ||
      trace.attemptId !== attemptId ||
      trace.code !== acquisition.code ||
      trace.startedAt !== new Date(acquisition.startedAtMs).toISOString() ||
      (trace.outcome !== 'timeout' && trace.outcome !== 'error')
    )
      return;
    acquisitions.delete(attemptId);
    await deps.saveTrace(trace);
  }

  function registerAttempt(input: ActiveCaptureAttempt): void {
    if (!isAttempt(input) || attempts.has(input.attemptId)) return;
    const acquisition = acquisitions.get(input.attemptId);
    const matchingAcquisition =
      acquisition !== undefined && acquisition.code === input.code && acquisition.startedAtMs === input.startedAtMs
        ? acquisition
        : undefined;
    if (matchingAcquisition !== undefined) acquisitions.delete(input.attemptId);
    attempts.set(input.attemptId, {
      ...input,
      lastSequence: 0,
      receiverAttempts: 0,
      receiverReadyMs: null,
      receiverMs: 0,
      latestTrace: matchingAcquisition?.latestTrace ?? null,
    });
  }

  function recordReceiverAttempt(attemptId: string, elapsedMs: number, receiverMs = elapsedMs): void {
    const attempt = attempts.get(attemptId);
    if (
      attempt === undefined ||
      !Number.isFinite(elapsedMs) ||
      elapsedMs < 0 ||
      elapsedMs > Number.MAX_SAFE_INTEGER ||
      !Number.isFinite(receiverMs) ||
      receiverMs < 0 ||
      receiverMs > Number.MAX_SAFE_INTEGER ||
      attempt.receiverAttempts >= Number.MAX_SAFE_INTEGER
    ) {
      return;
    }
    attempt.receiverAttempts += 1;
    attempt.receiverReadyMs = elapsedMs;
    attempt.receiverMs = Math.max(attempt.receiverMs, receiverMs);
  }

  function abandonAttempt(attemptId: string): void {
    attempts.delete(attemptId);
    acquisitions.delete(attemptId);
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

  function broadcastProgress(progress: ShikihoCaptureProgressV1): void {
    for (const [port, code] of subscriptions) {
      if (code !== progress.code) continue;
      try {
        port.postMessage({ type: 'capture_progress', progress });
      } catch {
        cleanups.get(port)?.();
      }
    }
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
    attempt.latestTrace = mergeMonotonicMetadata(attempt.latestTrace, trustedProgress.trace);
    const mergedProgress = parseShikihoCaptureProgress({
      ...trustedProgress,
      trace: attempt.latestTrace,
    });
    if (mergedProgress === null) return false;
    broadcastProgress(mergedProgress);
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
    registerAcquisition,
    updateAcquisition,
    finishAcquisition,
    registerAttempt,
    recordReceiverAttempt,
    acceptContentProgress,
    finishAttempt,
    attachPort,
    abandonAttempt,
  };
}
