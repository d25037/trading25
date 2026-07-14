import type { CaptureProgressBroker } from './capture-progress';
import {
  normalizeShikihoCode,
  parseShikihoCaptureTrace,
  parseShikihoSnapshot,
  type ShikihoCaptureTraceV1,
  type ShikihoTraceMode,
} from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { CaptureNowResponse, ProbeShikihoCodeResponse, ShikihoTabRequest } from './shikiho-tab-bridge';
import type { WarmTabLeaseManager, WarmTabMode } from './warm-tab-lease';

export const SHIKIHO_PROBE_TIMEOUT_MS = 500;
export const SHIKIHO_CAPTURE_TIMEOUT_MS = 25 * 1000;
const SHIKIHO_RECEIVER_RETRY_DELAY_MS = 100;
const SHIKIHO_RECEIVER_MISSING_MESSAGE = 'Could not establish connection. Receiving end does not exist.';

export type ShikihoCaptureMode = 'exact_user_tab' | WarmTabMode;

export interface ShikihoCaptureTiming {
  event: 'shikiho_capture_timing';
  mode: ShikihoCaptureMode;
  outcome: 'success' | 'partial' | 'diagnostic' | 'timeout' | 'error';
  probeMs: number;
  navigationMs: number;
  captureMs: number;
  totalMs: number;
}

export interface AcquiredShikihoResult {
  result: ShikihoExtractionResult;
  trace: ShikihoCaptureTraceV1;
  timing: ShikihoCaptureTiming;
}

export interface TabMessageReply {
  tabId: number;
  response: unknown;
}

export interface ShikihoTabAcquisitionDeps {
  now(): number;
  delay(ms: number): Promise<void>;
  createRequestId(): string;
  createAttemptId(): string;
  queryTabs(): Promise<Array<{ id?: number }>>;
  sendTabMessage(tabId: number, message: ShikihoTabRequest): Promise<TabMessageReply>;
  getValidWarmTabId(): Promise<number | null>;
  leaseManager: WarmTabLeaseManager;
  progress: Pick<
    CaptureProgressBroker,
    'registerAttempt' | 'recordReceiverAttempt' | 'finishAttempt' | 'abandonAttempt'
  >;
  logTiming(timing: ShikihoCaptureTiming): void;
}

export interface ShikihoTabAcquisition {
  capture(code: string): Promise<AcquiredShikihoResult>;
}

class AcquisitionTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Shikiho acquisition timed out after ${timeoutMs}ms`);
    this.name = 'AcquisitionTimeoutError';
  }
}

class ReceiverUnavailableError extends Error {
  constructor() {
    super('Shikiho content-script receiver is not available');
    this.name = 'ReceiverUnavailableError';
  }
}

class NavigationChangedError extends Error {
  constructor() {
    super('Shikiho tab navigation changed before capture completed');
    this.name = 'NavigationChangedError';
  }
}

type CaptureOutcome = ShikihoCaptureTiming['outcome'];

interface TimingState {
  totalStart: number;
  mode: ShikihoCaptureMode;
  probeMs: number;
  navigationMs: number;
  captureMs: number;
  logged: boolean;
}

interface AttemptContext {
  attemptId: string;
  tabId: number;
  code: string;
  mode: ShikihoTraceMode;
  startedAt: number;
  receiverStartedAt: number;
  probeMs: number;
  acquisitionMs: number;
  receiverMs: number;
  deadlineMs: number;
  receiverAttempts: number;
  receiverReadyMs: number;
  terminal: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return actual.length === sortedExpected.length && actual.every((key, index) => key === sortedExpected[index]);
}

function parseProbeReply(reply: TabMessageReply, selectedTabId: number): ProbeShikihoCodeResponse | null {
  if (reply.tabId !== selectedTabId || !isRecord(reply.response)) return null;
  if (!hasExactKeys(reply.response, ['type', 'code']) || reply.response.type !== 'shikiho_code') return null;
  if (reply.response.code === null) return { type: 'shikiho_code', code: null };
  const code = normalizeShikihoCode(reply.response.code);
  return code === reply.response.code ? { type: 'shikiho_code', code } : null;
}

function parseExtractionResult(value: unknown, expectedCode: string): ShikihoExtractionResult | null {
  if (!isRecord(value)) return null;
  if (value.kind === 'success' && hasExactKeys(value, ['kind', 'snapshot'])) {
    const snapshot = parseShikihoSnapshot(value.snapshot);
    return snapshot?.code === expectedCode ? { kind: 'success', snapshot } : null;
  }
  if (
    (value.kind === 'login_required' || value.kind === 'page_changed') &&
    hasExactKeys(value, ['kind', 'code']) &&
    value.code === expectedCode
  ) {
    return { kind: value.kind, code: expectedCode };
  }
  return null;
}

function terminalTraceMatchesResult(trace: ShikihoCaptureTraceV1, result: ShikihoExtractionResult): boolean {
  if (result.kind === 'success') {
    return (
      trace.phase === 'complete' &&
      trace.outcome === (result.snapshot.status === 'partial' ? 'partial' : 'success') &&
      trace.waitEndReason === (result.snapshot.status === 'partial' ? 'deadline' : 'field_stable')
    );
  }
  return (
    trace.phase === 'error' &&
    trace.outcome === result.kind &&
    (result.kind === 'login_required' ? trace.waitEndReason === 'login_confirmed' : trace.waitEndReason === 'deadline')
  );
}

function parseCaptureReply(
  reply: TabMessageReply,
  selectedTabId: number,
  requestId: string,
  attemptId: string,
  code: string
): CaptureNowResponse | null {
  if (reply.tabId !== selectedTabId || !isRecord(reply.response)) return null;
  if (!hasExactKeys(reply.response, ['type', 'requestId', 'attemptId', 'code', 'result', 'trace'])) return null;
  if (
    reply.response.type !== 'capture_result' ||
    reply.response.requestId !== requestId ||
    reply.response.attemptId !== attemptId ||
    reply.response.code !== code
  ) {
    return null;
  }
  const result = parseExtractionResult(reply.response.result, code);
  const trace = parseShikihoCaptureTrace(reply.response.trace);
  if (
    result === null ||
    trace === null ||
    trace.attemptId !== attemptId ||
    trace.code !== code ||
    !terminalTraceMatchesResult(trace, result)
  ) {
    return null;
  }
  return { type: 'capture_result', requestId, attemptId, code, result, trace };
}

function outcomeOf(result: ShikihoExtractionResult): CaptureOutcome {
  if (result.kind !== 'success') return 'diagnostic';
  return result.snapshot.status === 'partial' ? 'partial' : 'success';
}

function traceMatchesAttemptOrigin(trace: ShikihoCaptureTraceV1, attempt: AttemptContext): boolean {
  return (
    trace.startedAt === new Date(attempt.startedAt).toISOString() &&
    trace.timings.probeMs === attempt.probeMs &&
    trace.timings.acquisitionMs === attempt.acquisitionMs &&
    trace.timings.receiverMs === attempt.receiverMs &&
    trace.timings.totalMs >= attempt.receiverReadyMs
  );
}

function canonicalCode(codeValue: string): string {
  const code = normalizeShikihoCode(codeValue);
  if (code === null || code !== codeValue)
    throw new Error(`Expected a canonical four-digit Shikiho code: ${codeValue}`);
  return code;
}

function traceMode(mode: ShikihoCaptureMode): ShikihoTraceMode {
  return mode === 'warm_owned_navigation' ? 'warm_owned_navigated' : mode;
}

const TRACE_FIELDS = [
  'identity',
  'quote',
  'features',
  'consolidatedBusinesses',
  'commentary',
  'score',
  'comparisonCompanies',
  'industries',
  'marketThemes',
  'profile',
  'editionLabel',
  'pageUpdatedAt',
  'coreReady',
] as const;

export function createShikihoTabAcquisition(deps: ShikihoTabAcquisitionDeps): ShikihoTabAcquisition {
  function elapsed(start: number): number {
    return Math.max(0, deps.now() - start);
  }

  async function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
    if (timeoutMs <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    return Promise.race([
      operation,
      deps.delay(timeoutMs).then(() => {
        throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
      }),
    ]);
  }

  async function probeTab(tabId: number, expectedCode: string): Promise<number | null> {
    const reply = await withTimeout(
      deps.sendTabMessage(tabId, { type: 'probe_shikiho_code' }),
      SHIKIHO_PROBE_TIMEOUT_MS
    );
    return parseProbeReply(reply, tabId)?.code === expectedCode ? tabId : null;
  }

  function startAttempt(tabId: number, code: string, mode: ShikihoCaptureMode, state: TimingState): AttemptContext {
    const startedAt = state.totalStart;
    const attempt: AttemptContext = {
      attemptId: deps.createAttemptId(),
      tabId,
      code,
      mode: traceMode(mode),
      startedAt,
      receiverStartedAt: deps.now(),
      probeMs: state.probeMs,
      acquisitionMs: state.navigationMs,
      receiverMs: 0,
      deadlineMs: startedAt + SHIKIHO_CAPTURE_TIMEOUT_MS,
      receiverAttempts: 0,
      receiverReadyMs: 0,
      terminal: false,
    };
    deps.progress.registerAttempt({
      attemptId: attempt.attemptId,
      tabId,
      code,
      mode: attempt.mode,
      startedAtMs: startedAt,
      probeMs: attempt.probeMs,
      acquisitionMs: attempt.acquisitionMs,
    });
    return attempt;
  }

  function abandonAttempt(attempt: AttemptContext): void {
    if (attempt.terminal) return;
    attempt.terminal = true;
    deps.progress.abandonAttempt(attempt.attemptId);
  }

  async function finishAttempt(attempt: AttemptContext, trace: ShikihoCaptureTraceV1): Promise<void> {
    if (attempt.terminal) return;
    attempt.terminal = true;
    await deps.progress.finishAttempt(attempt.attemptId, trace);
  }

  function syntheticTerminalTrace(attempt: AttemptContext, outcome: 'timeout' | 'error'): ShikihoCaptureTraceV1 {
    const updatedAt = deps.now();
    const totalMs = Math.max(0, updatedAt - attempt.startedAt);
    return {
      schemaVersion: 1,
      attemptId: attempt.attemptId,
      code: attempt.code,
      mode: attempt.mode,
      phase: outcome === 'timeout' ? 'timeout' : 'error',
      startedAt: new Date(attempt.startedAt).toISOString(),
      updatedAt: new Date(updatedAt).toISOString(),
      outcome,
      waitEndReason: outcome === 'timeout' ? 'deadline' : 'error',
      receiverAttempts: attempt.receiverAttempts,
      receiverReadyMs: attempt.receiverReadyMs,
      documentReadyState: null,
      navigation: { responseStartMs: null, domInteractiveMs: null, domContentLoadedMs: null, loadEndMs: null },
      dom: {
        firstSampleMs: null,
        mutationBatches: 0,
        meaningfulChanges: 0,
        samples: 0,
        presentFields: [],
        missingFields: [...TRACE_FIELDS],
        firstSeenMs: {
          identity: null,
          quote: null,
          features: null,
          consolidatedBusinesses: null,
          commentary: null,
          score: null,
          comparisonCompanies: null,
          industries: null,
          marketThemes: null,
          profile: null,
          editionLabel: null,
          pageUpdatedAt: null,
          coreReady: null,
        },
      },
      extraction: { samples: 0, lastMs: null, maxMs: null, totalMs: 0 },
      timings: {
        probeMs: attempt.probeMs,
        acquisitionMs: attempt.acquisitionMs,
        receiverMs: attempt.receiverMs,
        domObservationMs: 0,
        storageMs: 0,
        totalMs,
      },
    };
  }

  async function captureTab(
    attempt: AttemptContext
  ): Promise<{ result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 }> {
    const requestId = deps.createRequestId();
    attempt.receiverAttempts += 1;
    attempt.receiverReadyMs = Math.max(0, deps.now() - attempt.startedAt);
    attempt.receiverMs = Math.max(0, deps.now() - attempt.receiverStartedAt);
    deps.progress.recordReceiverAttempt(attempt.attemptId, attempt.receiverReadyMs, attempt.receiverMs);
    let reply: TabMessageReply;
    try {
      reply = await deps.sendTabMessage(attempt.tabId, {
        type: 'capture_now',
        requestId,
        attemptId: attempt.attemptId,
        code: attempt.code,
        mode: attempt.mode,
        deadlineMs: attempt.deadlineMs,
        receiverAttempts: attempt.receiverAttempts,
        receiverReadyMs: attempt.receiverReadyMs,
        startedAtMs: attempt.startedAt,
        probeMs: attempt.probeMs,
        acquisitionMs: attempt.acquisitionMs,
        receiverMs: attempt.receiverMs,
      });
    } catch (error) {
      if (error instanceof Error && error.message === SHIKIHO_RECEIVER_MISSING_MESSAGE) {
        throw new ReceiverUnavailableError();
      }
      throw error;
    }
    if (reply.response === null || reply.response === undefined) throw new NavigationChangedError();
    const parsed = parseCaptureReply(reply, attempt.tabId, requestId, attempt.attemptId, attempt.code);
    if (parsed === null || parsed.trace === undefined || !traceMatchesAttemptOrigin(parsed.trace, attempt)) {
      throw new Error('Invalid Shikiho capture response');
    }
    return { result: parsed.result, trace: parsed.trace };
  }

  async function tryOwnedReceiver(
    attempt: AttemptContext
  ): Promise<{ result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 } | null> {
    try {
      return await captureTab(attempt);
    } catch (error) {
      if (error instanceof ReceiverUnavailableError) return null;
      throw error;
    }
  }

  async function waitForOwnedReceiver(attempt: AttemptContext): Promise<void> {
    const retryDelayMs = Math.min(SHIKIHO_RECEIVER_RETRY_DELAY_MS, attempt.deadlineMs - deps.now());
    if (retryDelayMs <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    await deps.delay(retryDelayMs);
  }

  async function captureOwnedTab(
    attempt: AttemptContext
  ): Promise<{ result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 }> {
    const captureUntilReady = async () => {
      while (true) {
        if (deps.now() >= attempt.deadlineMs) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
        const captured = await tryOwnedReceiver(attempt);
        if (captured !== null) return captured;
        await waitForOwnedReceiver(attempt);
      }
    };
    return withTimeout(captureUntilReady(), SHIKIHO_CAPTURE_TIMEOUT_MS);
  }

  function finishTiming(state: TimingState, outcome: CaptureOutcome): ShikihoCaptureTiming {
    const timing: ShikihoCaptureTiming = {
      event: 'shikiho_capture_timing',
      mode: state.mode,
      outcome,
      probeMs: Math.max(0, state.probeMs),
      navigationMs: Math.max(0, state.navigationMs),
      captureMs: Math.max(0, state.captureMs),
      totalMs: elapsed(state.totalStart),
    };
    deps.logTiming(timing);
    state.logged = true;
    return timing;
  }

  async function discoverExactTabs(code: string, state: TimingState): Promise<number[]> {
    const probeStart = deps.now();
    const tabs = await deps.queryTabs().catch(() => []);
    const tabIds = [
      ...new Set(
        tabs.map(({ id }) => id).filter((id): id is number => typeof id === 'number' && Number.isInteger(id) && id >= 0)
      ),
    ];
    const probes = await Promise.allSettled(
      tabIds.map(async (tabId) => ({ tabId, reply: await probeTab(tabId, code) }))
    );
    state.probeMs = elapsed(probeStart);
    return probes
      .flatMap((probe) => (probe.status === 'fulfilled' && probe.value.reply !== null ? [probe.value.tabId] : []))
      .sort((left, right) => left - right);
  }

  async function tryExactCapture(
    tabId: number,
    code: string,
    state: TimingState
  ): Promise<AcquiredShikihoResult | null> {
    state.mode = 'exact_user_tab';
    const attempt = startAttempt(tabId, code, state.mode, state);
    const captureStart = deps.now();
    try {
      const captured = await withTimeout(captureTab(attempt), SHIKIHO_CAPTURE_TIMEOUT_MS);
      state.captureMs += elapsed(captureStart);
      await finishAttempt(attempt, captured.trace);
      return { ...captured, timing: finishTiming(state, outcomeOf(captured.result)) };
    } catch {
      state.captureMs += elapsed(captureStart);
      abandonAttempt(attempt);
      return null;
    }
  }

  async function captureOwned(code: string, state: TimingState): Promise<AcquiredShikihoResult> {
    const navigationStart = deps.now();
    const handle = await deps.leaseManager.acquire(code);
    state.navigationMs = elapsed(navigationStart);
    state.mode = handle.mode;
    const attempt = startAttempt(handle.lease.tabId, code, state.mode, state);
    let released = false;
    try {
      const ownedCaptureStart = deps.now();
      let captured: { result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 };
      try {
        captured = await captureOwnedTab(attempt);
      } finally {
        state.captureMs += elapsed(ownedCaptureStart);
      }
      await finishAttempt(attempt, captured.trace);
      if (captured.result.kind === 'success') await deps.leaseManager.releaseSuccess(handle, code);
      else await deps.leaseManager.releaseFailure(handle);
      released = true;
      return { ...captured, timing: finishTiming(state, outcomeOf(captured.result)) };
    } catch (error) {
      if (error instanceof NavigationChangedError) abandonAttempt(attempt);
      else {
        await finishAttempt(
          attempt,
          syntheticTerminalTrace(attempt, error instanceof AcquisitionTimeoutError ? 'timeout' : 'error')
        );
      }
      if (!released) await deps.leaseManager.releaseFailure(handle);
      throw error;
    }
  }

  async function executeCapture(code: string, state: TimingState): Promise<AcquiredShikihoResult> {
    const exactIds = await discoverExactTabs(code, state);
    const validWarmTabId = await deps.getValidWarmTabId().catch(() => null);
    const ownedExact = validWarmTabId !== null && exactIds.includes(validWarmTabId);
    if (!ownedExact && exactIds.length > 0) {
      const direct = await tryExactCapture(exactIds[0] as number, code, state);
      if (direct !== null) return direct;
    }
    return captureOwned(code, state);
  }

  function capture(codeValue: string): Promise<AcquiredShikihoResult> {
    const code = canonicalCode(codeValue);
    const state: TimingState = {
      totalStart: deps.now(),
      mode: 'new_owned_tab',
      probeMs: 0,
      navigationMs: 0,
      captureMs: 0,
      logged: false,
    };
    return executeCapture(code, state).catch((error: unknown) => {
      if (!state.logged) finishTiming(state, error instanceof AcquisitionTimeoutError ? 'timeout' : 'error');
      throw error;
    });
  }

  return { capture };
}
