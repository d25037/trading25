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
import type { WarmTabHandle, WarmTabLeaseManager, WarmTabMode } from './warm-tab-lease';

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
    | 'registerAcquisition'
    | 'updateAcquisition'
    | 'finishAcquisition'
    | 'registerAttempt'
    | 'recordReceiverAttempt'
    | 'finishAttempt'
    | 'abandonAttempt'
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
  acquisitionAttemptId: string;
  acquisitionPhase: 'queued' | 'probing_tabs' | 'acquiring_tab';
  acquisitionPhaseStartedAt: number;
  acquisitionBound: boolean;
  acquisitionTerminal: boolean;
  mode: ShikihoCaptureMode;
  probeMs: number;
  navigationMs: number;
  captureMs: number;
  logged: boolean;
  expired: boolean;
  deadlinePromise: Promise<never> | null;
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
    throw new Error(`Expected a canonical four-character Shikiho code: ${codeValue}`);
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

  function remainingAttemptMs(attempt: AttemptContext): number {
    return Math.max(0, attempt.deadlineMs - deps.now());
  }

  function remainingCaptureMs(state: TimingState): number {
    if (state.expired) return 0;
    return Math.max(0, state.totalStart + SHIKIHO_CAPTURE_TIMEOUT_MS - deps.now());
  }

  function assertCaptureBudget(state: TimingState): void {
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
  }

  function deadlinePromise(state: TimingState): Promise<never> {
    if (state.deadlinePromise === null) throw new Error('Shikiho capture deadline is not initialized');
    return state.deadlinePromise;
  }

  function startOverallDeadline(state: TimingState): void {
    const timeoutMs = remainingCaptureMs(state);
    state.deadlinePromise = deps.delay(timeoutMs).then(() => {
      state.expired = true;
      throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    });
  }

  function acquisitionTrace(
    code: string,
    state: TimingState,
    outcome: 'timeout' | 'error' | null = null
  ): ShikihoCaptureTraceV1 {
    const now = deps.now();
    const totalMs = Math.max(0, now - state.totalStart);
    const activePhaseMs = Math.max(0, now - state.acquisitionPhaseStartedAt);
    const probeMs = state.acquisitionPhase === 'probing_tabs' ? Math.max(state.probeMs, activePhaseMs) : state.probeMs;
    const acquisitionMs =
      state.acquisitionPhase === 'acquiring_tab' ? Math.max(state.navigationMs, activePhaseMs) : state.navigationMs;
    return {
      schemaVersion: 1,
      attemptId: state.acquisitionAttemptId,
      code,
      mode: 'acquisition_unbound',
      phase: state.acquisitionPhase,
      startedAt: new Date(state.totalStart).toISOString(),
      updatedAt: new Date(now).toISOString(),
      outcome,
      waitEndReason: outcome === 'timeout' ? 'deadline' : outcome === 'error' ? 'error' : null,
      receiverAttempts: 0,
      receiverReadyMs: null,
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
        probeMs,
        acquisitionMs,
        receiverMs: 0,
        domObservationMs: 0,
        storageMs: 0,
        totalMs,
      },
    };
  }

  function setAcquisitionPhase(code: string, state: TimingState, phase: TimingState['acquisitionPhase']): void {
    if (state.acquisitionBound || state.acquisitionTerminal) return;
    if (state.acquisitionPhase !== phase) state.acquisitionPhaseStartedAt = deps.now();
    state.acquisitionPhase = phase;
    deps.progress.updateAcquisition(state.acquisitionAttemptId, acquisitionTrace(code, state));
  }

  async function runWithinOverallDeadline<T>(state: TimingState, start: () => Promise<T>): Promise<T> {
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    return Promise.race([start(), deadlinePromise(state)]);
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

  async function probeTab(tabId: number, expectedCode: string, state: TimingState): Promise<number | null> {
    const remainingMs = remainingCaptureMs(state);
    const timeoutMs = Math.min(SHIKIHO_PROBE_TIMEOUT_MS, remainingMs);
    try {
      const reply = await runWithinOverallDeadline(state, () =>
        withTimeout(deps.sendTabMessage(tabId, { type: 'probe_shikiho_code' }), timeoutMs)
      );
      return parseProbeReply(reply, tabId)?.code === expectedCode ? tabId : null;
    } catch (error) {
      if (error instanceof AcquisitionTimeoutError && timeoutMs >= remainingMs) state.expired = true;
      throw error;
    }
  }

  function startAttempt(tabId: number, code: string, mode: ShikihoCaptureMode, state: TimingState): AttemptContext {
    const startedAt = state.totalStart;
    const attempt: AttemptContext = {
      attemptId: state.acquisitionBound ? deps.createAttemptId() : state.acquisitionAttemptId,
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
    state.acquisitionBound = true;
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

  async function waitForOwnedReceiver(attempt: AttemptContext, state: TimingState): Promise<void> {
    const retryDelayMs = Math.min(SHIKIHO_RECEIVER_RETRY_DELAY_MS, attempt.deadlineMs - deps.now());
    if (retryDelayMs <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    await runWithinOverallDeadline(state, () => deps.delay(retryDelayMs));
  }

  async function captureOwnedTab(
    attempt: AttemptContext,
    state: TimingState
  ): Promise<{ result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 }> {
    const captureUntilReady = async () => {
      while (true) {
        if (remainingAttemptMs(attempt) <= 0 || remainingCaptureMs(state) <= 0) {
          throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
        }
        const captured = await tryOwnedReceiver(attempt);
        if (captured !== null) return captured;
        await waitForOwnedReceiver(attempt, state);
      }
    };
    return runWithinOverallDeadline(state, captureUntilReady);
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
    setAcquisitionPhase(code, state, 'probing_tabs');
    const probeStart = deps.now();
    let tabs: Array<{ id?: number }>;
    try {
      tabs = await runWithinOverallDeadline(state, deps.queryTabs);
    } catch (error) {
      if (error instanceof AcquisitionTimeoutError) throw error;
      tabs = [];
    }
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    const tabIds = [
      ...new Set(
        tabs.map(({ id }) => id).filter((id): id is number => typeof id === 'number' && Number.isInteger(id) && id >= 0)
      ),
    ];
    const probes = await Promise.allSettled(
      tabIds.map(async (tabId) => ({ tabId, reply: await probeTab(tabId, code, state) }))
    );
    state.probeMs = elapsed(probeStart);
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    return probes
      .flatMap((probe) => (probe.status === 'fulfilled' && probe.value.reply !== null ? [probe.value.tabId] : []))
      .sort((left, right) => left - right);
  }

  async function tryExactCapture(
    tabId: number,
    code: string,
    state: TimingState
  ): Promise<AcquiredShikihoResult | null> {
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    state.mode = 'exact_user_tab';
    const attempt = startAttempt(tabId, code, state.mode, state);
    const captureStart = deps.now();
    try {
      const timeoutMs = remainingAttemptMs(attempt);
      if (timeoutMs <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
      const captured = await runWithinOverallDeadline(state, () => captureTab(attempt));
      state.captureMs += elapsed(captureStart);
      await finishAttempt(attempt, captured.trace);
      return { ...captured, timing: finishTiming(state, outcomeOf(captured.result)) };
    } catch (error) {
      state.captureMs += elapsed(captureStart);
      if (error instanceof AcquisitionTimeoutError || remainingCaptureMs(state) <= 0) {
        await finishAttempt(attempt, syntheticTerminalTrace(attempt, 'timeout'));
        throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
      }
      abandonAttempt(attempt);
      return null;
    }
  }

  async function rejectExpiredOwnedHandle(handle: WarmTabHandle, state: TimingState): Promise<void> {
    if (remainingCaptureMs(state) > 0) return;
    await deps.leaseManager.releaseFailure(handle);
    throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
  }

  async function acquireOwnedHandle(code: string, state: TimingState): Promise<WarmTabHandle> {
    const started: { acquisition: Promise<WarmTabHandle> | null } = { acquisition: null };
    try {
      return await runWithinOverallDeadline(state, () => {
        started.acquisition = deps.leaseManager.acquire(code);
        return started.acquisition;
      });
    } catch (error) {
      const acquisition = started.acquisition;
      if (error instanceof AcquisitionTimeoutError && acquisition !== null) {
        void acquisition
          .then((handle: WarmTabHandle) => deps.leaseManager.releaseFailure(handle))
          .catch(() => undefined);
      }
      throw error;
    }
  }

  async function resolveValidWarmTabId(state: TimingState): Promise<number | null> {
    try {
      return await runWithinOverallDeadline(state, deps.getValidWarmTabId);
    } catch (error) {
      if (error instanceof AcquisitionTimeoutError) throw error;
      return null;
    }
  }

  async function captureOwned(code: string, state: TimingState): Promise<AcquiredShikihoResult> {
    if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    const navigationStart = deps.now();
    setAcquisitionPhase(code, state, 'acquiring_tab');
    const handle = await acquireOwnedHandle(code, state);
    state.navigationMs = elapsed(navigationStart);
    state.mode = handle.mode;
    await rejectExpiredOwnedHandle(handle, state);
    const attempt = startAttempt(handle.lease.tabId, code, state.mode, state);
    let released = false;
    try {
      const ownedCaptureStart = deps.now();
      let captured: { result: ShikihoExtractionResult; trace: ShikihoCaptureTraceV1 };
      try {
        captured = await captureOwnedTab(attempt, state);
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
    assertCaptureBudget(state);
    const validWarmTabId = await resolveValidWarmTabId(state);
    assertCaptureBudget(state);
    const ownedExact = validWarmTabId !== null && exactIds.includes(validWarmTabId);
    if (!ownedExact && exactIds.length > 0) {
      const direct = await tryExactCapture(exactIds[0] as number, code, state);
      if (direct !== null) return direct;
      if (remainingCaptureMs(state) <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    }
    return captureOwned(code, state);
  }

  function capture(codeValue: string): Promise<AcquiredShikihoResult> {
    const code = canonicalCode(codeValue);
    const stateStart = deps.now();
    const state: TimingState = {
      totalStart: stateStart,
      acquisitionAttemptId: deps.createAttemptId(),
      acquisitionPhase: 'queued',
      acquisitionPhaseStartedAt: stateStart,
      acquisitionBound: false,
      acquisitionTerminal: false,
      mode: 'new_owned_tab',
      probeMs: 0,
      navigationMs: 0,
      captureMs: 0,
      logged: false,
      expired: false,
      deadlinePromise: null,
    };
    deps.progress.registerAcquisition({
      attemptId: state.acquisitionAttemptId,
      code,
      startedAtMs: state.totalStart,
    });
    deps.progress.updateAcquisition(state.acquisitionAttemptId, acquisitionTrace(code, state));
    startOverallDeadline(state);
    return executeCapture(code, state).catch(async (error: unknown) => {
      if (!state.logged) finishTiming(state, error instanceof AcquisitionTimeoutError ? 'timeout' : 'error');
      if (!state.acquisitionBound && !state.acquisitionTerminal) {
        state.acquisitionTerminal = true;
        await deps.progress.finishAcquisition(
          state.acquisitionAttemptId,
          acquisitionTrace(code, state, error instanceof AcquisitionTimeoutError ? 'timeout' : 'error')
        );
      }
      throw error;
    });
  }

  return { capture };
}
