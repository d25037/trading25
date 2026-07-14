import type { CancelableTimer } from './cancelable-timer';
import { normalizeShikihoCode, parseShikihoSnapshot } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { CaptureNowResponse, ProbeShikihoCodeResponse, ShikihoTabRequest } from './shikiho-tab-bridge';
import {
  type WarmTabHandle,
  type WarmTabLeaseManager,
  type WarmTabMode,
  WarmTabReloadDeadlineError,
} from './warm-tab-lease';

export const SHIKIHO_PROBE_TIMEOUT_MS = 500;
export const SHIKIHO_CAPTURE_TIMEOUT_MS = 25 * 1000;
export const SHIKIHO_RELOAD_AFTER_MS = 7 * 1000;
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
  timing: ShikihoCaptureTiming;
}

export interface TabMessageReply {
  tabId: number;
  response: unknown;
}

export interface ShikihoTabAcquisitionDeps {
  now(): number;
  delay(ms: number): Promise<void>;
  createTimer(ms: number): CancelableTimer;
  createRequestId(): string;
  queryTabs(): Promise<Array<{ id?: number }>>;
  sendTabMessage(tabId: number, message: ShikihoTabRequest): Promise<TabMessageReply>;
  getValidWarmTabId(): Promise<number | null>;
  leaseManager: WarmTabLeaseManager;
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

class SupersededCaptureError extends Error {
  constructor() {
    super('Shikiho capture attempt was superseded');
    this.name = 'SupersededCaptureError';
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

interface CaptureAttemptState {
  superseded: boolean;
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

function parseCaptureReply(
  reply: TabMessageReply,
  selectedTabId: number,
  requestId: string,
  code: string
): CaptureNowResponse | null {
  if (reply.tabId !== selectedTabId || !isRecord(reply.response)) return null;
  if (!hasExactKeys(reply.response, ['type', 'requestId', 'code', 'result'])) return null;
  if (
    reply.response.type !== 'capture_result' ||
    reply.response.requestId !== requestId ||
    reply.response.code !== code
  ) {
    return null;
  }
  const result = parseExtractionResult(reply.response.result, code);
  return result === null ? null : { type: 'capture_result', requestId, code, result };
}

function outcomeOf(result: ShikihoExtractionResult): CaptureOutcome {
  if (result.kind !== 'success') return 'diagnostic';
  return result.snapshot.status === 'partial' ? 'partial' : 'success';
}

function canonicalCode(codeValue: string): string {
  const code = normalizeShikihoCode(codeValue);
  if (code === null || code !== codeValue)
    throw new Error(`Expected a canonical four-digit Shikiho code: ${codeValue}`);
  return code;
}

export function createShikihoTabAcquisition(deps: ShikihoTabAcquisitionDeps): ShikihoTabAcquisition {
  function elapsed(start: number): number {
    return Math.max(0, deps.now() - start);
  }

  async function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
    const timer = deps.createTimer(timeoutMs);
    try {
      return await Promise.race([
        operation,
        timer.promise.then(() => {
          throw new AcquisitionTimeoutError(timeoutMs);
        }),
      ]);
    } finally {
      timer.cancel();
    }
  }

  async function probeTab(tabId: number, expectedCode: string): Promise<number | null> {
    const reply = await withTimeout(
      deps.sendTabMessage(tabId, { type: 'probe_shikiho_code' }),
      SHIKIHO_PROBE_TIMEOUT_MS
    );
    return parseProbeReply(reply, tabId)?.code === expectedCode ? tabId : null;
  }

  async function captureTab(
    tabId: number,
    code: string,
    waitForReady: boolean,
    timeoutMs: number | null = SHIKIHO_CAPTURE_TIMEOUT_MS
  ): Promise<ShikihoExtractionResult> {
    const requestId = deps.createRequestId();
    let reply: TabMessageReply;
    try {
      const operation = deps.sendTabMessage(tabId, { type: 'capture_now', requestId, code, waitForReady });
      reply = timeoutMs === null ? await operation : await withTimeout(operation, timeoutMs);
    } catch (error) {
      if (waitForReady && error instanceof Error && error.message === SHIKIHO_RECEIVER_MISSING_MESSAGE) {
        throw new ReceiverUnavailableError();
      }
      throw error;
    }
    const parsed = parseCaptureReply(reply, tabId, requestId, code);
    if (parsed === null) throw new Error('Invalid Shikiho capture response');
    return parsed.result;
  }

  async function captureOwnedTab(handle: WarmTabHandle, code: string): Promise<ShikihoExtractionResult> {
    const deadline = deps.now() + SHIKIHO_CAPTURE_TIMEOUT_MS;
    const firstState: CaptureAttemptState = { superseded: false };
    let activeState = firstState;
    let timedOut = false;
    const milestoneTimer = deps.createTimer(SHIKIHO_RELOAD_AFTER_MS);
    const timeoutTimer = deps.createTimer(SHIKIHO_CAPTURE_TIMEOUT_MS);
    const throwIfExpired = (state: CaptureAttemptState): void => {
      if (!timedOut && deps.now() < deadline) return;
      state.superseded = true;
      throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    };
    const execute = async (): Promise<ShikihoExtractionResult> => {
      const first = captureUntilReady(handle.lease.tabId, code, deadline, firstState, () => timedOut);
      const phase = await Promise.race([
        first.then((result) => {
          throwIfExpired(firstState);
          return { kind: 'result' as const, result };
        }),
        milestoneTimer.promise.then(() => ({ kind: 'reload' as const })),
      ]);
      if (phase.kind === 'result') return phase.result;

      firstState.superseded = true;
      throwIfExpired(firstState);
      try {
        await deps.leaseManager.reloadOwned(handle, deadline);
      } catch (error) {
        if (error instanceof WarmTabReloadDeadlineError) throwIfExpired(firstState);
        throw error;
      }
      throwIfExpired(firstState);
      const secondState: CaptureAttemptState = { superseded: false };
      activeState = secondState;
      return captureUntilReady(handle.lease.tabId, code, deadline, secondState, () => timedOut);
    };
    try {
      return await Promise.race([
        execute(),
        timeoutTimer.promise.then(() => {
          timedOut = true;
          activeState.superseded = true;
          throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
        }),
      ]);
    } finally {
      milestoneTimer.cancel();
      timeoutTimer.cancel();
    }
  }

  async function captureUntilReady(
    tabId: number,
    code: string,
    deadline: number,
    state: CaptureAttemptState,
    hasTimedOut: () => boolean
  ): Promise<ShikihoExtractionResult> {
    while (!state.superseded) {
      throwIfCaptureExpired(deadline, state, hasTimedOut);
      const result = await tryOwnedCapture(tabId, code);
      throwIfCaptureExpired(deadline, state, hasTimedOut);
      if (state.superseded) throw new SupersededCaptureError();
      if (result !== null) return result;
      await waitForReceiverRetry(deadline);
    }
    throw new SupersededCaptureError();
  }

  function throwIfCaptureExpired(deadline: number, state: CaptureAttemptState, hasTimedOut: () => boolean): void {
    if (!hasTimedOut() && deps.now() < deadline) return;
    state.superseded = true;
    throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
  }

  async function tryOwnedCapture(tabId: number, code: string): Promise<ShikihoExtractionResult | null> {
    try {
      return await captureTab(tabId, code, true, null);
    } catch (error) {
      if (error instanceof ReceiverUnavailableError) return null;
      throw error;
    }
  }

  async function waitForReceiverRetry(deadline: number): Promise<void> {
    const retryDelayMs = Math.min(SHIKIHO_RECEIVER_RETRY_DELAY_MS, deadline - deps.now());
    if (retryDelayMs <= 0) throw new AcquisitionTimeoutError(SHIKIHO_CAPTURE_TIMEOUT_MS);
    await deps.delay(retryDelayMs);
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
    const captureStart = deps.now();
    try {
      const result = await captureTab(tabId, code, false);
      state.captureMs += elapsed(captureStart);
      return { result, timing: finishTiming(state, outcomeOf(result)) };
    } catch {
      state.captureMs += elapsed(captureStart);
      return null;
    }
  }

  async function captureOwned(code: string, state: TimingState): Promise<AcquiredShikihoResult> {
    const navigationStart = deps.now();
    const handle = await deps.leaseManager.acquire(code);
    state.navigationMs = elapsed(navigationStart);
    state.mode = handle.mode;
    let released = false;
    try {
      const ownedCaptureStart = deps.now();
      let result: ShikihoExtractionResult;
      try {
        result = await captureOwnedTab(handle, code);
      } finally {
        state.captureMs += elapsed(ownedCaptureStart);
      }
      if (result.kind === 'success') {
        await deps.leaseManager.releaseSuccess(handle, code);
      } else {
        released = true;
        await deps.leaseManager.releaseFailure(handle);
      }
      released = true;
      return { result, timing: finishTiming(state, outcomeOf(result)) };
    } catch (error) {
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
