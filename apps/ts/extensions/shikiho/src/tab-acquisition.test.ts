import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoCaptureTraceV1, ShikihoSnapshotV1 } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { ShikihoTabRequest } from './shikiho-tab-bridge';
import {
  createShikihoTabAcquisition,
  SHIKIHO_CAPTURE_TIMEOUT_MS,
  SHIKIHO_PROBE_TIMEOUT_MS,
  type ShikihoCaptureTiming,
  type ShikihoTabAcquisitionDeps,
  type TabMessageReply,
} from './tab-acquisition';
import type { WarmTabHandle } from './warm-tab-lease';

const snapshot = (code = '7203', status: ShikihoSnapshotV1['status'] = 'captured'): ShikihoSnapshotV1 => ({
  schemaVersion: 1,
  extractorVersion: 'test',
  code,
  companyName: 'Toyota',
  sourceUrl: `https://shikiho.toyokeizai.net/stocks/${code}`,
  capturedAt: '2026-07-14T00:00:00.000Z',
  pageUpdatedAt: null,
  editionLabel: null,
  earningsAnnouncementDate: null,
  contentHash: 'sha256:test',
  status,
  features: 'article secret',
  consolidatedBusinesses: null,
  commentary: [],
  score: {
    overall: null,
    growth: null,
    profitability: null,
    safety: null,
    scale: null,
    value: null,
    priceMomentum: null,
  },
  comparisonCompanies: [],
  industries: [],
  marketThemes: [],
  profile: [],
  missingFields: [],
});

const success = (code = '7203', status: ShikihoSnapshotV1['status'] = 'captured'): ShikihoExtractionResult => ({
  kind: 'success',
  snapshot: snapshot(code, status),
});

const diagnostic = (code = '7203'): ShikihoExtractionResult => ({ kind: 'login_required', code });

const handle = (tabId = 99, mode: WarmTabHandle['mode'] = 'new_owned_tab', code = '7203'): WarmTabHandle => ({
  mode,
  lease: {
    version: 1,
    tabId,
    ownerToken: 'owner',
    generation: 1,
    phase: 'capturing',
    code,
    createdAt: 0,
    idleDeadline: null,
  },
});

function probeReply(tabId: number, code: string | null) {
  return { tabId, response: { type: 'shikiho_code', code } };
}

type CaptureRequest = Extract<ShikihoTabRequest, { type: 'capture_now' }>;

function captureReply(
  tabId: number,
  request: CaptureRequest,
  result: ShikihoExtractionResult,
  overrides: Partial<{
    requestId: string;
    attemptId: string;
    code: string;
    responseTabId: number;
    trace: ShikihoCaptureTraceV1;
  }> = {}
) {
  const code = overrides.code ?? request.code;
  const defaultTrace = terminalTrace(overrides.attemptId ?? request.attemptId, code, {
    mode: request.mode,
    startedAt: new Date(request.startedAtMs).toISOString(),
    receiverAttempts: request.receiverAttempts,
    receiverReadyMs: request.receiverReadyMs,
    timings: {
      probeMs: request.probeMs,
      acquisitionMs: request.acquisitionMs,
      receiverMs: request.receiverMs,
      domObservationMs: 1,
      storageMs: 0,
      totalMs: Math.max(5, request.receiverReadyMs, request.probeMs, request.acquisitionMs, request.receiverMs),
    },
    ...(result.kind === 'success'
      ? {
          phase: 'complete' as const,
          outcome: result.snapshot.status === 'partial' ? ('partial' as const) : ('success' as const),
          waitEndReason: result.snapshot.status === 'partial' ? ('deadline' as const) : ('field_stable' as const),
        }
      : {
          phase: 'error' as const,
          outcome: result.kind,
          waitEndReason: result.kind === 'login_required' ? ('login_confirmed' as const) : ('deadline' as const),
        }),
  });
  return {
    tabId: overrides.responseTabId ?? tabId,
    response: {
      type: 'capture_result',
      requestId: overrides.requestId ?? request.requestId,
      attemptId: overrides.attemptId ?? request.attemptId,
      code,
      result,
      trace: overrides.trace ?? defaultTrace,
    },
  };
}

function terminalTrace(
  attemptId: string,
  code = '7203',
  overrides: Partial<ShikihoCaptureTraceV1> = {}
): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId,
    code,
    mode: 'new_owned_tab',
    phase: 'complete',
    startedAt: '2026-07-14T00:00:00.000Z',
    updatedAt: '2026-07-14T00:00:01.000Z',
    outcome: 'success',
    waitEndReason: 'field_stable',
    receiverAttempts: 0,
    receiverReadyMs: null,
    documentReadyState: 'complete',
    navigation: { responseStartMs: 1, domInteractiveMs: 2, domContentLoadedMs: 3, loadEndMs: 4 },
    dom: {
      firstSampleMs: 5,
      mutationBatches: 1,
      meaningfulChanges: 1,
      samples: 1,
      presentFields: ['identity'],
      missingFields: [],
      firstSeenMs: {
        identity: 5,
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
        earningsAnnouncementDate: null,
        pageUpdatedAt: null,
        coreReady: null,
      },
    },
    extraction: { samples: 1, lastMs: 1, maxMs: 1, totalMs: 1 },
    timings: { probeMs: 0, acquisitionMs: 0, receiverMs: 0, domObservationMs: 1, storageMs: 0, totalMs: 1 },
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 25; index += 1) await Promise.resolve();
}

function harness(overrides: Partial<ShikihoTabAcquisitionDeps> = {}) {
  let requestNumber = 0;
  let attemptNumber = 0;
  let clock = 0;
  const timings: ShikihoCaptureTiming[] = [];
  const events: string[] = [];
  const acquire = mock(async (code: string) => handle(99, 'new_owned_tab', code));
  const releaseSuccess = mock(async () => undefined);
  const releaseFailure = mock(async () => undefined);
  const queryTabs = mock(overrides.queryTabs ?? (async () => [{ id: 10 }]));
  const sendTabMessage = mock(
    overrides.sendTabMessage ??
      (async (tabId: number, message: ShikihoTabRequest) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        return captureReply(tabId, message, success(message.code));
      })
  );
  const { queryTabs: _queryTabs, sendTabMessage: _sendTabMessage, ...remainingOverrides } = overrides;
  const registerAttempt = mock((attempt: { attemptId: string }) => events.push(`register:${attempt.attemptId}`));
  const registerAcquisition = mock((_attempt: { attemptId: string }) => undefined);
  const updateAcquisition = mock((_attemptId: string, _trace: ShikihoCaptureTraceV1) => undefined);
  const finishAcquisition = mock(async (_attemptId: string, _trace: ShikihoCaptureTraceV1) => undefined);
  const recordReceiverAttempt = mock((_attemptId: string, _elapsedMs: number) => undefined);
  const finishAttempt = mock(async (_attemptId: string, _trace: ShikihoCaptureTraceV1) => undefined);
  const abandonAttempt = mock((_attemptId: string) => undefined);
  const deps: ShikihoTabAcquisitionDeps = {
    now: () => clock++,
    delay: () => new Promise(() => undefined),
    createRequestId: () => `request-${++requestNumber}`,
    createAttemptId: () => `attempt-${++attemptNumber}`,
    getValidWarmTabId: async () => null,
    leaseManager: {
      getValidOwnedTabId: async () => null,
      reconcile: async () => undefined,
      acquire,
      releaseSuccess,
      releaseFailure,
      onAlarm: async () => undefined,
      onActivated: async () => undefined,
      abandonOwnedTab: async () => undefined,
      abandonIfOwned: async () => undefined,
      onUpdatedComplete: async () => undefined,
      onRemoved: async () => undefined,
    },
    logTiming: (timing) => timings.push(timing),
    progress: {
      registerAcquisition,
      updateAcquisition,
      finishAcquisition,
      registerAttempt,
      recordReceiverAttempt,
      finishAttempt,
      abandonAttempt,
    },
    ...remainingOverrides,
    queryTabs,
    sendTabMessage,
  };
  return {
    acquisition: createShikihoTabAcquisition(deps),
    deps,
    acquire,
    releaseSuccess,
    releaseFailure,
    queryTabs,
    sendTabMessage,
    timings,
    events,
    registerAttempt,
    registerAcquisition,
    updateAcquisition,
    finishAcquisition,
    recordReceiverAttempt,
    finishAttempt,
    abandonAttempt,
  };
}

describe('instrumented attempt lifecycle', () => {
  test('the overall deadline rejects while queryTabs is still pending', async () => {
    let clock = 1_000;
    const pendingTabs = deferred<Array<{ id?: number }>>();
    let expireOverall: () => void = () => undefined;
    let overallArmed = false;
    const h = harness({
      now: () => clock,
      queryTabs: () => pendingTabs.promise,
      delay: async (ms) => {
        if (ms !== SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
        overallArmed = true;
        return new Promise<void>((resolve) => {
          expireOverall = resolve;
        });
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    let rejectedBeforeQueryResolved = false;
    if (overallArmed) {
      clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
      expireOverall();
      await expect(capture).rejects.toThrow('timed out');
      rejectedBeforeQueryResolved = true;
    }
    pendingTabs.resolve([]);
    if (!rejectedBeforeQueryResolved) await expect(capture).rejects.toThrow('timed out');

    expect(rejectedBeforeQueryResolved).toBe(true);
    expect(h.acquire).not.toHaveBeenCalled();
    expect(h.finishAcquisition).toHaveBeenCalledTimes(1);
    expect(h.finishAcquisition.mock.calls[0]?.[1]).toMatchObject({
      mode: 'acquisition_unbound',
      phase: 'probing_tabs',
      outcome: 'timeout',
      waitEndReason: 'deadline',
      timings: { probeMs: SHIKIHO_CAPTURE_TIMEOUT_MS },
    });
  });

  test('the overall deadline rejects while warm-tab lookup is still pending', async () => {
    let clock = 1_000;
    const pendingWarmTab = deferred<number | null>();
    let expireOverall: () => void = () => undefined;
    let overallArmed = false;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      getValidWarmTabId: () => pendingWarmTab.promise,
      delay: async (ms) => {
        if (ms !== SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
        overallArmed = true;
        return new Promise<void>((resolve) => {
          expireOverall = resolve;
        });
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    let rejectedBeforeLookupResolved = false;
    if (overallArmed) {
      clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
      expireOverall();
      await expect(capture).rejects.toThrow('timed out');
      rejectedBeforeLookupResolved = true;
    }
    pendingWarmTab.resolve(null);
    if (!rejectedBeforeLookupResolved) await expect(capture).rejects.toThrow('timed out');

    expect(rejectedBeforeLookupResolved).toBe(true);
    expect(h.acquire).not.toHaveBeenCalled();
    expect(h.finishAcquisition.mock.calls[0]?.[1]).toMatchObject({
      mode: 'acquisition_unbound',
      phase: 'probing_tabs',
      outcome: 'timeout',
    });
  });

  test('a lease resolving after the overall timeout is reclaimed without starting capture', async () => {
    let clock = 1_000;
    const pendingHandle = deferred<WarmTabHandle>();
    const releaseLateHandle = mock(async () => undefined);
    let expireOverall: () => void = () => undefined;
    let overallArmed = false;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: () => pendingHandle.promise,
        releaseFailure: releaseLateHandle,
      },
      delay: async (ms) => {
        if (ms !== SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
        overallArmed = true;
        return new Promise<void>((resolve) => {
          expireOverall = resolve;
        });
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    let rejectedBeforeAcquireResolved = false;
    if (overallArmed) {
      clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
      expireOverall();
      await expect(capture).rejects.toThrow('timed out');
      rejectedBeforeAcquireResolved = true;
    } else {
      clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
    }
    pendingHandle.resolve(handle());
    if (!rejectedBeforeAcquireResolved) await expect(capture).rejects.toThrow('timed out');
    await flushMicrotasks();

    expect(rejectedBeforeAcquireResolved).toBe(true);
    expect(h.registerAttempt).not.toHaveBeenCalled();
    expect(h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now')).toHaveLength(0);
    expect(releaseLateHandle).toHaveBeenCalledTimes(1);
    expect(h.finishAcquisition.mock.calls[0]?.[1]).toMatchObject({
      mode: 'acquisition_unbound',
      phase: 'acquiring_tab',
      outcome: 'timeout',
      timings: { acquisitionMs: SHIKIHO_CAPTURE_TIMEOUT_MS },
    });
  });

  test('caps each exact-tab probe to the remaining overall deadline', async () => {
    let clock = 1_000;
    const delays: number[] = [];
    const h = harness({
      now: () => clock,
      queryTabs: async () => {
        clock += SHIKIHO_CAPTURE_TIMEOUT_MS - 200;
        return [{ id: 41 }];
      },
      sendTabMessage: async () => new Promise(() => undefined),
      delay: async (ms) => {
        delays.push(ms);
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timed out');

    expect(delays).toContain(200);
    expect(delays).not.toContain(SHIKIHO_PROBE_TIMEOUT_MS);
    expect(h.acquire).not.toHaveBeenCalled();
  });

  test('does not start warm lookup or fallback after tab discovery exhausts the deadline', async () => {
    let clock = 1_000;
    const getValidWarmTabId = mock(async () => null);
    const h = harness({
      now: () => clock,
      queryTabs: async () => {
        clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
        return [];
      },
      getValidWarmTabId,
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timed out');

    expect(getValidWarmTabId).not.toHaveBeenCalled();
    expect(h.acquire).not.toHaveBeenCalled();
  });

  test('does not arm a fresh timeout after nonzero probe time for a hanging exact send', async () => {
    let clock = 1_000;
    const timeoutDelays: number[] = [];
    let expireOverall: () => void = () => undefined;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [{ id: 41 }],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') {
          clock += 400;
          return probeReply(tabId, '7203');
        }
        return new Promise(() => undefined);
      },
      delay: async (ms) => {
        if (ms === SHIKIHO_PROBE_TIMEOUT_MS) return new Promise(() => undefined);
        timeoutDelays.push(ms);
        return new Promise<void>((resolve) => {
          expireOverall = resolve;
        });
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    expect(h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now')).toHaveLength(1);
    expireOverall();
    await expect(capture).rejects.toThrow('timed out');
    expect(timeoutDelays).toEqual([SHIKIHO_CAPTURE_TIMEOUT_MS]);
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.abandonAttempt).not.toHaveBeenCalled();
  });

  test('does not acquire or send an owned fallback after an exact attempt exhausts the absolute deadline', async () => {
    let clock = 1_000;
    const acquire = mock(async () => handle());
    const h = harness({
      now: () => clock,
      queryTabs: async () => [{ id: 41 }],
      leaseManager: { ...harness().deps.leaseManager, acquire },
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
        throw new Error('exact send exhausted deadline');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timed out');
    expect(acquire).not.toHaveBeenCalled();
  });

  test('does not register or send after owned tab acquisition consumes the remaining deadline', async () => {
    let clock = 1_000;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: async () => {
          clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
          return handle();
        },
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timed out');
    expect(h.registerAttempt).not.toHaveBeenCalled();
    expect(h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now')).toHaveLength(0);
  });

  test('registers before send and keeps one attempt and absolute deadline across receiver retries', async () => {
    const startMs = 1_000;
    let clock = startMs;
    const captureRequests: Array<Extract<ShikihoTabRequest, { type: 'capture_now' }>> = [];
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureRequests.push(message);
        h.events.push(`send:${message.attemptId}`);
        if (captureRequests.length === 1) {
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return {
          tabId,
          response: {
            type: 'capture_result',
            requestId: message.requestId,
            attemptId: message.attemptId,
            code: message.code,
            result: success(message.code),
            trace: terminalTrace(message.attemptId, message.code, {
              startedAt: new Date(message.startedAtMs).toISOString(),
              receiverAttempts: message.receiverAttempts,
              receiverReadyMs: message.receiverReadyMs,
              timings: {
                ...terminalTrace(message.attemptId, message.code).timings,
                probeMs: message.probeMs,
                acquisitionMs: message.acquisitionMs,
                receiverMs: message.receiverMs,
                totalMs: Math.max(5, message.receiverReadyMs, message.receiverMs),
              },
            }),
          },
        };
      },
      delay: async (ms) => {
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
        clock += ms;
      },
    });

    await h.acquisition.capture('7203');

    expect(h.events.slice(0, 2)).toEqual(['register:attempt-1', 'send:attempt-1']);
    expect(captureRequests.map((request) => request.attemptId)).toEqual(['attempt-1', 'attempt-1']);
    expect(captureRequests.map((request) => request.requestId)).toEqual(['request-1', 'request-2']);
    expect(captureRequests.at(-1)?.deadlineMs).toBe(startMs + SHIKIHO_CAPTURE_TIMEOUT_MS);
    expect(captureRequests.at(-1)?.receiverAttempts).toBe(2);
    expect(captureRequests.at(-1)?.receiverReadyMs).toBe(100);
    expect(h.recordReceiverAttempt).toHaveBeenCalledTimes(2);
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt.mock.calls[0]?.[1]).toMatchObject({ receiverAttempts: 2, receiverReadyMs: 100 });
    expect(h.abandonAttempt).not.toHaveBeenCalled();
  });

  test('finishes the terminal trace before handing owned release to the caller', async () => {
    const order: string[] = [];
    const ownedHandle = handle();
    const releaseSuccess = mock(async () => {
      order.push('release');
    });
    const h = harness({
      queryTabs: async () => [],
      progress: {
        registerAcquisition: () => undefined,
        updateAcquisition: () => undefined,
        finishAcquisition: async () => undefined,
        registerAttempt: () => undefined,
        recordReceiverAttempt: () => undefined,
        finishAttempt: async () => {
          order.push('finish');
        },
        abandonAttempt: () => undefined,
      },
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: async () => ownedHandle,
        releaseSuccess,
      },
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        return captureReply(tabId, message, success(message.code));
      },
    });

    const acquired = await h.acquisition.capture('7203');

    expect(order).toEqual(['finish']);
    await acquired.releaseOwnedTab?.();
    expect(order).toEqual(['finish', 'release']);
    expect(releaseSuccess).toHaveBeenCalledWith(ownedHandle, '7203');
  });
});

describe('exact user-tab acquisition', () => {
  test('captures an exact user tab for an alphanumeric stock code', async () => {
    const h = harness({
      queryTabs: async () => [{ id: 4 }],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, '285A')
          : captureReply(tabId, message, success(message.code)),
    });

    const acquired = await h.acquisition.capture('285A');

    expect(acquired.result).toEqual(success('285A'));
    expect(h.sendTabMessage.mock.calls).toContainEqual([
      4,
      expect.objectContaining({ type: 'capture_now', code: '285A' }),
    ]);
  });

  test('captures the lowest exact tab without waiting or touching warm ownership', async () => {
    const h = harness({ queryTabs: async () => [{ id: 12 }, { id: 4 }, { id: 8 }] });

    const acquired = await h.acquisition.capture('7203');

    expect(acquired.result).toEqual(success());
    expect(acquired.timing.mode).toBe('exact_user_tab');
    const captureCalls = h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now');
    expect(captureCalls).toHaveLength(1);
    expect(captureCalls[0]).toEqual([
      4,
      expect.objectContaining({
        type: 'capture_now',
        requestId: 'request-1',
        attemptId: 'attempt-1',
        code: '7203',
        mode: 'exact_user_tab',
      }),
    ]);
    expect(h.queryTabs.mock.calls).toEqual([[]]);
    expect(h.acquire).not.toHaveBeenCalled();
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).not.toHaveBeenCalled();
  });

  test('returns a validated diagnostic without releasing or closing the user tab', async () => {
    const h = harness({
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, '7203')
          : captureReply(tabId, message, diagnostic(message.code)),
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({
      result: { kind: 'login_required', code: '7203' },
      timing: { mode: 'exact_user_tab', outcome: 'diagnostic' },
    });
    expect(h.acquire).not.toHaveBeenCalled();
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).not.toHaveBeenCalled();
  });

  test('does not retry a receiver-missing error from an exact user tab', async () => {
    let exactCaptureAttempts = 0;
    const h = harness({
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        if (tabId === 10) {
          exactCaptureAttempts += 1;
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return captureReply(tabId, message, success(message.code));
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ timing: { mode: 'new_owned_tab' } });
    expect(exactCaptureAttempts).toBe(1);
    expect(h.acquire).toHaveBeenCalledTimes(1);
    expect(h.abandonAttempt).toHaveBeenCalledTimes(1);
  });
});

describe('fallback validation and races', () => {
  const invalidExactResponses: Array<[string, (tabId: number, request: CaptureRequest) => unknown]> = [
    [
      'tab changed code before capture',
      (tabId, request) => captureReply(tabId, request, success('6758'), { code: '6758' }),
    ],
    ['malformed response', () => ({ tabId: 10, response: { type: 'capture_result' } })],
    [
      'mismatched request ID',
      (tabId, request) => captureReply(tabId, request, success(), { requestId: 'old-request' }),
    ],
    ['wrong response code', (tabId, request) => captureReply(tabId, request, success('6758'), { code: '6758' })],
    ['unselected tab response', (tabId, request) => captureReply(tabId, request, success(), { responseTabId: 11 })],
  ];

  for (const [name, invalid] of invalidExactResponses) {
    test(`${name} falls back to owned capture`, async () => {
      const h = harness({
        sendTabMessage: async (tabId, message) => {
          if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
          if (tabId === 10) return invalid(tabId, message) as ReturnType<typeof captureReply>;
          return captureReply(tabId, message, success(message.code));
        },
      });

      const acquired = await h.acquisition.capture('7203');

      expect(acquired.timing.mode).toBe('new_owned_tab');
      expect(h.acquire).toHaveBeenCalledWith('7203');
      await acquired.releaseOwnedTab?.();
      expect(h.releaseSuccess).toHaveBeenCalledTimes(1);
    });
  }

  test('wrong-code and missing-receiver probes fall back', async () => {
    const h = harness({
      queryTabs: async () => [{ id: 10 }, { id: 11 }],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') {
          if (tabId === 10) return probeReply(tabId, '6758');
          throw new Error('Receiving end does not exist');
        }
        return captureReply(tabId, message, success(message.code));
      },
    });

    expect((await h.acquisition.capture('7203')).timing.mode).toBe('new_owned_tab');
    expect(h.acquire).toHaveBeenCalledTimes(1);
    expect(
      h.sendTabMessage.mock.calls.filter(([tabId, message]) => message.type === 'capture_now' && tabId !== 99)
    ).toHaveLength(0);
  });

  test('a 500ms probe timeout falls back under the single outer capture deadline', async () => {
    const delays: number[] = [];
    const h = harness({
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return new Promise(() => undefined);
        return captureReply(tabId, message, success(message.code));
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms === SHIKIHO_PROBE_TIMEOUT_MS) return;
        return new Promise(() => undefined);
      },
    });

    expect((await h.acquisition.capture('7203')).timing.mode).toBe('new_owned_tab');
    expect(delays).toContain(SHIKIHO_PROBE_TIMEOUT_MS);
    const outerDelays = delays.filter((ms) => ms > SHIKIHO_PROBE_TIMEOUT_MS);
    expect(outerDelays).toHaveLength(1);
    expect(outerDelays[0]).toBeLessThanOrEqual(SHIKIHO_CAPTURE_TIMEOUT_MS);
  });

  test('owned capture waits for readiness and rejects stale A-B-A request IDs', async () => {
    const requests: string[] = [];
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        requests.push(message.requestId);
        const stale = message.code === '6758' ? (requests[0] ?? message.requestId) : message.requestId;
        return captureReply(tabId, message, success(message.code), { requestId: stale });
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
    await expect(h.acquisition.capture('6758')).rejects.toThrow('Invalid Shikiho capture response');
    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
    const ownedMessages = h.sendTabMessage.mock.calls
      .map(([, message]) => message)
      .filter(
        (message): message is Extract<ShikihoTabRequest, { type: 'capture_now' }> => message.type === 'capture_now'
      );
    expect(ownedMessages.map(({ requestId }) => requestId)).toEqual(['request-1', 'request-2', 'request-3']);
    expect(ownedMessages.every(({ mode }) => mode === 'new_owned_tab')).toBe(true);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('prefers an exact valid owned tab and acquires its lease', async () => {
    const ownedHandle = handle(12, 'warm_owned_same_code');
    const h = harness({
      queryTabs: async () => [{ id: 4 }, { id: 12 }],
      getValidWarmTabId: async () => 12,
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: mock(async () => ownedHandle),
        releaseSuccess: mock(async () => undefined),
      },
    });

    const acquired = await h.acquisition.capture('7203');

    expect(acquired.timing.mode).toBe('warm_owned_same_code');
    const captureCall = h.sendTabMessage.mock.calls.find(([, message]) => message.type === 'capture_now');
    expect(captureCall).toBeDefined();
    expect(captureCall?.[0]).toBe(12);
    if (captureCall?.[1]?.type !== 'capture_now') throw new Error('expected capture request');
    expect(captureCall[1].mode).toBe('warm_owned_same_code');
  });
});

describe('owned cleanup, timeout, and timing', () => {
  test('releases an owned tab if timing publication fails before cleanup handoff', async () => {
    const h = harness({
      queryTabs: async () => [],
      logTiming: () => {
        throw new Error('timing publication failed');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timing publication failed');
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('keeps the original owned capture running past seven seconds without reloading', async () => {
    let clock = 0;
    const pending = deferred<TabMessageReply>();
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : pending.promise,
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    clock = 7_000;
    await flushMicrotasks();

    const captureCalls = h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now');
    expect(captureCalls).toHaveLength(1);
    const request = captureCalls[0]?.[1] as Extract<ShikihoTabRequest, { type: 'capture_now' }>;
    pending.resolve(captureReply(99, request, success('7203')));
    await expect(capture).resolves.toMatchObject({ result: { kind: 'success' } });
  });

  test('retries an owned capture while the content-script receiver is not ready', async () => {
    let captureAttempts = 0;
    const delays: number[] = [];
    const ownedHandle = handle();
    const releaseSuccess = mock(async () => undefined);
    const releaseFailure = mock(async () => undefined);
    const h = harness({
      queryTabs: async () => [],
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: async () => ownedHandle,
        releaseSuccess,
        releaseFailure,
      },
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        if (captureAttempts === 1) {
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return captureReply(tabId, message, success(message.code));
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms > SHIKIHO_PROBE_TIMEOUT_MS) return new Promise(() => undefined);
      },
    });

    const acquired = await h.acquisition.capture('7203');
    expect(acquired).toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempts).toBe(2);
    expect(delays.slice(1)).toEqual([100]);
    expect(delays[0]).toBeGreaterThan(SHIKIHO_PROBE_TIMEOUT_MS);
    expect(delays[0]).toBeLessThanOrEqual(SHIKIHO_CAPTURE_TIMEOUT_MS);
    await acquired.releaseOwnedTab?.();
    expect(releaseSuccess).toHaveBeenCalledWith(ownedHandle, '7203');
    expect(releaseFailure).not.toHaveBeenCalled();
  });

  test('uses one outer timeout while repeatedly waiting for the owned content-script receiver', async () => {
    let captureAttempts = 0;
    const delays: number[] = [];
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        if (captureAttempts <= 3) {
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return captureReply(tabId, message, success(message.code));
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms > SHIKIHO_PROBE_TIMEOUT_MS) return new Promise(() => undefined);
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempts).toBe(4);
    expect(delays.slice(1)).toEqual([100, 100, 100]);
    expect(delays[0]).toBeGreaterThan(SHIKIHO_PROBE_TIMEOUT_MS);
    expect(delays[0]).toBeLessThanOrEqual(SHIKIHO_CAPTURE_TIMEOUT_MS);
  });

  test('does not retry a general owned capture error', async () => {
    let captureAttempts = 0;
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        throw new Error('tab was closed');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('tab was closed');
    expect(captureAttempts).toBe(1);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt.mock.calls[0]?.[1]).toMatchObject({ phase: 'error', outcome: 'error' });
    expect(h.abandonAttempt).not.toHaveBeenCalled();
  });

  test('rejects a nonterminal content trace and persists a synthesized terminal error', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        return captureReply(tabId, message, success(message.code), {
          trace: terminalTrace(message.attemptId, message.code, {
            mode: message.mode,
            phase: 'observing_dom',
            outcome: null,
            waitEndReason: null,
          }),
        });
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('Invalid Shikiho capture response');
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt.mock.calls[0]?.[1]).toMatchObject({ phase: 'error', outcome: 'error' });
  });

  test('rejects a terminal trace whose wait reason is incompatible with the extraction result', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        return captureReply(tabId, message, success(message.code), {
          trace: terminalTrace(message.attemptId, message.code, {
            mode: message.mode,
            phase: 'complete',
            outcome: 'success',
            waitEndReason: 'deadline',
          }),
        });
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('Invalid Shikiho capture response');
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt.mock.calls[0]?.[1]).toMatchObject({ phase: 'error', outcome: 'error' });
  });

  test.each([
    null,
    undefined,
  ])('abandons an owned capture response of %p as navigation replacement', async (response) => {
    let captureAttempts = 0;
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        return { tabId, response };
      },
      delay: async (ms) => {
        if (ms > SHIKIHO_PROBE_TIMEOUT_MS) return new Promise(() => undefined);
        throw new Error('owned null response must not be retried');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('navigation changed');
    expect(captureAttempts).toBe(1);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.abandonAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt).not.toHaveBeenCalled();
  });

  test('keeps receiver retries inside the original 25 second capture deadline', async () => {
    let clock = 0;
    let captureAttempts = 0;
    const delays: number[] = [];
    let expireOuterTimeout: () => void = () => {};
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      sendTabMessage: async (_tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(99, null);
        captureAttempts += 1;
        if (captureAttempts === 1) {
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return new Promise(() => undefined);
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms > SHIKIHO_PROBE_TIMEOUT_MS) {
          return new Promise<void>((resolve) => {
            expireOuterTimeout = resolve;
          });
        }
        clock += ms;
      },
    });

    const capture = h.acquisition.capture('7203');
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(captureAttempts).toBe(2);
    expireOuterTimeout();
    await expect(capture).rejects.toThrow('timed out');
    expect(delays).toEqual([SHIKIHO_CAPTURE_TIMEOUT_MS, 100]);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('partial success hands successful owned release to the caller', async () => {
    const ownedHandle = handle();
    const releaseSuccess = mock(async () => undefined);
    const releaseFailure = mock(async () => undefined);
    const h = harness({
      queryTabs: async () => [],
      leaseManager: {
        ...harness().deps.leaseManager,
        acquire: async () => ownedHandle,
        releaseSuccess,
        releaseFailure,
      },
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, null)
          : captureReply(tabId, message, success(message.code, 'partial')),
    });

    const acquired = await h.acquisition.capture('7203');
    expect(acquired).toMatchObject({ timing: { outcome: 'partial' } });
    expect(releaseSuccess).not.toHaveBeenCalled();
    await acquired.releaseOwnedTab?.();
    expect(releaseSuccess).toHaveBeenCalledWith(ownedHandle, '7203');
    expect(releaseFailure).not.toHaveBeenCalled();
  });

  test('diagnostic hands failed owned release to the caller', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, null)
          : captureReply(tabId, message, diagnostic(message.code)),
    });

    const acquired = await h.acquisition.capture('7203');
    expect(acquired).toMatchObject({ timing: { outcome: 'diagnostic' } });
    expect(h.releaseFailure).not.toHaveBeenCalled();
    await acquired.releaseOwnedTab?.();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.releaseSuccess).not.toHaveBeenCalled();
  });

  test('owned capture timeout releases failure and logs timeout once', async () => {
    let expireOverall: () => void = () => undefined;
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (_tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(99, null) : new Promise(() => undefined),
      delay: async (ms) => {
        if (ms > SHIKIHO_PROBE_TIMEOUT_MS) {
          return new Promise<void>((resolve) => {
            expireOverall = resolve;
          });
        }
        return new Promise(() => undefined);
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    expireOverall();
    await expect(capture).rejects.toThrow('timed out');
    await flushMicrotasks();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.timings).toHaveLength(1);
    expect(h.timings[0]?.outcome).toBe('timeout');
    expect(h.finishAttempt).toHaveBeenCalledTimes(1);
    expect(h.finishAttempt.mock.calls[0]?.[1]).toMatchObject({ phase: 'timeout', outcome: 'timeout' });
    expect(h.abandonAttempt).not.toHaveBeenCalled();
  });

  test('all four modes emit only nonnegative timing metadata', async () => {
    const modes: WarmTabHandle['mode'][] = ['warm_owned_same_code', 'warm_owned_navigation', 'new_owned_tab'];
    const timings: ShikihoCaptureTiming[] = [];
    const exact = harness({ logTiming: (timing) => timings.push(timing) });
    await exact.acquisition.capture('7203');
    for (const mode of modes) {
      const owned = handle(99, mode);
      const h = harness({
        queryTabs: async () => [],
        leaseManager: {
          ...harness().deps.leaseManager,
          acquire: async () => owned,
          releaseSuccess: async () => undefined,
        },
        logTiming: (timing) => timings.push(timing),
      });
      await h.acquisition.capture('7203');
    }

    expect(timings.map(({ mode }) => mode)).toEqual([
      'exact_user_tab',
      'warm_owned_same_code',
      'warm_owned_navigation',
      'new_owned_tab',
    ]);
    for (const timing of timings) {
      expect(Object.keys(timing).sort()).toEqual(
        ['event', 'mode', 'outcome', 'probeMs', 'navigationMs', 'captureMs', 'totalMs'].sort()
      );
      expect(timing.event).toBe('shikiho_capture_timing');
      expect(timing.probeMs).toBeGreaterThanOrEqual(0);
      expect(timing.navigationMs).toBeGreaterThanOrEqual(0);
      expect(timing.captureMs).toBeGreaterThanOrEqual(0);
      expect(timing.totalMs).toBeGreaterThanOrEqual(0);
    }
    const serialized = JSON.stringify(timings);
    for (const secret of ['7203', 'sourceUrl', 'article secret', 'currentPrice', 'snapshot', 'quote']) {
      expect(serialized).not.toContain(secret);
    }
  });
});
