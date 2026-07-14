import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoSnapshotV1 } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { ShikihoTabRequest } from './shikiho-tab-bridge';
import {
  createShikihoTabAcquisition,
  SHIKIHO_CAPTURE_TIMEOUT_MS,
  SHIKIHO_PROBE_TIMEOUT_MS,
  SHIKIHO_RELOAD_AFTER_MS,
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

function captureReply(tabId: number, requestId: string, code: string, result: ShikihoExtractionResult) {
  return { tabId, response: { type: 'capture_result', requestId, code, result } };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 25; index += 1) await Promise.resolve();
}

function controlledDelays() {
  const pending = new Map<number, Array<() => void>>();
  return {
    delay: (ms: number) =>
      new Promise<void>((resolve) => {
        pending.set(ms, [...(pending.get(ms) ?? []), resolve]);
      }),
    resolve(ms: number) {
      const queue = pending.get(ms) ?? [];
      const next = queue.shift();
      if (next === undefined) throw new Error(`No pending ${ms}ms delay`);
      pending.set(ms, queue);
      next();
    },
  };
}

function controlledTimers() {
  const pending = new Map<number, Array<{ resolve: () => void }>>();
  const created: Array<{ ms: number; cancel: ReturnType<typeof mock> }> = [];
  return {
    createTimer(ms: number) {
      const timer = deferred<void>();
      const cancel = mock(() => timer.resolve());
      pending.set(ms, [...(pending.get(ms) ?? []), { resolve: () => timer.resolve() }]);
      created.push({ ms, cancel });
      return { promise: timer.promise, cancel };
    },
    resolve(ms: number) {
      const queue = pending.get(ms) ?? [];
      const next = queue.shift();
      if (next === undefined) throw new Error(`No pending ${ms}ms timer`);
      pending.set(ms, queue);
      next.resolve();
    },
    created,
  };
}

function ownedCaptureRequests(sendTabMessage: {
  mock: { calls: Array<[number, ShikihoTabRequest]> };
}): Array<Extract<ShikihoTabRequest, { type: 'capture_now' }>> {
  return sendTabMessage.mock.calls.flatMap(([, message]) => (message.type === 'capture_now' ? [message] : []));
}

function harness(overrides: Partial<ShikihoTabAcquisitionDeps> = {}) {
  let requestNumber = 0;
  let clock = 0;
  const timings: ShikihoCaptureTiming[] = [];
  const acquire = mock(overrides.leaseManager?.acquire ?? (async (code: string) => handle(99, 'new_owned_tab', code)));
  const reloadOwned = mock(overrides.leaseManager?.reloadOwned ?? (async () => undefined));
  const releaseSuccess = mock(overrides.leaseManager?.releaseSuccess ?? (async () => undefined));
  const releaseFailure = mock(overrides.leaseManager?.releaseFailure ?? (async () => undefined));
  const createTimer = mock(
    overrides.createTimer ??
      ((_ms: number) => ({ promise: new Promise<void>(() => undefined), cancel: mock(() => undefined) }))
  );
  const queryTabs = mock(overrides.queryTabs ?? (async () => [{ id: 10 }]));
  const sendTabMessage = mock(
    overrides.sendTabMessage ??
      (async (tabId: number, message: ShikihoTabRequest) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      })
  );
  const {
    queryTabs: _queryTabs,
    sendTabMessage: _sendTabMessage,
    leaseManager: leaseManagerOverrides,
    ...remainingOverrides
  } = overrides;
  const deps: ShikihoTabAcquisitionDeps = {
    now: () => clock++,
    delay: () => new Promise(() => undefined),
    createTimer,
    createRequestId: () => `request-${++requestNumber}`,
    getValidWarmTabId: async () => null,
    leaseManager: {
      getValidOwnedTabId: async () => null,
      reconcile: async () => undefined,
      onAlarm: async () => undefined,
      onActivated: async () => undefined,
      abandonOwnedTab: async () => undefined,
      abandonIfOwned: async () => undefined,
      onUpdatedComplete: async () => undefined,
      onRemoved: async () => undefined,
      ...leaseManagerOverrides,
      acquire,
      reloadOwned,
      releaseSuccess,
      releaseFailure,
    },
    logTiming: (timing) => timings.push(timing),
    ...remainingOverrides,
    queryTabs,
    sendTabMessage,
  };
  return {
    acquisition: createShikihoTabAcquisition(deps),
    deps,
    acquire,
    reloadOwned,
    createTimer,
    releaseSuccess,
    releaseFailure,
    queryTabs,
    sendTabMessage,
    timings,
  };
}

describe('exact user-tab acquisition', () => {
  test('captures the lowest exact tab without waiting or touching warm ownership', async () => {
    const h = harness({ queryTabs: async () => [{ id: 12 }, { id: 4 }, { id: 8 }] });

    const acquired = await h.acquisition.capture('7203');

    expect(acquired.result).toEqual(success());
    expect(acquired.timing.mode).toBe('exact_user_tab');
    const captureCalls = h.sendTabMessage.mock.calls.filter(([, message]) => message.type === 'capture_now');
    expect(captureCalls).toEqual([
      [4, { type: 'capture_now', requestId: 'request-1', code: '7203', waitForReady: false }],
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
          : captureReply(tabId, message.requestId, message.code, diagnostic(message.code)),
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({
      result: { kind: 'login_required', code: '7203' },
      timing: { mode: 'exact_user_tab', outcome: 'diagnostic' },
    });
    expect(h.acquire).not.toHaveBeenCalled();
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
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ timing: { mode: 'new_owned_tab' } });
    expect(exactCaptureAttempts).toBe(1);
    expect(h.acquire).toHaveBeenCalledTimes(1);
  });
});

describe('fallback validation and races', () => {
  const invalidExactResponses: Array<[string, (tabId: number, requestId: string) => unknown]> = [
    ['tab changed code before capture', (tabId, requestId) => captureReply(tabId, requestId, '6758', success('6758'))],
    ['malformed response', () => ({ tabId: 10, response: { type: 'capture_result' } })],
    ['mismatched request ID', (tabId) => captureReply(tabId, 'old-request', '7203', success())],
    ['wrong response code', (tabId, requestId) => captureReply(tabId, requestId, '6758', success('6758'))],
    ['unselected tab response', (_tabId, requestId) => captureReply(11, requestId, '7203', success())],
  ];

  for (const [name, invalid] of invalidExactResponses) {
    test(`${name} falls back to owned capture`, async () => {
      const h = harness({
        sendTabMessage: async (tabId, message) => {
          if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
          if (tabId === 10) return invalid(tabId, message.requestId) as ReturnType<typeof captureReply>;
          return captureReply(tabId, message.requestId, message.code, success(message.code));
        },
      });

      const acquired = await h.acquisition.capture('7203');

      expect(acquired.timing.mode).toBe('new_owned_tab');
      expect(h.acquire).toHaveBeenCalledWith('7203');
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
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
    });

    expect((await h.acquisition.capture('7203')).timing.mode).toBe('new_owned_tab');
    expect(h.acquire).toHaveBeenCalledTimes(1);
    expect(
      h.sendTabMessage.mock.calls.filter(([tabId, message]) => message.type === 'capture_now' && tabId !== 99)
    ).toHaveLength(0);
  });

  test('a 500ms probe timeout falls back and uses the 25s outer capture timeout', async () => {
    const delays: number[] = [];
    const h = harness({
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return new Promise(() => undefined);
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
      createTimer: (ms) => {
        delays.push(ms);
        return {
          promise: ms === SHIKIHO_PROBE_TIMEOUT_MS ? Promise.resolve() : new Promise<void>(() => undefined),
          cancel: () => undefined,
        };
      },
    });

    expect((await h.acquisition.capture('7203')).timing.mode).toBe('new_owned_tab');
    expect(delays).toContain(SHIKIHO_PROBE_TIMEOUT_MS);
    expect(delays).toContain(SHIKIHO_CAPTURE_TIMEOUT_MS);
  });

  test('owned capture waits for readiness and rejects stale A-B-A request IDs', async () => {
    const requests: string[] = [];
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        requests.push(message.requestId);
        const stale = message.code === '6758' ? (requests[0] ?? message.requestId) : message.requestId;
        return captureReply(tabId, stale, message.code, success(message.code));
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
    expect(ownedMessages.every(({ waitForReady }) => waitForReady)).toBe(true);
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
    expect(captureCall?.[0]).toBe(12);
    expect(captureCall?.[1]).toMatchObject({ waitForReady: true });
  });
});

describe('owned reload recovery', () => {
  test('reloads once at seven seconds and accepts only the fresh second request', async () => {
    const first = deferred<TabMessageReply>();
    const second = deferred<TabMessageReply>();
    const timers = controlledTimers();
    let captureAttempt = 0;
    const h = harness({
      now: () => 100,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempt += 1;
        return captureAttempt === 1 ? first.promise : second.promise;
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await flushMicrotasks();

    expect(h.reloadOwned).toHaveBeenCalledTimes(1);
    expect(h.reloadOwned.mock.calls[0]?.[1]).toBe(100 + SHIKIHO_CAPTURE_TIMEOUT_MS);
    const requestIds = ownedCaptureRequests(h.sendTabMessage).map((request) => request.requestId);
    expect(requestIds).toHaveLength(2);
    expect(requestIds[0]).not.toBe(requestIds[1]);

    first.resolve(captureReply(99, requestIds[0] as string, '7203', success('7203')));
    await flushMicrotasks();
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    second.resolve(captureReply(99, requestIds[1] as string, '7203', success('7203')));
    await expect(capture).resolves.toMatchObject({ result: { kind: 'success' } });
  });

  const immediateResults: Array<[string, ShikihoExtractionResult]> = [
    ['success', success()],
    ['partial', success('7203', 'partial')],
    ['diagnostic', diagnostic()],
  ];

  for (const [name, result] of immediateResults) {
    test(`${name} before seven seconds does not reload`, async () => {
      const h = harness({
        queryTabs: async () => [],
        sendTabMessage: async (tabId, message) =>
          message.type === 'probe_shikiho_code'
            ? probeReply(tabId, null)
            : captureReply(tabId, message.requestId, message.code, result),
      });

      await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result });
      expect(h.reloadOwned).not.toHaveBeenCalled();
    });
  }

  test('general error before seven seconds does not reload', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (_tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(99, null);
        throw new Error('tab was closed');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('tab was closed');
    expect(h.reloadOwned).not.toHaveBeenCalled();
  });

  test('malformed response before seven seconds does not reload', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : { tabId, response: null },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('Invalid Shikiho capture response');
    expect(h.reloadOwned).not.toHaveBeenCalled();
  });

  for (const [name, error] of [
    ['reload rejection', new Error('reload failed')],
    ['ownership loss', new Error('Shikiho warm tab ownership changed')],
  ] as const) {
    test(`${name} is terminal and releases failure once`, async () => {
      const first = deferred<TabMessageReply>();
      const timers = controlledTimers();
      const h = harness({
        queryTabs: async () => [],
        createTimer: timers.createTimer,
        sendTabMessage: async (tabId, message) =>
          message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : first.promise,
        leaseManager: {
          ...harness().deps.leaseManager,
          reloadOwned: async () => {
            throw error;
          },
        },
      });

      const capture = h.acquisition.capture('7203');
      await flushMicrotasks();
      timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
      await expect(capture).rejects.toThrow(error.message);
      expect(h.reloadOwned).toHaveBeenCalledTimes(1);
      expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    });
  }

  test('reload latency consumes the original phase-two budget', async () => {
    const first = deferred<TabMessageReply>();
    const reload = deferred<void>();
    const timers = controlledTimers();
    const h = harness({
      now: () => 0,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : first.promise,
      leaseManager: {
        ...harness().deps.leaseManager,
        reloadOwned: () => reload.promise,
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await flushMicrotasks();
    timers.resolve(SHIKIHO_CAPTURE_TIMEOUT_MS);

    await expect(capture).rejects.toThrow('timed out');
    reload.resolve();
    await flushMicrotasks();
    expect(ownedCaptureRequests(h.sendTabMessage)).toHaveLength(1);
    expect(h.reloadOwned).toHaveBeenCalledTimes(1);
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('phase two retries a missing receiver every 100 milliseconds', async () => {
    const first = deferred<TabMessageReply>();
    const timers = controlledTimers();
    const delays = controlledDelays();
    let captureAttempt = 0;
    const h = harness({
      queryTabs: async () => [],
      delay: delays.delay,
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempt += 1;
        if (captureAttempt === 1) return first.promise;
        if (captureAttempt === 2) throw new Error('Could not establish connection. Receiving end does not exist.');
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await flushMicrotasks();
    expect(captureAttempt).toBe(2);
    delays.resolve(100);

    await expect(capture).resolves.toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempt).toBe(3);
    expect(h.reloadOwned).toHaveBeenCalledTimes(1);
  });

  test('times out at the original 25 seconds after one reload with no third phase', async () => {
    const timers = controlledTimers();
    const h = harness({
      now: () => 0,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : new Promise(() => undefined),
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await flushMicrotasks();
    expect(ownedCaptureRequests(h.sendTabMessage)).toHaveLength(2);
    timers.resolve(SHIKIHO_CAPTURE_TIMEOUT_MS);

    await expect(capture).rejects.toThrow('timed out');
    expect(h.reloadOwned).toHaveBeenCalledTimes(1);
    expect(ownedCaptureRequests(h.sendTabMessage)).toHaveLength(2);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('does not reload when the absolute deadline has passed before the milestone is observed', async () => {
    const first = deferred<TabMessageReply>();
    const timers = controlledTimers();
    let clock = 0;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : first.promise,
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    clock = SHIKIHO_CAPTURE_TIMEOUT_MS;
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);

    await expect(capture).rejects.toThrow('timed out');
    expect(h.reloadOwned).not.toHaveBeenCalled();
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('ignores a first capture result that resolves after the absolute deadline', async () => {
    const first = deferred<TabMessageReply>();
    const timers = controlledTimers();
    let clock = 0;
    const h = harness({
      now: () => clock,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(tabId, null) : first.promise,
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    const request = ownedCaptureRequests(h.sendTabMessage)[0];
    clock = SHIKIHO_CAPTURE_TIMEOUT_MS;
    first.resolve(captureReply(99, request?.requestId as string, '7203', success()));

    await expect(capture).rejects.toThrow('timed out');
    expect(h.reloadOwned).not.toHaveBeenCalled();
    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('ignores a phase-two success that arrives after the outer timeout', async () => {
    const first = deferred<TabMessageReply>();
    const second = deferred<TabMessageReply>();
    const timers = controlledTimers();
    let captureAttempt = 0;
    const h = harness({
      now: () => 0,
      queryTabs: async () => [],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempt += 1;
        return captureAttempt === 1 ? first.promise : second.promise;
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await flushMicrotasks();
    const secondRequest = ownedCaptureRequests(h.sendTabMessage)[1];
    timers.resolve(SHIKIHO_CAPTURE_TIMEOUT_MS);
    await expect(capture).rejects.toThrow('timed out');
    second.resolve(captureReply(99, secondRequest?.requestId as string, '7203', success()));
    await flushMicrotasks();

    expect(h.releaseSuccess).not.toHaveBeenCalled();
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('superseding a first-phase receiver retry prevents another first-phase request', async () => {
    const timers = controlledTimers();
    const delays = controlledDelays();
    let captureAttempt = 0;
    const h = harness({
      queryTabs: async () => [],
      delay: delays.delay,
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempt += 1;
        if (captureAttempt === 1) throw new Error('Could not establish connection. Receiving end does not exist.');
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_RELOAD_AFTER_MS);
    await expect(capture).resolves.toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempt).toBe(2);
    delays.resolve(100);
    await Promise.resolve();
    expect(captureAttempt).toBe(2);
  });

  for (const outcome of ['success', 'error'] as const) {
    test(`fast ${outcome} cancels the milestone and outer timeout timers`, async () => {
      const timers = controlledTimers();
      const h = harness({
        queryTabs: async () => [],
        createTimer: timers.createTimer,
        sendTabMessage: async (tabId, message) => {
          if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
          if (outcome === 'error') throw new Error('terminal capture error');
          return captureReply(tabId, message.requestId, message.code, success(message.code));
        },
      });

      if (outcome === 'success') {
        await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
      } else {
        await expect(h.acquisition.capture('7203')).rejects.toThrow('terminal capture error');
      }
      expect(timers.created.map(({ ms }) => ms)).toEqual([SHIKIHO_RELOAD_AFTER_MS, SHIKIHO_CAPTURE_TIMEOUT_MS]);
      expect(timers.created.every(({ cancel }) => cancel.mock.calls.length === 1)).toBe(true);
    });
  }

  test('an exact user-tab timeout never reloads that tab', async () => {
    const timers = controlledTimers();
    const h = harness({
      queryTabs: async () => [{ id: 10 }],
      createTimer: timers.createTimer,
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        if (tabId === 10) return new Promise(() => undefined);
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    timers.resolve(SHIKIHO_CAPTURE_TIMEOUT_MS);

    await expect(capture).resolves.toMatchObject({ timing: { mode: 'new_owned_tab' } });
    expect(h.reloadOwned).not.toHaveBeenCalled();
  });
});

describe('owned cleanup, timeout, and timing', () => {
  test('retries an owned capture while the content-script receiver is not ready', async () => {
    let captureAttempts = 0;
    const delays: number[] = [];
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        if (captureAttempts === 1) {
          throw new Error('Could not establish connection. Receiving end does not exist.');
        }
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempts).toBe(2);
    expect(delays).toEqual([100]);
    expect(h.releaseSuccess).toHaveBeenCalledTimes(1);
    expect(h.releaseFailure).not.toHaveBeenCalled();
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
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      },
      delay: async (ms) => {
        delays.push(ms);
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
      },
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ result: { kind: 'success' } });
    expect(captureAttempts).toBe(4);
    expect(delays).toEqual([100, 100, 100]);
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
  });

  test.each([null, undefined])('does not retry an owned capture response of %p', async (response) => {
    let captureAttempts = 0;
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, null);
        captureAttempts += 1;
        return { tabId, response };
      },
      delay: async (ms) => {
        if (ms === SHIKIHO_RELOAD_AFTER_MS || ms === SHIKIHO_CAPTURE_TIMEOUT_MS) {
          return new Promise(() => undefined);
        }
        throw new Error('owned null response must not be retried');
      },
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('Invalid Shikiho capture response');
    expect(captureAttempts).toBe(1);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
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
        clock += ms;
      },
      createTimer: (ms) => {
        const timer = deferred<void>();
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) expireOuterTimeout = timer.resolve;
        return { promise: timer.promise, cancel: () => timer.resolve() };
      },
    });

    const capture = h.acquisition.capture('7203');
    await flushMicrotasks();
    expect(captureAttempts).toBe(2);
    expireOuterTimeout();
    await expect(capture).rejects.toThrow('timed out');
    expect(delays).toEqual([100]);
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
  });

  test('partial success releases the owned lease successfully', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, null)
          : captureReply(tabId, message.requestId, message.code, success(message.code, 'partial')),
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ timing: { outcome: 'partial' } });
    expect(h.releaseSuccess).toHaveBeenCalledTimes(1);
    expect(h.releaseFailure).not.toHaveBeenCalled();
  });

  test('diagnostic closes the owned lease through releaseFailure', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (tabId, message) =>
        message.type === 'probe_shikiho_code'
          ? probeReply(tabId, null)
          : captureReply(tabId, message.requestId, message.code, diagnostic(message.code)),
    });

    await expect(h.acquisition.capture('7203')).resolves.toMatchObject({ timing: { outcome: 'diagnostic' } });
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.releaseSuccess).not.toHaveBeenCalled();
  });

  test('owned capture timeout releases failure and logs timeout once', async () => {
    const h = harness({
      queryTabs: async () => [],
      sendTabMessage: async (_tabId, message) =>
        message.type === 'probe_shikiho_code' ? probeReply(99, null) : new Promise(() => undefined),
      createTimer: (ms) => ({
        promise: ms === SHIKIHO_CAPTURE_TIMEOUT_MS ? Promise.resolve() : new Promise<void>(() => undefined),
        cancel: () => undefined,
      }),
    });

    await expect(h.acquisition.capture('7203')).rejects.toThrow('timed out');
    expect(h.releaseFailure).toHaveBeenCalledTimes(1);
    expect(h.timings).toHaveLength(1);
    expect(h.timings[0]?.outcome).toBe('timeout');
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
