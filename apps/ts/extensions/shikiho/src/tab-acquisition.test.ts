import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoSnapshotV1 } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { ShikihoTabRequest } from './shikiho-tab-bridge';
import {
  createShikihoTabAcquisition,
  SHIKIHO_CAPTURE_TIMEOUT_MS,
  SHIKIHO_PROBE_TIMEOUT_MS,
  type ShikihoCaptureTiming,
  type ShikihoTabAcquisitionDeps,
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

function harness(overrides: Partial<ShikihoTabAcquisitionDeps> = {}) {
  let requestNumber = 0;
  let clock = 0;
  const timings: ShikihoCaptureTiming[] = [];
  const acquire = mock(async (code: string) => handle(99, 'new_owned_tab', code));
  const releaseSuccess = mock(async () => undefined);
  const releaseFailure = mock(async () => undefined);
  const queryTabs = mock(overrides.queryTabs ?? (async () => [{ id: 10 }]));
  const sendTabMessage = mock(
    overrides.sendTabMessage ??
      (async (tabId: number, message: ShikihoTabRequest) => {
        if (message.type === 'probe_shikiho_code') return probeReply(tabId, '7203');
        return captureReply(tabId, message.requestId, message.code, success(message.code));
      })
  );
  const { queryTabs: _queryTabs, sendTabMessage: _sendTabMessage, ...remainingOverrides } = overrides;
  const deps: ShikihoTabAcquisitionDeps = {
    now: () => clock++,
    delay: () => new Promise(() => undefined),
    createRequestId: () => `request-${++requestNumber}`,
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
      onRemoved: async () => undefined,
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
      delay: async (ms) => {
        delays.push(ms);
        if (ms === SHIKIHO_PROBE_TIMEOUT_MS) return;
        return new Promise(() => undefined);
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
    expect((captureCall?.[1] as { waitForReady?: boolean }).waitForReady).toBe(true);
  });
});

describe('owned cleanup, timeout, and timing', () => {
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
      delay: async (ms) => {
        if (ms === SHIKIHO_CAPTURE_TIMEOUT_MS) return;
        return new Promise(() => undefined);
      },
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
