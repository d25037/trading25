import { describe, expect, mock, test } from 'bun:test';
import {
  type BackgroundCaptureDeps,
  createBackgroundCaptureCoordinator,
  resolvePublicShikihoState,
} from './background-capture';
import { createCaptureProgressBroker, type ListenerEvent, type ProgressPort } from './capture-progress';
import {
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoCaptureProgressV1,
  type ShikihoCaptureTraceV1,
  type ShikihoSnapshotV1,
} from './contract';
import {
  isAllowedTrading25Origin,
  type LocalhostBridgeOptions,
  SHIKIHO_CAPTURE_PROGRESS_PORT_NAME,
  startLocalhostBridge,
} from './localhost-content';
import {
  createShikihoRepository,
  SHIKIHO_DIAGNOSTICS_STORAGE_KEY,
  SHIKIHO_SNAPSHOTS_STORAGE_KEY,
  type StorageArea,
} from './storage';
import { createShikihoTabAcquisition, SHIKIHO_CAPTURE_TIMEOUT_MS } from './tab-acquisition';
import type { WarmTabLeaseManager } from './warm-tab-lease';

type WindowListener = (event: MessageEvent) => void;
type StorageListener = (changes: Record<string, { oldValue?: unknown; newValue?: unknown }>, areaName: string) => void;

function diagnostic(observedAt: string, status: ShikihoCaptureDiagnosticV1['status']): ShikihoCaptureDiagnosticV1 {
  return { schemaVersion: 1, code: '7203', observedAt, status };
}

function snapshot(): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: 'test',
    code: '7203',
    companyName: 'Toyota',
    sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
    capturedAt: '2026-07-12T12:00:00.000Z',
    pageUpdatedAt: null,
    editionLabel: null,
    earningsAnnouncementDate: null,
    contentHash: 'sha256:test',
    status: 'captured',
    features: null,
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
    quote: {
      tradingDate: '2026-07-12',
      observedAt: '2026-07-12T12:00:00.000Z',
      delayMinutes: 15,
      currentPrice: 102,
      open: 100,
      high: 105,
      low: 98,
      previousClose: 99,
      volume: 12_300,
      openTime: '09:00',
      highTime: '13:20',
      lowTime: null,
      sourceLabel: '会社四季報オンライン',
    },
    missingFields: [],
  };
}

function trace(attemptId = 'attempt-1', code = '7203'): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId,
    code,
    mode: 'new_owned_tab',
    phase: 'observing_dom',
    startedAt: '2026-07-14T00:00:00.000Z',
    updatedAt: '2026-07-14T00:00:01.000Z',
    outcome: null,
    waitEndReason: null,
    receiverAttempts: 1,
    receiverReadyMs: 100,
    documentReadyState: 'interactive',
    navigation: {
      responseStartMs: 10,
      domInteractiveMs: 500,
      domContentLoadedMs: null,
      loadEndMs: null,
    },
    dom: {
      firstSampleMs: 150,
      mutationBatches: 1,
      meaningfulChanges: 1,
      samples: 1,
      presentFields: ['identity'],
      missingFields: ['features'],
      firstSeenMs: {
        identity: 150,
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
    extraction: { samples: 1, lastMs: 2, maxMs: 2, totalMs: 2 },
    timings: {
      probeMs: 10,
      acquisitionMs: 20,
      receiverMs: 100,
      domObservationMs: 870,
      storageMs: 0,
      totalMs: 1_000,
    },
  };
}

function progress(
  code = '7203',
  attemptId = 'attempt-1',
  sequence = 1
): { type: 'capture_progress'; progress: ShikihoCaptureProgressV1 } {
  return {
    type: 'capture_progress',
    progress: {
      schemaVersion: 1,
      code,
      attemptId,
      sequence,
      candidate: null,
      trace: trace(attemptId, code),
    },
  };
}

function listenerEvent<TListener extends (...args: never[]) => void>() {
  const listeners = new Set<TListener>();
  const event: ListenerEvent<TListener> = {
    addListener(listener) {
      listeners.add(listener);
    },
    removeListener(listener) {
      listeners.delete(listener);
    },
  };
  return { event, listeners };
}

function progressPortHarness() {
  type MessageListener = (message: unknown) => void;
  type DisconnectListener = () => void;
  const messages = listenerEvent<MessageListener>();
  const disconnects = listenerEvent<DisconnectListener>();
  const posted: unknown[] = [];
  let disconnected = false;
  const port: ProgressPort & { disconnect(): void } = {
    postMessage(message) {
      posted.push(structuredClone(message));
    },
    onMessage: messages.event,
    onDisconnect: disconnects.event,
    disconnect() {
      disconnected = true;
      for (const listener of [...disconnects.listeners]) listener();
    },
  };
  return {
    port,
    posted,
    emit(message: unknown) {
      for (const listener of [...messages.listeners]) listener(message);
    },
    disconnectFromRuntime() {
      for (const listener of [...disconnects.listeners]) listener();
    },
    isDisconnected: () => disconnected,
    listenerCounts: () => ({ message: messages.listeners.size, disconnect: disconnects.listeners.size }),
  };
}

function memoryStorage(): StorageArea {
  const values: Record<string, unknown> = {};
  return {
    async get(keys) {
      if (keys === null) return structuredClone(values);
      const selected = Array.isArray(keys) ? keys : [keys];
      return Object.fromEntries(
        selected.filter((key) => key in values).map((key) => [key, structuredClone(values[key])])
      );
    },
    async set(items) {
      Object.assign(values, structuredClone(items));
    },
  };
}

function deferred<T>() {
  let resolve: (value: T) => void = () => undefined;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function createHarness(
  sendMessage: LocalhostBridgeOptions['sendMessage'] = async () => ({ snapshot: null, diagnostic: null, trace: null })
) {
  const currentWindow = {};
  let windowListener: WindowListener | null = null;
  let storageListener: StorageListener | null = null;
  const removeWindowListener = mock((listener: WindowListener) => {
    if (windowListener === listener) windowListener = null;
  });
  const removeStorageListener = mock((listener: StorageListener) => {
    if (storageListener === listener) storageListener = null;
  });
  const posted: unknown[] = [];
  const progressPort = progressPortHarness();
  const connectProgressPort = mock(() => progressPort.port);
  const options: LocalhostBridgeOptions = {
    url: new URL('http://localhost:5173/symbol-workbench'),
    currentWindow,
    addWindowListener: (listener) => {
      windowListener = listener;
    },
    removeWindowListener,
    addStorageListener: (listener) => {
      storageListener = listener;
    },
    removeStorageListener,
    connectProgressPort,
    sendMessage,
    postMessage: (message) => posted.push(message),
  };
  return {
    currentWindow,
    options,
    posted,
    progressPort,
    connectProgressPort,
    removeWindowListener,
    removeStorageListener,
    emitWindow(data: unknown, source: unknown = currentWindow, origin = options.url.origin) {
      windowListener?.({ data, source, origin } as MessageEvent);
    },
    emitStorage(changes: Parameters<StorageListener>[0], areaName = 'local') {
      storageListener?.(changes, areaName);
    },
  };
}

function request(type: 'ping' | 'get_snapshot', code = '7203', forceRefresh = false) {
  return {
    channel: SHIKIHO_BRIDGE_CHANNEL,
    direction: 'page-to-extension',
    type,
    requestId: 'request-1',
    ...(type === 'get_snapshot' ? { code, forceRefresh } : {}),
  };
}

describe('localhost content bridge', () => {
  test('uses the exact Chrome progress Port name', () => {
    expect(SHIKIHO_CAPTURE_PROGRESS_PORT_NAME).toBe('shikiho-capture-progress-v1');
  });

  test('forwards an alphanumeric stock-code request to the background', async () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot', '285A'));
    await flushPromises();

    expect(sendMessage).toHaveBeenCalledWith({ type: 'resolve_snapshot', code: '285A', forceRefresh: false });
    expect(harness.progressPort.posted).toEqual([{ type: 'subscribe_capture_progress', code: '285A' }]);
    stop();
  });

  test('subscribes for each valid current page request and forwards progress with its request ID', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });
    expect(harness.connectProgressPort).toHaveBeenCalledTimes(1);
    expect(harness.progressPort.posted).toEqual([{ type: 'subscribe_capture_progress', code: '7203' }]);

    harness.progressPort.emit(progress());
    expect(harness.posted.at(-1)).toEqual({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'extension-to-page',
      type: 'capture_progress',
      requestId: 'page-1',
      code: '7203',
      attemptId: 'attempt-1',
      sequence: 1,
      candidate: null,
      trace: trace(),
    });
    stop();
  });

  test('resubscribes and rebinds progress only when the page request is replaced', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });
    harness.progressPort.emit(progress('7203', 'attempt-old', 1));
    harness.progressPort.emit(progress('7203', 'attempt-new', 1));
    expect(harness.posted.filter((message) => (message as { type?: string }).type === 'capture_progress')).toHaveLength(
      2
    );

    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-2' });
    expect(harness.progressPort.posted.at(-1)).toEqual({ type: 'subscribe_capture_progress', code: '7203' });
    harness.progressPort.emit(progress('7203', 'attempt-new', 1));
    expect(harness.posted.at(-1)).toMatchObject({
      type: 'capture_progress',
      requestId: 'page-2',
      code: '7203',
      attemptId: 'attempt-new',
      sequence: 1,
    });

    harness.emitWindow({ ...request('get_snapshot', '6758'), requestId: 'page-3' });
    expect(harness.progressPort.posted.at(-1)).toEqual({ type: 'subscribe_capture_progress', code: '6758' });
    harness.progressPort.emit(progress('7203', 'attempt-old', 2));
    harness.progressPort.emit(progress('6758', 'attempt-latest', 1));

    expect(harness.posted.at(-1)).toMatchObject({
      type: 'capture_progress',
      requestId: 'page-3',
      code: '6758',
      attemptId: 'attempt-latest',
      sequence: 1,
    });
    stop();
  });

  test('accepts a new trusted attempt for the current request and resets its sequence', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });

    harness.progressPort.emit(progress('7203', 'exact-tab-attempt', 9));
    harness.progressPort.emit(progress('7203', 'owned-tab-attempt', 1));

    const forwarded = harness.posted.filter((message) => (message as { type?: string }).type === 'capture_progress');
    expect(forwarded).toHaveLength(2);
    expect(forwarded[1]).toMatchObject({ attemptId: 'owned-tab-attempt', sequence: 1 });
    stop();
  });

  test('rejects a return to a retired attempt after one-way attempt replacement', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });

    harness.progressPort.emit(progress('7203', 'exact-tab-attempt', 1));
    harness.progressPort.emit(progress('7203', 'owned-tab-attempt', 1));
    harness.progressPort.emit(progress('7203', 'exact-tab-attempt', 2));

    const forwarded = harness.posted.filter((message) => (message as { type?: string }).type === 'capture_progress');
    expect(forwarded).toHaveLength(2);
    expect(forwarded.map((message) => (message as { attemptId?: string }).attemptId)).toEqual([
      'exact-tab-attempt',
      'owned-tab-attempt',
    ]);
    stop();
  });

  test('rejects stale sequences, wrong codes, and malformed Port messages', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });

    harness.progressPort.emit({ ...progress(), extra: true });
    harness.progressPort.emit({ type: 'capture_progress', progress: { ...progress().progress, extra: true } });
    harness.progressPort.emit(progress('6758'));
    harness.progressPort.emit(progress('7203', 'attempt-1', 2));
    harness.progressPort.emit(progress('7203', 'attempt-1', 1));

    const forwarded = harness.posted.filter((message) => (message as { type?: string }).type === 'capture_progress');
    expect(forwarded).toHaveLength(1);
    expect(forwarded[0]).toMatchObject({ attemptId: 'attempt-1', sequence: 2 });
    stop();
  });

  test('does not subscribe for invalid source/origin requests', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'), {});
    harness.emitWindow(request('get_snapshot'), harness.currentWindow, 'http://localhost:4173');
    harness.emitWindow({ ...request('get_snapshot'), extra: true });
    expect(harness.progressPort.posted).toEqual([]);
    stop();
  });

  test('cleans Port listeners on disconnect and reconnects for the next request', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });
    expect(harness.progressPort.listenerCounts()).toEqual({ message: 1, disconnect: 1 });

    harness.progressPort.disconnectFromRuntime();
    expect(harness.progressPort.listenerCounts()).toEqual({ message: 0, disconnect: 0 });
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-2' });
    expect(harness.connectProgressPort).toHaveBeenCalledTimes(2);
    stop();
  });

  test('disconnects and prevents progress after bridge stop', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });
    const beforeStop = harness.posted.length;

    stop();
    harness.progressPort.emit(progress());

    expect(harness.progressPort.isDisconnected()).toBe(true);
    expect(harness.progressPort.listenerCounts()).toEqual({ message: 0, disconnect: 0 });
    expect(harness.posted).toHaveLength(beforeStop);
  });

  test('stops forwarding progress after the current request receives a terminal response', async () => {
    const terminal = deferred<unknown>();
    const harness = createHarness(() => terminal.promise);
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow({ ...request('get_snapshot'), requestId: 'page-1' });
    harness.progressPort.emit(progress('7203', 'attempt-1', 1));

    terminal.resolve({ snapshot: null, diagnostic: null, trace: null });
    await flushPromises();
    harness.progressPort.emit(progress('7203', 'attempt-1', 2));

    expect(harness.posted.map((message) => (message as { type?: string }).type)).toEqual([
      'capture_progress',
      'snapshot',
    ]);
    stop();
  });

  test('publishes the exact public response shape from a real repository-backed background resolve', async () => {
    const repository = createShikihoRepository(memoryStorage());
    await repository.saveSnapshot(snapshot());
    const deps: BackgroundCaptureDeps = {
      now: () => Date.parse('2026-07-12T12:00:01.000Z'),
      get: (code) => repository.get(code),
      getTrace: (code) => repository.getTrace(code),
      saveSnapshot: (value) => repository.saveSnapshot(value),
      saveDiagnostic: (value) => repository.saveDiagnostic(value),
      saveTrace: (value) => repository.saveTrace(value),
      capture: async () => {
        throw new Error('fresh repository state must not capture');
      },
    };
    const coordinator = createBackgroundCaptureCoordinator(deps);
    const runtimeResponses: unknown[] = [];
    const sendMessage = mock(async (message: { code: string; forceRefresh: boolean }) => {
      const response = await resolvePublicShikihoState(coordinator.resolve, message.code, message.forceRefresh);
      runtimeResponses.push(response);
      return response;
    });
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'));
    for (let index = 0; index < 10; index += 1) await flushPromises();

    expect(sendMessage).toHaveReturned();
    expect(Object.keys(runtimeResponses[0] as Record<string, unknown>).sort()).toEqual([
      'diagnostic',
      'snapshot',
      'trace',
    ]);
    expect(harness.posted).toHaveLength(1);
    expect(harness.posted[0]).toMatchObject({ type: 'snapshot', code: '7203', snapshot: snapshot() });
    stop();
  });

  test('publishes a repository-backed terminal trace after background timeout with no snapshot', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const baseTrace = trace('attempt-timeout');
    const terminalTrace = {
      ...baseTrace,
      mode: 'acquisition_unbound' as const,
      phase: 'probing_tabs' as const,
      outcome: 'timeout' as const,
      waitEndReason: 'deadline' as const,
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
        missingFields: [],
        firstSeenMs: Object.fromEntries(Object.keys(baseTrace.dom.firstSeenMs).map((key) => [key, null])) as never,
      },
      extraction: { samples: 0, lastMs: null, maxMs: null, totalMs: 0 },
      timings: { probeMs: 25, acquisitionMs: 0, receiverMs: 0, domObservationMs: 0, storageMs: 0, totalMs: 25 },
    };
    const deps: BackgroundCaptureDeps = {
      now: () => Date.parse('2026-07-14T00:00:25.000Z'),
      get: (code) => repository.get(code),
      getTrace: (code) => repository.getTrace(code),
      saveSnapshot: (value) => repository.saveSnapshot(value),
      saveDiagnostic: (value) => repository.saveDiagnostic(value),
      saveTrace: (value) => repository.saveTrace(value),
      capture: async () => {
        await repository.saveTrace(terminalTrace);
        throw new Error('capture timed out');
      },
    };
    const coordinator = createBackgroundCaptureCoordinator(deps);
    const harness = createHarness((message) =>
      resolvePublicShikihoState(coordinator.resolve, message.code, message.forceRefresh)
    );
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot', '7203', true));
    for (let index = 0; index < 10; index += 1) await flushPromises();

    expect(harness.posted).toHaveLength(1);
    expect(harness.posted[0]).toMatchObject({
      type: 'snapshot',
      code: '7203',
      snapshot: null,
      diagnostic: null,
      trace: terminalTrace,
    });
    stop();
  });

  test('publishes an actual query-timeout trace through acquisition, broker, repository, and coordinator', async () => {
    let clock = 1_000;
    let expire: () => void = () => undefined;
    let armed = false;
    const repository = createShikihoRepository(memoryStorage());
    const broker = createCaptureProgressBroker({ saveTrace: (value) => repository.saveTrace(value) });
    const neverTabs = new Promise<Array<{ id?: number }>>(() => undefined);
    const leaseManager: WarmTabLeaseManager = {
      getValidOwnedTabId: async () => null,
      reconcile: async () => undefined,
      acquire: async () => {
        throw new Error('lease must not start');
      },
      releaseSuccess: async () => undefined,
      releaseFailure: async () => undefined,
      onAlarm: async () => undefined,
      onActivated: async () => undefined,
      abandonOwnedTab: async () => undefined,
      abandonIfOwned: async () => undefined,
      onUpdatedComplete: async () => undefined,
      onRemoved: async () => undefined,
    };
    const acquisition = createShikihoTabAcquisition({
      now: () => clock,
      delay: async (ms) => {
        if (ms !== SHIKIHO_CAPTURE_TIMEOUT_MS) return new Promise(() => undefined);
        armed = true;
        return new Promise<void>((resolve) => {
          expire = resolve;
        });
      },
      createRequestId: () => 'request-capture',
      createAttemptId: () => 'acquisition-query-timeout',
      queryTabs: () => neverTabs,
      sendTabMessage: async () => {
        throw new Error('send must not start');
      },
      getValidWarmTabId: async () => null,
      leaseManager,
      progress: broker,
      logTiming: () => undefined,
    });
    const coordinator = createBackgroundCaptureCoordinator({
      now: () => clock,
      get: (code) => repository.get(code),
      getTrace: (code) => repository.getTrace(code),
      saveSnapshot: (value) => repository.saveSnapshot(value),
      saveDiagnostic: (value) => repository.saveDiagnostic(value),
      saveTrace: (value) => repository.saveTrace(value),
      capture: (code) => acquisition.capture(code),
    });
    const harness = createHarness((message) =>
      resolvePublicShikihoState(coordinator.resolve, message.code, message.forceRefresh)
    );
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot', '7203', true));
    for (let index = 0; index < 20 && !armed; index += 1) await flushPromises();
    expect(armed).toBe(true);
    clock += SHIKIHO_CAPTURE_TIMEOUT_MS;
    expire();
    for (let index = 0; index < 20; index += 1) await flushPromises();

    expect(harness.posted).toHaveLength(1);
    expect(harness.posted[0]).toMatchObject({
      type: 'snapshot',
      code: '7203',
      snapshot: null,
      diagnostic: null,
      trace: {
        attemptId: 'acquisition-query-timeout',
        mode: 'acquisition_unbound',
        phase: 'probing_tabs',
        outcome: 'timeout',
        waitEndReason: 'deadline',
      },
    });
    stop();
  });

  test('does not activate the localhost bridge on an unapproved port', () => {
    expect(isAllowedTrading25Origin(new URL('http://localhost:3002'))).toBe(false);
    expect(isAllowedTrading25Origin(new URL('http://localhost:5173'))).toBe(true);
    expect(isAllowedTrading25Origin(new URL('http://127.0.0.1:4173'))).toBe(true);
    expect(isAllowedTrading25Origin(new URL('https://localhost:5173'))).toBe(false);
  });

  test('returns before adding listeners on an unapproved port', () => {
    const addWindowListener = mock(() => undefined);
    const addStorageListener = mock(() => undefined);
    const stop = startLocalhostBridge({
      ...createHarness().options,
      url: new URL('http://localhost:3002'),
      addWindowListener,
      addStorageListener,
    });

    expect(addWindowListener).not.toHaveBeenCalled();
    expect(addStorageListener).not.toHaveBeenCalled();
    stop();
  });

  test('requires the current window source and exact page protocol messages', () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('ping'), {});
    harness.emitWindow({ ...request('ping'), extra: true });
    harness.emitWindow({ ...request('ping'), direction: 'extension-to-page' });
    expect(harness.posted).toEqual([]);
    expect(sendMessage).not.toHaveBeenCalled();

    harness.emitWindow(request('ping'));
    expect(harness.posted).toEqual([
      {
        channel: SHIKIHO_BRIDGE_CHANNEL,
        direction: 'extension-to-page',
        type: 'ready',
        requestId: 'request-1',
      },
    ]);
    stop();
  });

  test.each([
    ['missing', undefined],
    ['string', 'false'],
    ['numeric', 0],
  ])('rejects %s forceRefresh on get-snapshot requests', (_label, forceRefresh) => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);
    const { forceRefresh: _forceRefresh, ...requestWithoutForceRefresh } = request('get_snapshot');
    const malformed =
      forceRefresh === undefined ? requestWithoutForceRefresh : { ...requestWithoutForceRefresh, forceRefresh };

    harness.emitWindow(malformed);
    expect(sendMessage).not.toHaveBeenCalled();
    stop();
  });

  test('rejects extra get-snapshot request keys', () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow({ ...request('get_snapshot'), extra: true });
    expect(sendMessage).not.toHaveBeenCalled();
    stop();
  });

  test('translates page refresh intent exactly for the runtime', async () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot', '7203', false));
    await flushPromises();
    expect(sendMessage).toHaveBeenLastCalledWith({ type: 'resolve_snapshot', code: '7203', forceRefresh: false });

    harness.emitWindow({ ...request('get_snapshot', '7203', true), requestId: 'request-2' });
    await flushPromises();
    expect(sendMessage).toHaveBeenLastCalledWith({ type: 'resolve_snapshot', code: '7203', forceRefresh: true });
    stop();
  });

  test('drops malformed runtime responses instead of publishing them', async () => {
    const harness = createHarness(async () => ({ snapshot: null }));
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'));
    await flushPromises();
    expect(harness.posted).toEqual([]);
    stop();
  });

  test('rejects the removed legacy two-field runtime response', async () => {
    const harness = createHarness(async () => ({ snapshot: null, diagnostic: null }));
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'));
    await flushPromises();

    expect(harness.posted).toEqual([]);
    stop();
  });

  test('rejects an undefined mandatory trace while accepting an explicit null trace', async () => {
    const sendMessage = mock()
      .mockResolvedValueOnce({ snapshot: null, diagnostic: null, trace: undefined })
      .mockResolvedValueOnce({ snapshot: null, diagnostic: null, trace: null });
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'));
    await flushPromises();
    expect(harness.posted).toEqual([]);

    harness.emitWindow({ ...request('get_snapshot'), requestId: 'request-2' });
    await flushPromises();
    expect(harness.posted).toHaveLength(1);
    expect(harness.posted[0]).toMatchObject({ requestId: 'request-2', trace: null });
    stop();
  });

  test('publishes an explicit null snapshot when background acquisition fails', async () => {
    const harness = createHarness(async () => ({ ok: false }));
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow(request('get_snapshot'));
    await flushPromises();

    expect(harness.posted).toEqual([
      {
        channel: SHIKIHO_BRIDGE_CHANNEL,
        direction: 'extension-to-page',
        type: 'snapshot',
        requestId: 'request-1',
        code: '7203',
        snapshot: null,
        diagnostic: null,
        trace: null,
      },
    ]);
    stop();
  });

  test('refreshes only the selected code for relevant local storage changes', async () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null, trace: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow(request('get_snapshot'));
    await flushPromises();
    expect(sendMessage).toHaveBeenCalledTimes(1);

    harness.emitStorage({ [SHIKIHO_SNAPSHOTS_STORAGE_KEY]: { newValue: { '6758': {} } } });
    harness.emitStorage({ [SHIKIHO_DIAGNOSTICS_STORAGE_KEY]: { newValue: { '7203': {} } } }, 'sync');
    expect(sendMessage).toHaveBeenCalledTimes(1);

    harness.emitStorage({ [SHIKIHO_DIAGNOSTICS_STORAGE_KEY]: { newValue: { '7203': {} } } });
    expect(sendMessage).toHaveBeenCalledTimes(2);
    expect(sendMessage).toHaveBeenLastCalledWith({ type: 'resolve_snapshot', code: '7203', forceRefresh: false });

    harness.emitWindow({ ...request('get_snapshot', '6758'), requestId: 'request-2' });
    await flushPromises();
    expect(sendMessage).toHaveBeenCalledTimes(3);
    harness.emitStorage({ [SHIKIHO_SNAPSHOTS_STORAGE_KEY]: { newValue: { '7203': {} } } });
    expect(sendMessage).toHaveBeenCalledTimes(3);
    stop();
  });

  test('publishes only the newest overlapping read for the same request and code', async () => {
    const olderRead = deferred<unknown>();
    const newerRead = deferred<unknown>();
    const reads = [olderRead, newerRead];
    const harness = createHarness(() => reads.shift()?.promise ?? Promise.resolve(null));
    const stop = startLocalhostBridge(harness.options);
    harness.emitWindow(request('get_snapshot'));
    harness.emitStorage({ [SHIKIHO_DIAGNOSTICS_STORAGE_KEY]: { newValue: { '7203': {} } } });

    const newer = diagnostic('2026-07-10T02:00:00.000Z', 'page_changed');
    newerRead.resolve({ snapshot: null, diagnostic: newer, trace: null });
    await flushPromises();
    const older = diagnostic('2026-07-10T01:00:00.000Z', 'login_required');
    olderRead.resolve({ snapshot: null, diagnostic: older, trace: null });
    await flushPromises();

    expect(harness.posted).toHaveLength(1);
    expect(harness.posted[0]).toMatchObject({ type: 'snapshot', code: '7203', diagnostic: newer });
    stop();
  });

  test('removes the exact window and storage listeners on stop', () => {
    const harness = createHarness();
    const stop = startLocalhostBridge(harness.options);
    stop();

    expect(harness.removeWindowListener).toHaveBeenCalledTimes(1);
    expect(harness.removeStorageListener).toHaveBeenCalledTimes(1);
    harness.emitWindow(request('ping'));
    expect(harness.posted).toEqual([]);
  });
});
