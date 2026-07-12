import { describe, expect, mock, test } from 'bun:test';
import { SHIKIHO_BRIDGE_CHANNEL, type ShikihoCaptureDiagnosticV1 } from './contract';
import { isAllowedTrading25Origin, type LocalhostBridgeOptions, startLocalhostBridge } from './localhost-content';
import { SHIKIHO_DIAGNOSTICS_STORAGE_KEY, SHIKIHO_SNAPSHOTS_STORAGE_KEY } from './storage';

type WindowListener = (event: MessageEvent) => void;
type StorageListener = (changes: Record<string, { oldValue?: unknown; newValue?: unknown }>, areaName: string) => void;

function diagnostic(observedAt: string, status: ShikihoCaptureDiagnosticV1['status']): ShikihoCaptureDiagnosticV1 {
  return { schemaVersion: 1, code: '7203', observedAt, status };
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
  sendMessage: LocalhostBridgeOptions['sendMessage'] = async () => ({ snapshot: null, diagnostic: null })
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
    sendMessage,
    postMessage: (message) => posted.push(message),
  };
  return {
    currentWindow,
    options,
    posted,
    removeWindowListener,
    removeStorageListener,
    emitWindow(data: unknown, source: unknown = currentWindow) {
      windowListener?.({ data, source } as MessageEvent);
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
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null }));
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
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);
    const malformed = request('get_snapshot') as Record<string, unknown>;
    if (forceRefresh === undefined) delete malformed.forceRefresh;
    else malformed.forceRefresh = forceRefresh;

    harness.emitWindow(malformed);
    expect(sendMessage).not.toHaveBeenCalled();
    stop();
  });

  test('rejects extra get-snapshot request keys', () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null }));
    const harness = createHarness(sendMessage);
    const stop = startLocalhostBridge(harness.options);

    harness.emitWindow({ ...request('get_snapshot'), extra: true });
    expect(sendMessage).not.toHaveBeenCalled();
    stop();
  });

  test('translates page refresh intent exactly for the runtime', async () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null }));
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

  test('refreshes only the selected code for relevant local storage changes', async () => {
    const sendMessage = mock(async () => ({ snapshot: null, diagnostic: null }));
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
    newerRead.resolve({ snapshot: null, diagnostic: newer });
    await flushPromises();
    const older = diagnostic('2026-07-10T01:00:00.000Z', 'login_required');
    olderRead.resolve({ snapshot: null, diagnostic: older });
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
