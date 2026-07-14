import { afterEach, describe, expect, mock, test } from 'bun:test';
import { type ShikihoBackgroundRuntimeDeps, startShikihoBackgroundRuntime } from './background-runtime';
import type { TabMessageReply } from './tab-acquisition';
import type { WarmTabLeaseManager } from './warm-tab-lease';

function event<T extends (...args: never[]) => void>() {
  const listeners = new Set<T>();
  return {
    addListener: mock((listener: T) => listeners.add(listener)),
    removeListener: mock((listener: T) => listeners.delete(listener)),
    emit(...args: Parameters<T>) {
      for (const listener of listeners) listener(...args);
    },
    size: () => listeners.size,
  };
}

async function waitForCalls(fn: unknown, count: number): Promise<void> {
  const mocked = fn as unknown as { mock: { calls: unknown[][] } };
  for (let attempt = 0; attempt < 20 && mocked.mock.calls.length < count; attempt += 1) await Promise.resolve();
  expect(mocked.mock.calls.length).toBe(count);
}

function harness() {
  const getValidOwnedTabId = mock(async () => 41 as number | null);
  const reconcile = mock(async () => undefined);
  const onAlarm = mock(async () => undefined);
  const onActivated = mock(async () => undefined);
  const abandonOwnedTab = mock(async () => undefined);
  const onRemoved = mock(async () => undefined);
  const leaseManager: WarmTabLeaseManager = {
    getValidOwnedTabId,
    reconcile,
    acquire: mock(async () => {
      throw new Error('not used');
    }),
    releaseSuccess: mock(async () => undefined),
    releaseFailure: mock(async () => undefined),
    onAlarm,
    onActivated,
    abandonOwnedTab,
    abandonIfOwned: mock(async () => undefined),
    onRemoved,
  };
  const alarmsOnAlarm = event<(alarm: { name: string }) => void>();
  const tabsOnActivated = event<(activeInfo: { tabId: number }) => void>();
  const tabsOnRemoved = event<(tabId: number) => void>();
  const tabsOnUpdated = event<(tabId: number, changeInfo: { status?: string }) => void>();
  const runtimeOnStartup = event<() => void>();
  const sendTabMessage = mock(
    async (tabId: number): Promise<TabMessageReply> => ({
      tabId,
      response: { type: 'shikiho_code', code: '7203' },
    })
  );
  const deps: ShikihoBackgroundRuntimeDeps = {
    leaseManager,
    sendTabMessage,
    alarmsOnAlarm,
    tabsOnActivated,
    tabsOnRemoved,
    tabsOnUpdated,
    runtimeOnStartup,
  };
  return {
    deps,
    leaseManager,
    getValidOwnedTabId,
    reconcile,
    onAlarm,
    onActivated,
    abandonOwnedTab,
    onRemoved,
    sendTabMessage,
    alarmsOnAlarm,
    tabsOnActivated,
    tabsOnRemoved,
    tabsOnUpdated,
    runtimeOnStartup,
  };
}

afterEach(() => mock.restore());

describe('Shikiho background runtime lifecycle', () => {
  test('reconciles on initialization and startup and removes every listener on cleanup', async () => {
    const h = harness();

    const stop = startShikihoBackgroundRuntime(h.deps);
    await waitForCalls(h.reconcile, 1);
    h.runtimeOnStartup.emit();
    await waitForCalls(h.reconcile, 2);

    expect(h.alarmsOnAlarm.size()).toBe(1);
    expect(h.tabsOnActivated.size()).toBe(1);
    expect(h.tabsOnRemoved.size()).toBe(1);
    expect(h.tabsOnUpdated.size()).toBe(1);
    expect(h.runtimeOnStartup.size()).toBe(1);

    stop();

    expect(h.alarmsOnAlarm.size()).toBe(0);
    expect(h.tabsOnActivated.size()).toBe(0);
    expect(h.tabsOnRemoved.size()).toBe(0);
    expect(h.tabsOnUpdated.size()).toBe(0);
    expect(h.runtimeOnStartup.size()).toBe(0);
  });

  test('delegates alarms, activations, and removals by name or tab ID', async () => {
    const h = harness();
    startShikihoBackgroundRuntime(h.deps);

    h.alarmsOnAlarm.emit({ name: 'shikiho-warm-tab:41:owner:1:123' });
    h.tabsOnActivated.emit({ tabId: 42 });
    h.tabsOnRemoved.emit(43);

    await waitForCalls(h.onAlarm, 1);
    await waitForCalls(h.onActivated, 1);
    await waitForCalls(h.onRemoved, 1);
    expect(h.onAlarm).toHaveBeenCalledWith('shikiho-warm-tab:41:owner:1:123');
    expect(h.onActivated).toHaveBeenCalledWith(42);
    expect(h.onRemoved).toHaveBeenCalledWith(43);
  });

  test('probes only a matching owned tab after loading completes without reading tab metadata', async () => {
    const h = harness();
    startShikihoBackgroundRuntime(h.deps);

    h.tabsOnUpdated.emit(41, { status: 'loading' });
    h.tabsOnUpdated.emit(42, { status: 'complete' });
    h.tabsOnUpdated.emit(41, { status: 'complete' });

    await waitForCalls(h.getValidOwnedTabId, 2);
    await waitForCalls(h.sendTabMessage, 1);
    expect(h.sendTabMessage).toHaveBeenCalledWith(41, { type: 'probe_shikiho_code' });
    expect(h.abandonOwnedTab).toHaveBeenCalledTimes(0);
  });

  test('abandons ownership without closing when the probe receiver is missing or reports no code', async () => {
    const h = harness();
    h.sendTabMessage
      .mockRejectedValueOnce(new Error('Receiving end does not exist'))
      .mockResolvedValueOnce({ tabId: 41, response: { type: 'shikiho_code', code: null } });
    startShikihoBackgroundRuntime(h.deps);

    h.tabsOnUpdated.emit(41, { status: 'complete' });
    await waitForCalls(h.abandonOwnedTab, 1);
    h.tabsOnUpdated.emit(41, { status: 'complete' });
    await waitForCalls(h.abandonOwnedTab, 2);

    expect(h.abandonOwnedTab).toHaveBeenNthCalledWith(1, 41);
    expect(h.abandonOwnedTab).toHaveBeenNthCalledWith(2, 41);
    expect(h.leaseManager.releaseFailure).toHaveBeenCalledTimes(0);
    expect(h.onRemoved).toHaveBeenCalledTimes(0);
  });
});
