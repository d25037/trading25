import { createBackgroundCaptureCoordinator, resolvePublicShikihoState } from './background-capture';
import { startShikihoBackgroundRuntime } from './background-runtime';
import { createCancelableTimer } from './cancelable-timer';
import { normalizeShikihoCode, parseShikihoDiagnostic, parseShikihoSnapshot } from './contract';
import type { ShikihoTabRequest } from './shikiho-tab-bridge';
import { createShikihoRepository } from './storage';
import { createShikihoTabAcquisition, type TabMessageReply } from './tab-acquisition';
import { createWarmTabLeaseManager } from './warm-tab-lease';

const repository = createShikihoRepository();
const sendTabMessage = async (tabId: number, message: ShikihoTabRequest): Promise<TabMessageReply> => ({
  tabId,
  response: await chrome.tabs.sendMessage(tabId, message),
});
const leaseManager = createWarmTabLeaseManager({
  now: () => Date.now(),
  createOwnerToken: () => crypto.randomUUID(),
  tabs: {
    create: (properties) => chrome.tabs.create(properties),
    update: (tabId, properties) => chrome.tabs.update(tabId, properties),
    reload: (tabId) => chrome.tabs.reload(tabId),
    remove: (tabId) => chrome.tabs.remove(tabId),
    get: (tabId) => chrome.tabs.get(tabId),
  },
  session: {
    get: async (key) => (await chrome.storage.session.get(key))[key],
    set: (key, value) => chrome.storage.session.set({ [key]: value }),
    remove: (key) => chrome.storage.session.remove(key),
  },
  alarms: {
    create: (name, when) => chrome.alarms.create(name, { when }),
    clear: (name) => chrome.alarms.clear(name),
  },
  hasShikihoStockContentScript: async (tabId) => {
    const { response } = await sendTabMessage(tabId, { type: 'probe_shikiho_code' });
    if (typeof response !== 'object' || response === null) return false;
    const code = (response as Record<string, unknown>).code;
    return typeof code === 'string' && normalizeShikihoCode(code) === code;
  },
});
const acquisition = createShikihoTabAcquisition({
  now: () => Date.now(),
  delay: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  createTimer: (ms) => createCancelableTimer(ms),
  createRequestId: () => crypto.randomUUID(),
  queryTabs: () => chrome.tabs.query({}),
  sendTabMessage,
  getValidWarmTabId: () => leaseManager.getValidOwnedTabId(),
  leaseManager,
  logTiming: (timing) => console.debug(timing),
});
const coordinator = createBackgroundCaptureCoordinator({
  now: () => Date.now(),
  get: (code) => repository.get(code),
  saveSnapshot: (snapshot) => repository.saveSnapshot(snapshot),
  saveDiagnostic: (diagnostic) => repository.saveDiagnostic(diagnostic),
  capture: (code) => acquisition.capture(code),
});

startShikihoBackgroundRuntime({
  leaseManager,
  alarmsOnAlarm: chrome.alarms.onAlarm,
  tabsOnActivated: chrome.tabs.onActivated,
  tabsOnRemoved: chrome.tabs.onRemoved,
  tabsOnUpdated: chrome.tabs.onUpdated,
  runtimeOnStartup: chrome.runtime.onStartup,
});

type BackgroundMessage =
  | { type: 'capture_success'; snapshot: unknown }
  | { type: 'capture_diagnostic'; diagnostic: unknown }
  | { type: 'resolve_snapshot'; code: unknown; forceRefresh: unknown };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function parseBackgroundMessage(value: unknown): BackgroundMessage | null {
  if (!isRecord(value)) return null;
  if (value.type === 'capture_success' && hasExactKeys(value, ['type', 'snapshot'])) {
    return { type: value.type, snapshot: value.snapshot };
  }
  if (value.type === 'capture_diagnostic' && hasExactKeys(value, ['type', 'diagnostic'])) {
    return { type: value.type, diagnostic: value.diagnostic };
  }
  if (value.type === 'resolve_snapshot' && hasExactKeys(value, ['type', 'code', 'forceRefresh'])) {
    return { type: value.type, code: value.code, forceRefresh: value.forceRefresh };
  }
  return null;
}

async function handleBackgroundMessage(message: BackgroundMessage, senderTabId: number | null): Promise<unknown> {
  if (message.type === 'capture_success') {
    const snapshot = parseShikihoSnapshot(message.snapshot);
    if (snapshot === null) return { ok: false };
    try {
      await coordinator.acceptSnapshot(snapshot, senderTabId);
    } catch {
      await repository
        .saveDiagnostic({
          schemaVersion: 1,
          code: snapshot.code,
          observedAt: new Date().toISOString(),
          status: 'storage_error',
        })
        .catch(() => undefined);
      return { ok: false };
    }
    return { ok: true };
  }
  if (message.type === 'capture_diagnostic') {
    const diagnostic = parseShikihoDiagnostic(message.diagnostic);
    if (diagnostic === null) return { ok: false };
    await coordinator.acceptDiagnostic(diagnostic, senderTabId);
    return { ok: true };
  }
  const code = normalizeShikihoCode(message.code);
  if (code === null || code !== message.code || typeof message.forceRefresh !== 'boolean') {
    return { snapshot: null, diagnostic: null };
  }
  return resolvePublicShikihoState(coordinator.resolve, code, message.forceRefresh);
}

chrome.runtime.onMessage.addListener((rawMessage: unknown, sender, sendResponse) => {
  const message = parseBackgroundMessage(rawMessage);
  if (message === null) return false;

  void handleBackgroundMessage(message, sender.tab?.id ?? null)
    .then(sendResponse)
    .catch(() => sendResponse({ ok: false }));

  return true;
});
