import { normalizeShikihoCode, parseShikihoDiagnostic, parseShikihoSnapshot } from './contract';
import { createBackgroundCaptureCoordinator, resolvePublicShikihoState } from './background-capture';
import { createShikihoRepository } from './storage';

const repository = createShikihoRepository();
const coordinator = createBackgroundCaptureCoordinator({
  now: () => Date.now(),
  get: (code) => repository.get(code),
  saveSnapshot: (snapshot) => repository.saveSnapshot(snapshot),
  saveDiagnostic: (diagnostic) => repository.saveDiagnostic(diagnostic),
  createTab: async (url) => {
    const tab = await chrome.tabs.create({ active: false, url });
    if (tab.id === undefined) throw new Error('Created Shikiho tab has no ID');
    return { id: tab.id };
  },
  closeTab: (tabId) => chrome.tabs.remove(tabId),
  setTimer: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimer: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
});

chrome.tabs.onRemoved.addListener((tabId) => {
  void coordinator.onTabRemoved(tabId).catch(() => undefined);
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
