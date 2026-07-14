import {
  normalizeShikihoCode,
  parseShikihoDiagnostic,
  parseShikihoSnapshot,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoSnapshotV1,
} from './contract';
import { SHIKIHO_DIAGNOSTICS_STORAGE_KEY, SHIKIHO_SNAPSHOTS_STORAGE_KEY } from './storage';

type RuntimeSnapshotResponse = {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
};

type StorageChanges = Record<string, { oldValue?: unknown; newValue?: unknown }>;

export interface LocalhostBridgeOptions {
  url: URL;
  currentWindow: unknown;
  addWindowListener(listener: (event: MessageEvent) => void): void;
  removeWindowListener(listener: (event: MessageEvent) => void): void;
  addStorageListener(listener: (changes: StorageChanges, areaName: string) => void): void;
  removeStorageListener(listener: (changes: StorageChanges, areaName: string) => void): void;
  sendMessage(message: { type: 'resolve_snapshot'; code: string; forceRefresh: boolean }): Promise<unknown>;
  postMessage(message: ShikihoBridgeResponseV1): void;
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

export function parseShikihoBridgeRequest(value: unknown): ShikihoBridgeRequestV1 | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const message = value as Record<string, unknown>;
  if (
    message.channel !== SHIKIHO_BRIDGE_CHANNEL ||
    message.direction !== 'page-to-extension' ||
    typeof message.requestId !== 'string' ||
    message.requestId.length === 0 ||
    message.requestId.length > 256
  ) {
    return null;
  }
  if (message.type === 'ping' && hasExactKeys(message, ['channel', 'direction', 'type', 'requestId'])) {
    return message as unknown as ShikihoBridgeRequestV1;
  }
  if (
    message.type === 'get_snapshot' &&
    hasExactKeys(message, ['channel', 'direction', 'type', 'requestId', 'code', 'forceRefresh']) &&
    typeof message.code === 'string' &&
    normalizeShikihoCode(message.code) === message.code &&
    typeof message.forceRefresh === 'boolean'
  ) {
    return message as unknown as ShikihoBridgeRequestV1;
  }
  return null;
}

export function isAllowedTrading25Origin(url: URL): boolean {
  return (
    url.protocol === 'http:' &&
    (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
    (url.port === '5173' || url.port === '4173') &&
    url.username === '' &&
    url.password === ''
  );
}

function defaultOptions(): LocalhostBridgeOptions | null {
  if (typeof window === 'undefined' || typeof chrome === 'undefined') return null;
  return {
    url: new URL(window.location.href),
    currentWindow: window,
    addWindowListener: (listener) => window.addEventListener('message', listener),
    removeWindowListener: (listener) => window.removeEventListener('message', listener),
    addStorageListener: (listener) => chrome.storage.onChanged.addListener(listener),
    removeStorageListener: (listener) => chrome.storage.onChanged.removeListener(listener),
    sendMessage: (message) => chrome.runtime.sendMessage(message),
    postMessage: (message) => window.postMessage(message, window.location.origin),
  };
}

function parseRuntimeResponse(value: unknown, code: string): RuntimeSnapshotResponse | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const response = value as Record<string, unknown>;
  if (!hasExactKeys(response, ['snapshot', 'diagnostic'])) return null;
  const snapshot = response.snapshot === null ? null : parseShikihoSnapshot(response.snapshot);
  const diagnostic = response.diagnostic === null ? null : parseShikihoDiagnostic(response.diagnostic);
  if (
    (response.snapshot !== null && snapshot === null) ||
    (response.diagnostic !== null && diagnostic === null) ||
    (snapshot !== null && snapshot.code !== code) ||
    (diagnostic !== null && diagnostic.code !== code)
  ) {
    return null;
  }
  return { snapshot, diagnostic };
}

function storageMapHasCode(value: unknown, code: string): boolean {
  return typeof value === 'object' && value !== null && !Array.isArray(value) && code in value;
}

function isExplicitRuntimeFailure(value: unknown): boolean {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    Object.keys(value).length === 1 &&
    (value as Record<string, unknown>).ok === false
  );
}

export function startLocalhostBridge(provided?: LocalhostBridgeOptions): () => void {
  const options = provided ?? defaultOptions();
  if (options === null || !isAllowedTrading25Origin(options.url)) return () => undefined;
  const activeOptions = options;

  let currentRequest: { code: string; requestId: string } | null = null;
  let latestReadGeneration = 0;

  async function sendSnapshot(request: { code: string; requestId: string }, forceRefresh: boolean): Promise<void> {
    const readGeneration = ++latestReadGeneration;
    const raw = await activeOptions.sendMessage({ type: 'resolve_snapshot', code: request.code, forceRefresh });
    if (
      readGeneration !== latestReadGeneration ||
      currentRequest?.code !== request.code ||
      currentRequest.requestId !== request.requestId
    ) {
      return;
    }
    const response = parseRuntimeResponse(raw, request.code);
    if (response === null && !isExplicitRuntimeFailure(raw)) return;
    activeOptions.postMessage({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'extension-to-page',
      type: 'snapshot',
      requestId: request.requestId,
      code: request.code,
      snapshot: response?.snapshot ?? null,
      diagnostic: response?.diagnostic ?? null,
      trace: null,
    });
  }

  const onWindowMessage = (event: MessageEvent): void => {
    if (event.source !== activeOptions.currentWindow) return;
    const request = parseShikihoBridgeRequest(event.data);
    if (request === null) return;
    if (request.type === 'ping') {
      activeOptions.postMessage({
        channel: SHIKIHO_BRIDGE_CHANNEL,
        direction: 'extension-to-page',
        type: 'ready',
        requestId: request.requestId,
      });
      return;
    }
    currentRequest = { code: request.code, requestId: request.requestId };
    void sendSnapshot(currentRequest, request.forceRefresh);
  };

  const onStorageChanged = (changes: StorageChanges, areaName: string): void => {
    if (areaName !== 'local' || currentRequest === null) return;
    const snapshotsChanged = changes[SHIKIHO_SNAPSHOTS_STORAGE_KEY] !== undefined;
    const diagnosticsChanged = changes[SHIKIHO_DIAGNOSTICS_STORAGE_KEY] !== undefined;
    if (!snapshotsChanged && !diagnosticsChanged) return;
    const requestedCode = currentRequest.code;
    const relevantChange = [changes[SHIKIHO_SNAPSHOTS_STORAGE_KEY], changes[SHIKIHO_DIAGNOSTICS_STORAGE_KEY]]
      .filter((change): change is NonNullable<typeof change> => change !== undefined)
      .some(
        (change) =>
          storageMapHasCode(change.newValue, requestedCode) || storageMapHasCode(change.oldValue, requestedCode)
      );
    if (relevantChange) void sendSnapshot(currentRequest, false);
  };

  activeOptions.addWindowListener(onWindowMessage);
  activeOptions.addStorageListener(onStorageChanged);
  return () => {
    activeOptions.removeWindowListener(onWindowMessage);
    activeOptions.removeStorageListener(onStorageChanged);
    currentRequest = null;
    latestReadGeneration += 1;
  };
}

startLocalhostBridge();
