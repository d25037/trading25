import type { ProgressPort } from './capture-progress';
import {
  normalizeShikihoCode,
  parseShikihoCaptureProgress,
  parseShikihoCaptureTrace,
  parseShikihoDiagnostic,
  parseShikihoSnapshot,
  SHIKIHO_BRIDGE_CHANNEL,
  type ShikihoBridgeRequestV1,
  type ShikihoBridgeResponseV1,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoCaptureTraceV1,
  type ShikihoSnapshotV1,
} from './contract';
import { SHIKIHO_DIAGNOSTICS_STORAGE_KEY, SHIKIHO_SNAPSHOTS_STORAGE_KEY, SHIKIHO_TRACES_STORAGE_KEY } from './storage';

export const SHIKIHO_CAPTURE_PROGRESS_PORT_NAME = 'shikiho-capture-progress-v1';

export interface LocalhostProgressPort extends ProgressPort {
  disconnect(): void;
}

type RuntimeSnapshotResponse = {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  trace: ShikihoCaptureTraceV1 | null;
};

type StorageChanges = Record<string, { oldValue?: unknown; newValue?: unknown }>;

interface CurrentPageRequest {
  code: string;
  requestId: string;
  attemptId: string | null;
  retiredAttemptIds: Set<string>;
  lastSequence: number;
  terminal: boolean;
}

export interface LocalhostBridgeOptions {
  url: URL;
  currentWindow: unknown;
  addWindowListener(listener: (event: MessageEvent) => void): void;
  removeWindowListener(listener: (event: MessageEvent) => void): void;
  addStorageListener(listener: (changes: StorageChanges, areaName: string) => void): void;
  removeStorageListener(listener: (changes: StorageChanges, areaName: string) => void): void;
  connectProgressPort(): LocalhostProgressPort;
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
    connectProgressPort: () => {
      const port = chrome.runtime.connect({ name: SHIKIHO_CAPTURE_PROGRESS_PORT_NAME });
      return {
        postMessage: (message) => port.postMessage(message),
        onMessage: {
          addListener: (listener) => port.onMessage.addListener(listener),
          removeListener: (listener) => port.onMessage.removeListener(listener),
        },
        onDisconnect: {
          addListener: (listener) => port.onDisconnect.addListener(listener),
          removeListener: (listener) => port.onDisconnect.removeListener(listener),
        },
        disconnect: () => port.disconnect(),
      };
    },
    sendMessage: (message) => chrome.runtime.sendMessage(message),
    postMessage: (message) => window.postMessage(message, window.location.origin),
  };
}

function hasRuntimeResponseShape(response: Record<string, unknown>): boolean {
  return hasExactKeys(response, ['snapshot', 'diagnostic', 'trace']);
}

function hasInvalidParsedRuntimeField(response: Record<string, unknown>, parsed: RuntimeSnapshotResponse): boolean {
  return (
    (response.snapshot !== null && parsed.snapshot === null) ||
    (response.diagnostic !== null && parsed.diagnostic === null) ||
    (response.trace !== null && parsed.trace === null)
  );
}

function hasMismatchedRuntimeCode(response: RuntimeSnapshotResponse, code: string): boolean {
  return [response.snapshot?.code, response.diagnostic?.code, response.trace?.code].some(
    (candidate) => candidate !== undefined && candidate !== code
  );
}

function parseRuntimeResponse(value: unknown, code: string): RuntimeSnapshotResponse | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const response = value as Record<string, unknown>;
  if (!hasRuntimeResponseShape(response)) return null;
  const snapshot = response.snapshot === null ? null : parseShikihoSnapshot(response.snapshot);
  const diagnostic = response.diagnostic === null ? null : parseShikihoDiagnostic(response.diagnostic);
  const trace = response.trace === null ? null : parseShikihoCaptureTrace(response.trace);
  const parsed = { snapshot, diagnostic, trace };
  return hasInvalidParsedRuntimeField(response, parsed) || hasMismatchedRuntimeCode(parsed, code) ? null : parsed;
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

function advanceProgressAttempt(request: CurrentPageRequest, attemptId: string, sequence: number): boolean {
  if (request.attemptId === null) {
    request.attemptId = attemptId;
  } else if (attemptId !== request.attemptId) {
    if (request.retiredAttemptIds.has(attemptId)) return false;
    request.retiredAttemptIds.add(request.attemptId);
    request.attemptId = attemptId;
    request.lastSequence = 0;
  }
  if (sequence <= request.lastSequence) return false;
  request.lastSequence = sequence;
  return true;
}

export function startLocalhostBridge(provided?: LocalhostBridgeOptions): () => void {
  const options = provided ?? defaultOptions();
  if (options === null || !isAllowedTrading25Origin(options.url)) return () => undefined;
  const activeOptions = options;

  let currentRequest: CurrentPageRequest | null = null;
  let latestReadGeneration = 0;
  let stopped = false;
  let progressPort: LocalhostProgressPort | null = null;
  let removeProgressPortListeners: (() => void) | null = null;

  function closeProgressPort(disconnect: boolean): void {
    const port = progressPort;
    removeProgressPortListeners?.();
    removeProgressPortListeners = null;
    progressPort = null;
    if (disconnect && port !== null) {
      try {
        port.disconnect();
      } catch {
        // A Chrome Port may already be disconnected.
      }
    }
  }

  function onProgressMessage(message: unknown): void {
    if (
      stopped ||
      currentRequest === null ||
      currentRequest.terminal ||
      typeof message !== 'object' ||
      message === null ||
      Array.isArray(message)
    ) {
      return;
    }
    const record = message as Record<string, unknown>;
    if (!hasExactKeys(record, ['type', 'progress']) || record.type !== 'capture_progress') return;
    const progress = parseShikihoCaptureProgress(record.progress);
    if (progress === null || progress.code !== currentRequest.code) return;
    if (!advanceProgressAttempt(currentRequest, progress.attemptId, progress.sequence)) return;
    activeOptions.postMessage({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'extension-to-page',
      type: 'capture_progress',
      requestId: currentRequest.requestId,
      code: progress.code,
      attemptId: progress.attemptId,
      sequence: progress.sequence,
      candidate: progress.candidate,
      trace: progress.trace,
    });
  }

  function ensureProgressPort(): LocalhostProgressPort | null {
    if (progressPort !== null) return progressPort;
    try {
      const port = activeOptions.connectProgressPort();
      const onDisconnect = () => {
        if (progressPort !== port) return;
        closeProgressPort(false);
        if (currentRequest !== null) {
          currentRequest.attemptId = null;
          currentRequest.retiredAttemptIds.clear();
          currentRequest.lastSequence = 0;
        }
      };
      port.onMessage.addListener(onProgressMessage);
      port.onDisconnect.addListener(onDisconnect);
      progressPort = port;
      removeProgressPortListeners = () => {
        port.onMessage.removeListener(onProgressMessage);
        port.onDisconnect.removeListener(onDisconnect);
      };
      return port;
    } catch {
      return null;
    }
  }

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
    if (currentRequest !== null) currentRequest.terminal = true;
    activeOptions.postMessage({
      channel: SHIKIHO_BRIDGE_CHANNEL,
      direction: 'extension-to-page',
      type: 'snapshot',
      requestId: request.requestId,
      code: request.code,
      snapshot: response?.snapshot ?? null,
      diagnostic: response?.diagnostic ?? null,
      trace: response?.trace ?? null,
    });
  }

  const onWindowMessage = (event: MessageEvent): void => {
    if (event.source !== activeOptions.currentWindow || event.origin !== activeOptions.url.origin) return;
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
    if (currentRequest?.code !== request.code || currentRequest.requestId !== request.requestId) {
      currentRequest = {
        code: request.code,
        requestId: request.requestId,
        attemptId: null,
        retiredAttemptIds: new Set(),
        lastSequence: 0,
        terminal: false,
      };
    }
    ensureProgressPort()?.postMessage({ type: 'subscribe_capture_progress', code: request.code });
    void sendSnapshot(currentRequest, request.forceRefresh);
  };

  const onStorageChanged = (changes: StorageChanges, areaName: string): void => {
    if (areaName !== 'local' || currentRequest === null) return;
    const snapshotsChanged = changes[SHIKIHO_SNAPSHOTS_STORAGE_KEY] !== undefined;
    const diagnosticsChanged = changes[SHIKIHO_DIAGNOSTICS_STORAGE_KEY] !== undefined;
    const tracesChanged = changes[SHIKIHO_TRACES_STORAGE_KEY] !== undefined;
    if (!snapshotsChanged && !diagnosticsChanged && !tracesChanged) return;
    const requestedCode = currentRequest.code;
    const relevantChange = [
      changes[SHIKIHO_SNAPSHOTS_STORAGE_KEY],
      changes[SHIKIHO_DIAGNOSTICS_STORAGE_KEY],
      changes[SHIKIHO_TRACES_STORAGE_KEY],
    ]
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
    if (stopped) return;
    stopped = true;
    activeOptions.removeWindowListener(onWindowMessage);
    activeOptions.removeStorageListener(onStorageChanged);
    closeProgressPort(true);
    currentRequest = null;
    latestReadGeneration += 1;
  };
}

startLocalhostBridge();
