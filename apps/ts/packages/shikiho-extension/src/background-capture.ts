import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from './contract';

export const SHIKIHO_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
export const SHIKIHO_RETRY_SUPPRESSION_MS = 60 * 1000;
export const SHIKIHO_CAPTURE_TIMEOUT_MS = 15 * 1000;

export interface StoredShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
}

export interface BackgroundCaptureDeps {
  now(): number;
  get(code: string): Promise<StoredShikihoState>;
  saveSnapshot(snapshot: ShikihoSnapshotV1): Promise<void>;
  saveDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1): Promise<void>;
  createTab(url: string): Promise<{ id: number }>;
  closeTab(tabId: number): Promise<void>;
  setTimer(callback: () => void, delayMs: number): unknown;
  clearTimer(timer: unknown): void;
}

interface PendingCapture {
  tabId: number;
  resolve(): void;
  reject(reason: unknown): void;
}

function age(now: number, timestamp: string): number | null {
  const parsed = Date.parse(timestamp);
  return Number.isNaN(parsed) ? null : now - parsed;
}

function shouldUseStoredState(state: StoredShikihoState, now: number): boolean {
  const snapshotAge = state.snapshot === null ? null : age(now, state.snapshot.capturedAt);
  if (snapshotAge !== null && snapshotAge >= 0 && snapshotAge < SHIKIHO_CACHE_TTL_MS) return true;
  const diagnosticAge = state.diagnostic === null ? null : age(now, state.diagnostic.observedAt);
  return diagnosticAge !== null && diagnosticAge >= 0 && diagnosticAge < SHIKIHO_RETRY_SUPPRESSION_MS;
}

export function createBackgroundCaptureCoordinator(deps: BackgroundCaptureDeps) {
  let fifoTail: Promise<unknown> = Promise.resolve();
  const singleflights = new Map<string, Promise<StoredShikihoState>>();
  const pendingByCode = new Map<string, PendingCapture>();

  async function run(code: string, forceRefresh: boolean): Promise<StoredShikihoState> {
    let ownedTabId: number | undefined;
    let timer: unknown;
    try {
      const stored = await deps.get(code);
      if (!forceRefresh && shouldUseStoredState(stored, deps.now())) return stored;

      const { id } = await deps.createTab(`https://shikiho.toyokeizai.net/stocks/${code}`);
      ownedTabId = id;
      const terminal = new Promise<void>((resolve, reject) => {
        pendingByCode.set(code, { tabId: id, resolve, reject });
        timer = deps.setTimer(resolve, SHIKIHO_CAPTURE_TIMEOUT_MS);
      });
      await terminal;
      return await deps.get(code);
    } finally {
      if (timer !== undefined) deps.clearTimer(timer);
      const pending = pendingByCode.get(code);
      if (pending?.tabId === ownedTabId) pendingByCode.delete(code);
      if (ownedTabId !== undefined) await deps.closeTab(ownedTabId).catch(() => undefined);
    }
  }

  function resolve(code: string, forceRefresh: boolean): Promise<StoredShikihoState> {
    const existing = singleflights.get(code);
    if (existing !== undefined) return existing;

    const result = fifoTail.then(
      () => run(code, forceRefresh),
      () => run(code, forceRefresh)
    );
    singleflights.set(code, result);
    fifoTail = result.catch(() => undefined);
    void result.finally(() => {
      if (singleflights.get(code) === result) singleflights.delete(code);
    }).catch(() => undefined);
    return result;
  }

  async function acceptSnapshot(snapshot: ShikihoSnapshotV1, senderTabId: number | null): Promise<void> {
    const pending = pendingByCode.get(snapshot.code);
    try {
      await deps.saveSnapshot(snapshot);
    } catch (error) {
      if (pending?.tabId === senderTabId) pending.reject(error);
      throw error;
    }
    if (pending?.tabId === senderTabId) pending.resolve();
  }

  async function acceptDiagnostic(
    diagnostic: ShikihoCaptureDiagnosticV1,
    senderTabId: number | null
  ): Promise<void> {
    const pending = pendingByCode.get(diagnostic.code);
    try {
      await deps.saveDiagnostic(diagnostic);
    } catch (error) {
      if (pending?.tabId === senderTabId) pending.reject(error);
      throw error;
    }
    if (pending?.tabId === senderTabId) pending.resolve();
  }

  return { resolve, acceptSnapshot, acceptDiagnostic };
}
