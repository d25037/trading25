import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from './contract';

export const SHIKIHO_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
export const SHIKIHO_QUOTE_TTL_MS = 15 * 60 * 1000;
export const SHIKIHO_RETRY_SUPPRESSION_MS = 60 * 1000;
export const SHIKIHO_CAPTURE_TIMEOUT_MS = 15 * 1000;

export interface StoredShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  successfulObservedAt?: string | null;
}

export interface PublicShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
}

export async function resolvePublicShikihoState(
  resolve: (code: string, forceRefresh: boolean) => Promise<StoredShikihoState>,
  code: string,
  forceRefresh: boolean
): Promise<PublicShikihoState> {
  const { snapshot, diagnostic } = await resolve(code, forceRefresh);
  return { snapshot, diagnostic };
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

function currentJstDate(now: number): string {
  return new Date(now + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function hasFreshArticle(state: StoredShikihoState, now: number): boolean {
  if (state.snapshot === null) return false;
  const successfulObservedAt = state.successfulObservedAt ?? state.snapshot.capturedAt;
  const snapshotAge = age(now, successfulObservedAt);
  return snapshotAge !== null && snapshotAge >= 0 && snapshotAge < SHIKIHO_CACHE_TTL_MS;
}

function hasFreshCurrentJstQuote(state: StoredShikihoState, now: number): boolean {
  const quote = state.snapshot?.quote;
  if (quote === undefined || quote.tradingDate !== currentJstDate(now)) return false;
  const quoteAge = age(now, quote.observedAt);
  return quoteAge !== null && quoteAge >= 0 && quoteAge < SHIKIHO_QUOTE_TTL_MS;
}

function shouldUseStoredState(state: StoredShikihoState, now: number): boolean {
  if (hasFreshArticle(state, now) && hasFreshCurrentJstQuote(state, now)) return true;
  const diagnosticAge = state.diagnostic === null ? null : age(now, state.diagnostic.observedAt);
  return diagnosticAge !== null && diagnosticAge >= 0 && diagnosticAge < SHIKIHO_RETRY_SUPPRESSION_MS;
}

export function createBackgroundCaptureCoordinator(deps: BackgroundCaptureDeps) {
  let fifoTail: Promise<unknown> = Promise.resolve();
  const singleflights = new Map<string, Promise<StoredShikihoState>>();
  const pendingByCode = new Map<string, PendingCapture>();
  const removedOwnedTabs = new Set<number>();

  async function capture(code: string): Promise<StoredShikihoState> {
    let ownedTabId: number | undefined;
    let timer: unknown;
    try {
      const { id } = await deps.createTab(`https://shikiho.toyokeizai.net/stocks/${code}`);
      ownedTabId = id;
      const terminal = new Promise<void>((resolve, reject) => {
        pendingByCode.set(code, { tabId: id, resolve, reject });
        timer = deps.setTimer(() => {
          void deps
            .saveDiagnostic({
              schemaVersion: 1,
              code,
              observedAt: new Date(deps.now()).toISOString(),
              status: 'page_changed',
            })
            .then(resolve, reject);
        }, SHIKIHO_CAPTURE_TIMEOUT_MS);
      });
      await terminal;
      return await deps.get(code);
    } finally {
      if (timer !== undefined) deps.clearTimer(timer);
      const pending = pendingByCode.get(code);
      if (pending?.tabId === ownedTabId) pendingByCode.delete(code);
      if (ownedTabId !== undefined) {
        if (removedOwnedTabs.has(ownedTabId)) removedOwnedTabs.delete(ownedTabId);
        else await deps.closeTab(ownedTabId).catch(() => undefined);
      }
    }
  }

  function resolve(code: string, forceRefresh: boolean): Promise<StoredShikihoState> {
    const existing = singleflights.get(code);
    if (existing !== undefined) return existing;

    const result = (async () => {
      const stored = await deps.get(code);
      if (!forceRefresh && shouldUseStoredState(stored, deps.now())) return stored;
      const queued = fifoTail.then(
        () => capture(code),
        () => capture(code)
      );
      fifoTail = queued.catch(() => undefined);
      return queued;
    })();
    singleflights.set(code, result);
    void result
      .finally(() => {
        if (singleflights.get(code) === result) singleflights.delete(code);
      })
      .catch(() => undefined);
    return result;
  }

  async function onTabRemoved(tabId: number): Promise<void> {
    const entry = [...pendingByCode.entries()].find(([, pending]) => pending.tabId === tabId);
    if (entry === undefined) return;
    const [code, pending] = entry;
    removedOwnedTabs.add(tabId);
    try {
      await deps.saveDiagnostic({
        schemaVersion: 1,
        code,
        observedAt: new Date(deps.now()).toISOString(),
        status: 'page_changed',
      });
      pending.resolve();
    } catch (error) {
      pending.reject(error);
      throw error;
    }
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

  async function acceptDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1, senderTabId: number | null): Promise<void> {
    const pending = pendingByCode.get(diagnostic.code);
    try {
      await deps.saveDiagnostic(diagnostic);
    } catch (error) {
      if (pending?.tabId === senderTabId) pending.reject(error);
      throw error;
    }
    if (pending?.tabId === senderTabId && diagnostic.status !== 'page_changed') pending.resolve();
  }

  return { resolve, acceptSnapshot, acceptDiagnostic, onTabRemoved };
}
