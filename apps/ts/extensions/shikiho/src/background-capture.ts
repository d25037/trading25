import type { ShikihoCaptureDiagnosticV1, ShikihoCaptureTraceV1, ShikihoSnapshotV1 } from './contract';
import type { AcquiredShikihoResult } from './tab-acquisition';

export const SHIKIHO_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
export const SHIKIHO_QUOTE_TTL_MS = 15 * 60 * 1000;
export const SHIKIHO_RETRY_SUPPRESSION_MS = 60 * 1000;

export interface StoredShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  trace?: ShikihoCaptureTraceV1 | null;
  successfulObservedAt?: string | null;
}

export interface PublicShikihoState {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  trace: ShikihoCaptureTraceV1 | null;
}

export async function resolvePublicShikihoState(
  resolve: (code: string, forceRefresh: boolean) => Promise<StoredShikihoState>,
  code: string,
  forceRefresh: boolean
): Promise<PublicShikihoState> {
  const { snapshot, diagnostic, successfulObservedAt, trace } = await resolve(code, forceRefresh);
  const successfulTime =
    successfulObservedAt === null || successfulObservedAt === undefined ? Number.NaN : Date.parse(successfulObservedAt);
  const diagnosticTime = diagnostic === null ? Number.NaN : Date.parse(diagnostic.observedAt);
  const snapshotTime = snapshot === null ? Number.NaN : Date.parse(snapshot.capturedAt);
  const publicSnapshot =
    snapshot !== null && Number.isFinite(successfulTime) && successfulTime >= snapshotTime
      ? { ...snapshot, capturedAt: new Date(successfulTime).toISOString() }
      : snapshot;
  const currentDiagnostic =
    diagnostic !== null &&
    Number.isFinite(successfulTime) &&
    Number.isFinite(diagnosticTime) &&
    successfulTime >= diagnosticTime
      ? null
      : diagnostic;
  return { snapshot: publicSnapshot, diagnostic: currentDiagnostic, trace: trace ?? null };
}

export interface BackgroundCaptureDeps {
  now(): number;
  get(code: string): Promise<StoredShikihoState>;
  getTrace(code: string): Promise<ShikihoCaptureTraceV1 | null>;
  saveSnapshot(snapshot: ShikihoSnapshotV1): Promise<void>;
  saveDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1): Promise<void>;
  saveTrace(trace: ShikihoCaptureTraceV1): Promise<void>;
  capture(code: string): Promise<AcquiredShikihoResult>;
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
  const sourceAge = age(now, quote.observedAt);
  const successfulObservedAt = state.successfulObservedAt ?? state.snapshot?.capturedAt;
  const captureAge = successfulObservedAt === undefined ? null : age(now, successfulObservedAt);
  return (
    sourceAge !== null && sourceAge >= 0 && captureAge !== null && captureAge >= 0 && captureAge < SHIKIHO_QUOTE_TTL_MS
  );
}

function shouldUseStoredState(state: StoredShikihoState, now: number): boolean {
  if (hasFreshArticle(state, now) && hasFreshCurrentJstQuote(state, now)) return true;
  const diagnosticAge = state.diagnostic === null ? null : age(now, state.diagnostic.observedAt);
  return diagnosticAge !== null && diagnosticAge >= 0 && diagnosticAge < SHIKIHO_RETRY_SUPPRESSION_MS;
}

export function createBackgroundCaptureCoordinator(deps: BackgroundCaptureDeps) {
  let fifoTail: Promise<unknown> = Promise.resolve();
  const singleflights = new Map<string, Promise<StoredShikihoState>>();

  async function readState(code: string): Promise<StoredShikihoState> {
    const state = await deps.get(code);
    return { ...state, trace: await deps.getTrace(code) };
  }

  async function capture(code: string): Promise<StoredShikihoState> {
    let acquired: AcquiredShikihoResult;
    try {
      acquired = await deps.capture(code);
    } catch (error) {
      const fallback = await readState(code);
      if (fallback.snapshot !== null || fallback.trace != null) return fallback;
      throw error;
    }
    try {
      const storageStartedAt = deps.now();
      if (acquired.result.kind === 'success') await deps.saveSnapshot(acquired.result.snapshot);
      else {
        await deps.saveDiagnostic({
          schemaVersion: 1,
          code,
          observedAt: new Date(deps.now()).toISOString(),
          status: acquired.result.kind,
        });
      }
      const storageFinishedAt = deps.now();
      const storageMs = Math.max(0, storageFinishedAt - storageStartedAt);
      const persistedTrace = (await deps.getTrace(code)) ?? acquired.trace;
      const attemptStartedAt = Date.parse(persistedTrace.startedAt);
      const updatedAt = Math.max(Date.parse(persistedTrace.updatedAt), storageFinishedAt);
      await deps.saveTrace({
        ...persistedTrace,
        updatedAt: new Date(updatedAt).toISOString(),
        timings: {
          ...persistedTrace.timings,
          storageMs,
          totalMs: Math.max(
            persistedTrace.timings.totalMs,
            Number.isFinite(attemptStartedAt) ? storageFinishedAt - attemptStartedAt : 0
          ),
        },
      });
      return await readState(code);
    } finally {
      await acquired.releaseOwnedTab?.();
    }
  }

  function resolve(code: string, forceRefresh: boolean): Promise<StoredShikihoState> {
    const existing = singleflights.get(code);
    if (existing !== undefined) return existing;

    const result = (async () => {
      const stored = await readState(code);
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

  async function acceptSnapshot(snapshot: ShikihoSnapshotV1, _senderTabId: number | null): Promise<void> {
    await deps.saveSnapshot(snapshot);
  }

  async function acceptDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1, _senderTabId: number | null): Promise<void> {
    await deps.saveDiagnostic(diagnostic);
  }

  return { resolve, acceptSnapshot, acceptDiagnostic };
}
