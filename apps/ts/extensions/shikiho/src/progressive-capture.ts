import type {
  ShikihoCaptureProgressV1,
  ShikihoCaptureTraceV1,
  ShikihoFieldMilestonesV1,
  ShikihoSnapshotV1,
  ShikihoTraceField,
  ShikihoTraceMode,
  ShikihoTraceOutcome,
  ShikihoTracePhase,
  ShikihoWaitEndReason,
} from './contract';
import type { ShikihoExtractionResult, ShikihoPageInspection } from './extractor';

export const SHIKIHO_SAMPLE_DEBOUNCE_MS = 250;
export const SHIKIHO_SAMPLE_MAX_INTERVAL_MS = 1_000;
export const SHIKIHO_FIELD_STABLE_MS = 500;

type TimerHandle = number;
type SampleReason = 'initial' | 'mutation' | 'forced' | 'stable' | 'deadline';

interface ObserverHandle {
  disconnect(): void;
}

export interface ProgressiveCaptureRequest {
  attemptId: string;
  code: string;
  mode: ShikihoTraceMode;
  deadlineMs: number;
  receiverAttempts: number;
  receiverReadyMs: number;
  startedAtMs?: number;
  probeMs?: number;
  acquisitionMs?: number;
  receiverMs?: number;
}

export interface ProgressiveCaptureResult {
  result: ShikihoExtractionResult;
  trace: ShikihoCaptureTraceV1;
}

export class ProgressiveCaptureCancelledError extends Error {
  readonly reason = 'navigation_changed' as const;

  constructor() {
    super('Shikiho progressive capture was cancelled because the page navigation changed');
    this.name = 'ProgressiveCaptureCancelledError';
  }
}

export interface ProgressiveNavigationTiming {
  responseStartMs: number | null;
  domInteractiveMs: number | null;
  domContentLoadedMs: number | null;
  loadEndMs: number | null;
}

export interface ProgressiveCaptureOptions {
  now(): number;
  setTimeout(callback: () => void, delay: number): TimerHandle;
  clearTimeout(handle: TimerHandle): void;
  observe(callback: () => void): ObserverHandle;
  getCode(): string | null;
  getReadyState(): DocumentReadyState | null;
  getNavigationTiming(): ProgressiveNavigationTiming;
  inspect(): ShikihoPageInspection;
  onProgress(progress: ShikihoCaptureProgressV1): void;
}

export interface ProgressiveShikihoCapture {
  run(request: ProgressiveCaptureRequest): Promise<ProgressiveCaptureResult>;
  stop(): void;
}

const TRACE_FIELDS: readonly ShikihoTraceField[] = [
  'identity',
  'quote',
  'features',
  'consolidatedBusinesses',
  'commentary',
  'score',
  'comparisonCompanies',
  'industries',
  'marketThemes',
  'profile',
  'editionLabel',
  'pageUpdatedAt',
  'coreReady',
];

function emptyMilestones(): ShikihoFieldMilestonesV1 {
  return {
    identity: null,
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
    pageUpdatedAt: null,
    coreReady: null,
  };
}

function finiteNonnegative(value: number, fallback = 0): number {
  if (!Number.isFinite(value) || value < 0) return fallback;
  return Math.min(value, Number.MAX_SAFE_INTEGER);
}

function finiteInteger(value: number): number {
  return Math.floor(finiteNonnegative(value));
}

function presentFields(snapshot: ShikihoSnapshotV1): ShikihoTraceField[] {
  const fields: ShikihoTraceField[] = ['identity'];
  if (snapshot.quote !== undefined) fields.push('quote');
  if (snapshot.features !== null) fields.push('features');
  if (snapshot.consolidatedBusinesses !== null) fields.push('consolidatedBusinesses');
  if (snapshot.commentary.length > 0) fields.push('commentary');
  if (Object.values(snapshot.score).some((value) => value !== null)) fields.push('score');
  if (snapshot.comparisonCompanies.length > 0) fields.push('comparisonCompanies');
  if (snapshot.industries.length > 0) fields.push('industries');
  if (snapshot.marketThemes.length > 0) fields.push('marketThemes');
  if (snapshot.profile.length > 0) fields.push('profile');
  if (snapshot.editionLabel !== null) fields.push('editionLabel');
  if (snapshot.pageUpdatedAt !== null) fields.push('pageUpdatedAt');
  if (snapshot.features !== null && snapshot.consolidatedBusinesses !== null && snapshot.commentary.length > 0) {
    fields.push('coreReady');
  }
  return fields;
}

function fingerprint(snapshot: ShikihoSnapshotV1, fields: readonly ShikihoTraceField[]): string {
  return `${snapshot.contentHash}\u0000${fields.join('\u0000')}`;
}

export function createProgressiveShikihoCapture(options: ProgressiveCaptureOptions): ProgressiveShikihoCapture {
  let activeCancel: (() => void) | null = null;

  function run(request: ProgressiveCaptureRequest): Promise<ProgressiveCaptureResult> {
    activeCancel?.();
    const rawStartedAt = request.startedAtMs ?? options.now();
    const startedAt = Number.isFinite(rawStartedAt) ? rawStartedAt : Date.now();
    const contentStartedAt = finiteNonnegative(options.now(), startedAt);
    const deadline = Math.max(startedAt, finiteNonnegative(request.deadlineMs, startedAt));
    const timers = new Set<TimerHandle>();
    let observer: ObserverHandle | null = null;
    let resolveRun: (result: ProgressiveCaptureResult) => void = () => undefined;
    let rejectRun: (error: unknown) => void = () => undefined;
    let settled = false;
    let debounceTimer: TimerHandle | null = null;
    let forcedTimer: TimerHandle | null = null;
    let stableTimer: TimerHandle | null = null;
    let sequence = 0;
    let mutationBatches = 0;
    let meaningfulChanges = 0;
    let samples = 0;
    let extractionSamples = 0;
    let extractionLastMs: number | null = null;
    let extractionMaxMs: number | null = null;
    let extractionTotalMs = 0;
    let firstSampleMs: number | null = null;
    let latestFields: ShikihoTraceField[] = [];
    const everSeenFields = new Set<ShikihoTraceField>();
    const firstSeenMs = emptyMilestones();
    let latestResult: ShikihoExtractionResult | null = null;
    let latestCandidate: ShikihoSnapshotV1 | null = null;
    let latestFingerprint: string | null = null;
    let stableSinceMs: number | null = null;
    let lastSampleAt = startedAt;
    let phase: ShikihoTracePhase = 'observing_dom';

    function now(): number {
      const value = options.now();
      return Number.isFinite(value) ? value : startedAt;
    }

    function elapsed(at = now()): number {
      return finiteNonnegative(at - startedAt);
    }

    function schedule(callback: () => void, delay: number): TimerHandle {
      let handle = 0;
      handle = options.setTimeout(() => {
        timers.delete(handle);
        callback();
      }, finiteNonnegative(delay));
      timers.add(handle);
      return handle;
    }

    function cancelTimer(handle: TimerHandle | null): void {
      if (handle === null) return;
      options.clearTimeout(handle);
      timers.delete(handle);
    }

    function cleanup(): void {
      observer?.disconnect();
      observer = null;
      for (const timer of timers) options.clearTimeout(timer);
      timers.clear();
      debounceTimer = null;
      forcedTimer = null;
      stableTimer = null;
      if (activeCancel === cancel) activeCancel = null;
    }

    function navigation(): ProgressiveNavigationTiming {
      const timing = options.getNavigationTiming();
      const metric = (value: number | null): number | null => (value === null ? null : finiteNonnegative(value, 0));
      return {
        responseStartMs: metric(timing.responseStartMs),
        domInteractiveMs: metric(timing.domInteractiveMs),
        domContentLoadedMs: metric(timing.domContentLoadedMs),
        loadEndMs: metric(timing.loadEndMs),
      };
    }

    function trace(
      tracePhase = phase,
      outcome: ShikihoTraceOutcome | null = null,
      waitEndReason: ShikihoWaitEndReason | null = null
    ): ShikihoCaptureTraceV1 {
      const observedAt = now();
      return {
        schemaVersion: 1,
        attemptId: request.attemptId,
        code: request.code,
        mode: request.mode,
        phase: tracePhase,
        startedAt: new Date(startedAt).toISOString(),
        updatedAt: new Date(observedAt).toISOString(),
        outcome,
        waitEndReason,
        receiverAttempts: finiteInteger(request.receiverAttempts),
        receiverReadyMs: finiteNonnegative(request.receiverReadyMs),
        documentReadyState: options.getReadyState(),
        navigation: navigation(),
        dom: {
          firstSampleMs,
          mutationBatches,
          meaningfulChanges,
          samples,
          presentFields: [...latestFields],
          missingFields: TRACE_FIELDS.filter((field) => !latestFields.includes(field)),
          firstSeenMs: { ...firstSeenMs },
        },
        extraction: {
          samples: extractionSamples,
          lastMs: extractionLastMs,
          maxMs: extractionMaxMs,
          totalMs: extractionTotalMs,
        },
        timings: {
          probeMs: finiteNonnegative(request.probeMs ?? 0),
          acquisitionMs: finiteNonnegative(request.acquisitionMs ?? 0),
          receiverMs: finiteNonnegative(request.receiverMs ?? request.receiverReadyMs),
          domObservationMs: finiteNonnegative(observedAt - contentStartedAt),
          storageMs: 0,
          totalMs: elapsed(observedAt),
        },
      };
    }

    function finish(
      result: ShikihoExtractionResult,
      terminalPhase: ShikihoTracePhase,
      outcome: ShikihoTraceOutcome,
      reason: ShikihoWaitEndReason
    ): void {
      if (settled) return;
      settled = true;
      phase = terminalPhase;
      const terminalTrace = trace(terminalPhase, outcome, reason);
      cleanup();
      resolveRun({ result, trace: terminalTrace });
    }

    function cancel(): void {
      if (settled) return;
      settled = true;
      cleanup();
      rejectRun(new ProgressiveCaptureCancelledError());
    }

    function emit(candidate: ShikihoSnapshotV1 | null): boolean {
      sequence += 1;
      try {
        options.onProgress({
          schemaVersion: 1,
          attemptId: request.attemptId,
          code: request.code,
          sequence,
          candidate,
          trace: trace(),
        });
        return true;
      } catch (error) {
        settled = true;
        cleanup();
        rejectRun(error);
        return false;
      }
    }

    function scheduleStableCheck(): void {
      if (stableSinceMs === null || stableTimer !== null) return;
      stableTimer = schedule(
        () => {
          stableTimer = null;
          sample('stable');
        },
        stableSinceMs + SHIKIHO_FIELD_STABLE_MS - now()
      );
    }

    function recordFields(fields: ShikihoTraceField[], seenAt: number): void {
      for (const field of fields) {
        if (firstSeenMs[field] === null) firstSeenMs[field] = seenAt;
      }
      latestFields = fields;
    }

    function recordExtraction(sampleStartedAt: number): void {
      const extractionMs = finiteNonnegative(now() - sampleStartedAt);
      extractionSamples += 1;
      extractionLastMs = extractionMs;
      extractionMaxMs = Math.max(extractionMaxMs ?? 0, extractionMs);
      extractionTotalMs = finiteNonnegative(extractionTotalMs + extractionMs);
    }

    function handleCoreSnapshot(
      result: Extract<ShikihoExtractionResult, { kind: 'success' }>,
      changed: boolean,
      reason: SampleReason,
      sampleStartedAt: number
    ): boolean {
      if (changed || stableSinceMs === null) stableSinceMs = sampleStartedAt;
      phase = 'settling';
      if (reason === 'stable' && now() - stableSinceMs >= SHIKIHO_FIELD_STABLE_MS) {
        finish(result, 'complete', 'success', 'field_stable');
        return true;
      }
      scheduleStableCheck();
      return false;
    }

    function recordFingerprint(nextFingerprint: string): boolean {
      const changed = nextFingerprint !== latestFingerprint;
      if (changed) {
        meaningfulChanges += 1;
        latestFingerprint = nextFingerprint;
      }
      return changed;
    }

    function dispatchSnapshotProgress(
      snapshot: ShikihoSnapshotV1,
      coverageAdvanced: boolean,
      changed: boolean
    ): boolean {
      if (coverageAdvanced) return emit(snapshot);
      if (changed) return emit(null);
      return true;
    }

    function handleCandidate(
      candidate: ShikihoSnapshotV1,
      inspectedFields: ShikihoTraceField[],
      canonicalResult: Extract<ShikihoExtractionResult, { kind: 'success' }> | null,
      reason: SampleReason,
      sampleStartedAt: number,
      sampleElapsed: number
    ): boolean {
      const fields = [...new Set([...inspectedFields, ...presentFields(candidate)])];
      const nextFingerprint = fingerprint(candidate, fields);
      const changed = recordFingerprint(nextFingerprint);
      const coverageAdvanced = fields.some((field) => !everSeenFields.has(field));
      recordFields(fields, sampleElapsed);
      for (const field of fields) everSeenFields.add(field);
      latestCandidate = candidate;
      const coreReady = canonicalResult !== null && fields.includes('coreReady');
      phase = coreReady ? 'core_ready' : 'core_partial';
      if (!dispatchSnapshotProgress(candidate, coverageAdvanced, changed)) return true;
      if (coreReady) return handleCoreSnapshot(canonicalResult, changed, reason, sampleStartedAt);

      stableSinceMs = null;
      cancelTimer(stableTimer);
      stableTimer = null;
      return false;
    }

    function inspectOnce(sampleStartedAt: number): ShikihoPageInspection | null {
      try {
        const inspection = options.inspect();
        recordExtraction(sampleStartedAt);
        return inspection;
      } catch (error) {
        settled = true;
        cleanup();
        rejectRun(error);
        return null;
      }
    }

    function cancelForNavigationChange(): boolean {
      if (options.getCode() !== request.code) {
        cancel();
        return true;
      }
      return false;
    }

    function beginSample(): { startedAt: number; elapsed: number } {
      cancelTimer(debounceTimer);
      cancelTimer(forcedTimer);
      debounceTimer = null;
      forcedTimer = null;
      const startedAt = now();
      lastSampleAt = startedAt;
      const sampleElapsed = elapsed(startedAt);
      if (firstSampleMs === null) firstSampleMs = sampleElapsed;
      samples += 1;
      return { startedAt, elapsed: sampleElapsed };
    }

    function handleSampleResult(
      inspection: ShikihoPageInspection,
      reason: SampleReason,
      current: { startedAt: number; elapsed: number }
    ): boolean {
      latestResult = inspection.result;
      const fields = [...new Set(inspection.fields)].filter((field) => TRACE_FIELDS.includes(field));
      if (inspection.candidate !== null) {
        const canonicalResult = inspection.result.kind === 'success' ? inspection.result : null;
        return handleCandidate(
          inspection.candidate,
          fields,
          canonicalResult,
          reason,
          current.startedAt,
          current.elapsed
        );
      }
      const coverageAdvanced = fields.some((field) => !everSeenFields.has(field));
      recordFields(fields, current.elapsed);
      for (const field of fields) everSeenFields.add(field);
      return coverageAdvanced && !emit(null);
    }

    function cannotSample(): boolean {
      return settled || cancelForNavigationChange();
    }

    function reachedDeadline(reason: SampleReason): boolean {
      return reason === 'deadline' || now() >= deadline;
    }

    function sample(reason: SampleReason): void {
      if (cannotSample()) return;
      const current = beginSample();
      const inspection = inspectOnce(current.startedAt);
      if (inspection === null) return;
      if (handleSampleResult(inspection, reason, current)) return;
      if (reachedDeadline(reason)) finishAtDeadline();
    }

    function finishAtDeadline(): void {
      if (settled) return;
      if (cancelForNavigationChange()) return;
      if (latestCandidate !== null) {
        const partial: ShikihoSnapshotV1 = { ...latestCandidate, status: 'partial' };
        finish({ kind: 'success', snapshot: partial }, 'complete', 'partial', 'deadline');
        return;
      }
      if (latestResult?.kind === 'login_required') {
        finish(latestResult, 'error', 'login_required', 'login_confirmed');
        return;
      }
      const pageChanged: ShikihoExtractionResult = { kind: 'page_changed', code: request.code };
      finish(pageChanged, 'error', 'page_changed', 'deadline');
    }

    function mutation(): void {
      if (settled) return;
      mutationBatches += 1;
      cancelTimer(debounceTimer);
      debounceTimer = schedule(() => {
        debounceTimer = null;
        sample('mutation');
      }, SHIKIHO_SAMPLE_DEBOUNCE_MS);
      if (forcedTimer === null) {
        forcedTimer = schedule(
          () => {
            forcedTimer = null;
            sample('forced');
          },
          lastSampleAt + SHIKIHO_SAMPLE_MAX_INTERVAL_MS - now()
        );
      }
    }

    const promise = new Promise<ProgressiveCaptureResult>((resolve, reject) => {
      resolveRun = resolve;
      rejectRun = reject;
    });
    activeCancel = cancel;
    observer = options.observe(mutation);
    schedule(() => sample('deadline'), deadline - now());
    sample('initial');
    return promise;
  }

  return {
    run,
    stop(): void {
      activeCancel?.();
    },
  };
}
