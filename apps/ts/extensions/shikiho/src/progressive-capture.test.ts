import { describe, expect, mock, test } from 'bun:test';
import type { ShikihoCaptureProgressV1, ShikihoSnapshotV1, ShikihoTraceMode } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import {
  createProgressiveShikihoCapture,
  ProgressiveCaptureCancelledError,
  type ProgressiveCaptureRequest,
  SHIKIHO_FIELD_STABLE_MS,
  SHIKIHO_SAMPLE_DEBOUNCE_MS,
  SHIKIHO_SAMPLE_MAX_INTERVAL_MS,
} from './progressive-capture';

class FakeScheduler {
  now = Date.parse('2026-07-14T00:00:00.000Z');
  private nextId = 1;
  private tasks = new Map<number, { at: number; callback: () => void }>();

  setTimeout = (callback: () => void, delay: number): number => {
    const id = this.nextId++;
    this.tasks.set(id, { at: this.now + Math.max(0, delay), callback });
    return id;
  };

  clearTimeout = (id: number): void => {
    this.tasks.delete(id);
  };

  advanceToElapsed(elapsed: number): void {
    const target = Date.parse('2026-07-14T00:00:00.000Z') + elapsed;
    while (true) {
      const next = [...this.tasks.entries()]
        .filter(([, task]) => task.at <= target)
        .sort((left, right) => left[1].at - right[1].at || left[0] - right[0])[0];
      if (next === undefined) break;
      this.tasks.delete(next[0]);
      this.now = next[1].at;
      next[1].callback();
    }
    this.now = target;
  }

  pending(): number {
    return this.tasks.size;
  }
}

function snapshot(overrides: Partial<ShikihoSnapshotV1> = {}): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: 'test',
    code: '7203',
    companyName: 'Toyota',
    sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
    capturedAt: '2026-07-14T00:00:00.000Z',
    pageUpdatedAt: null,
    editionLabel: null,
    contentHash: 'hash-empty',
    status: 'partial',
    features: null,
    consolidatedBusinesses: null,
    commentary: [],
    score: {
      overall: null,
      growth: null,
      profitability: null,
      safety: null,
      scale: null,
      value: null,
      priceMomentum: null,
    },
    comparisonCompanies: [],
    industries: [],
    marketThemes: [],
    profile: [],
    missingFields: ['features', 'consolidatedBusinesses', 'commentary'],
    ...overrides,
  };
}

const partialFeatures: ShikihoExtractionResult = {
  kind: 'success',
  snapshot: snapshot({ features: 'features', contentHash: 'hash-features' }),
};
const completeCore: ShikihoExtractionResult = {
  kind: 'success',
  snapshot: snapshot({
    status: 'captured',
    features: 'features',
    consolidatedBusinesses: 'businesses',
    commentary: [{ heading: 'outlook', body: 'body' }],
    contentHash: 'hash-core',
    missingFields: [],
  }),
};

function request(code = '7203', mode: ShikihoTraceMode = 'new_owned_tab'): ProgressiveCaptureRequest {
  return {
    attemptId: 'attempt-1',
    code,
    mode,
    deadlineMs: Date.parse('2026-07-14T00:00:25.000Z'),
    receiverAttempts: 2,
    receiverReadyMs: 125,
  };
}

function harness(samples: ShikihoExtractionResult[]) {
  const scheduler = new FakeScheduler();
  let mutationCallback: () => void = () => undefined;
  let code = '7203';
  let sampleIndex = 0;
  const sampleTimes: number[] = [];
  const disconnect = mock(() => undefined);
  const progressEvents: ShikihoCaptureProgressV1[] = [];
  const progress = mock((event: ShikihoCaptureProgressV1): void => {
    progressEvents.push(event);
  });
  const capture = createProgressiveShikihoCapture({
    now: () => scheduler.now,
    setTimeout: scheduler.setTimeout,
    clearTimeout: scheduler.clearTimeout,
    observe: (callback) => {
      mutationCallback = callback;
      return { disconnect };
    },
    getCode: () => code,
    getReadyState: () => 'interactive',
    getNavigationTiming: () => ({
      responseStartMs: 50,
      domInteractiveMs: 100,
      domContentLoadedMs: 120,
      loadEndMs: null,
    }),
    extract: () => {
      sampleTimes.push(scheduler.now - Date.parse('2026-07-14T00:00:00.000Z'));
      const result = samples[Math.min(sampleIndex, samples.length - 1)] as ShikihoExtractionResult;
      sampleIndex += 1;
      scheduler.now += 3;
      return result;
    },
    onProgress: progress,
  });

  return {
    capture,
    scheduler,
    disconnect,
    progress,
    setCode(value: string) {
      code = value;
    },
    fireMutationAt(elapsed: number) {
      scheduler.advanceToElapsed(elapsed);
      mutationCallback();
    },
    progressCandidates() {
      return progressEvents.map((event) => event.candidate).filter((candidate) => candidate !== null);
    },
    progressEvents,
    sampleTimes,
  };
}

describe('progressive Shikiho capture', () => {
  test('samples immediately and emits a provisional candidate when recognizable coverage advances', () => {
    const h = harness([partialFeatures]);

    void h.capture.run(request());

    expect(h.progress).toHaveBeenCalledTimes(1);
    expect(h.progressEvents[0]).toMatchObject({
      sequence: 1,
      candidate: { features: 'features', status: 'partial' },
      trace: {
        phase: 'core_partial',
        dom: { firstSampleMs: 0, presentFields: ['identity', 'features'] },
      },
    });
  });

  test('debounces mutations by 250ms and forces at most one sample per 1000ms', () => {
    const h = harness([partialFeatures]);
    void h.capture.run(request());

    for (let elapsed = 100; elapsed <= 1_100; elapsed += 100) h.fireMutationAt(elapsed);
    expect(SHIKIHO_SAMPLE_DEBOUNCE_MS).toBe(250);
    expect(SHIKIHO_SAMPLE_MAX_INTERVAL_MS).toBe(1_000);
    expect(h.progressEvents[0]?.trace.dom.samples).toBe(1);
    h.scheduler.advanceToElapsed(1_350);

    expect(h.sampleTimes).toEqual([0, 1_000, 1_350]);
    expect(h.progress).toHaveBeenCalledTimes(1);
  });

  test('does not emit progress when only mutation count changes and the field fingerprint does not', () => {
    const h = harness([partialFeatures]);
    void h.capture.run(request());

    h.fireMutationAt(100);
    h.scheduler.advanceToElapsed(350);

    expect(h.progress).toHaveBeenCalledTimes(1);
  });

  test('writes first-seen milestones once when fields remain present', async () => {
    const later = { ...completeCore, snapshot: { ...completeCore.snapshot, contentHash: 'hash-core-updated' } };
    const h = harness([partialFeatures, completeCore, later]);
    const running = h.capture.run(request());
    h.fireMutationAt(100);
    h.scheduler.advanceToElapsed(350);
    h.fireMutationAt(400);
    h.scheduler.advanceToElapsed(1_200);

    const terminal = await running;
    const traces = h.progressEvents.map((event) => event.trace);
    expect(traces.at(-1)?.dom.firstSeenMs.features).toBe(0);
    expect(terminal.trace.dom.firstSeenMs.features).toBe(0);
    expect(terminal.trace.dom.firstSeenMs.coreReady).toBe(350);
  });

  test('continuous unrelated mutations do not delay a stable core capture', async () => {
    const h = harness([partialFeatures, completeCore, completeCore]);
    const running = h.capture.run(request());
    h.fireMutationAt(100);
    h.fireMutationAt(200);
    h.scheduler.advanceToElapsed(1_000);

    await expect(running).resolves.toMatchObject({
      result: { kind: 'success', snapshot: { status: 'captured' } },
      trace: { phase: 'complete', outcome: 'success', waitEndReason: 'field_stable' },
    });
    expect(SHIKIHO_FIELD_STABLE_MS).toBe(500);
    expect(h.progressCandidates()).toHaveLength(2);
  });

  test('returns a recognizable partial candidate at the deadline', async () => {
    const h = harness([partialFeatures]);
    const running = h.capture.run(request());
    h.scheduler.advanceToElapsed(25_000);

    await expect(running).resolves.toMatchObject({
      result: { kind: 'success', snapshot: { status: 'partial' } },
      trace: { phase: 'complete', outcome: 'partial', waitEndReason: 'deadline' },
    });
  });

  test('returns login_required only after the deadline confirms a stable login marker', async () => {
    const login: ShikihoExtractionResult = { kind: 'login_required', code: '7203' };
    const h = harness([login]);
    const running = h.capture.run(request());
    expect(h.progress).not.toHaveBeenCalled();
    h.scheduler.advanceToElapsed(25_000);

    await expect(running).resolves.toMatchObject({
      result: login,
      trace: { phase: 'error', outcome: 'login_required', waitEndReason: 'login_confirmed' },
    });
  });

  test('cancels code replacement without returning a page_changed diagnostic', async () => {
    const h = harness([partialFeatures]);
    const running = h.capture.run(request());
    h.setCode('6758');
    h.fireMutationAt(100);
    h.scheduler.advanceToElapsed(350);

    await expect(running).rejects.toBeInstanceOf(ProgressiveCaptureCancelledError);
  });

  test('keeps extraction timings finite and nonnegative', async () => {
    const h = harness([partialFeatures]);
    const running = h.capture.run(request());
    h.fireMutationAt(100);
    h.scheduler.advanceToElapsed(350);
    h.scheduler.advanceToElapsed(25_000);

    const { trace } = await running;
    expect(trace.extraction.samples).toBeGreaterThanOrEqual(2);
    expect(Number.isFinite(trace.extraction.lastMs)).toBe(true);
    expect(Number.isFinite(trace.extraction.maxMs)).toBe(true);
    expect(Number.isFinite(trace.extraction.totalMs)).toBe(true);
    expect(trace.extraction.lastMs).toBeGreaterThanOrEqual(0);
    expect(trace.extraction.maxMs).toBeGreaterThanOrEqual(0);
    expect(trace.extraction.totalMs).toBeGreaterThanOrEqual(0);
  });

  test('stop disconnects the observer and cancels every timer', async () => {
    const h = harness([partialFeatures]);
    const running = h.capture.run(request());
    expect(h.scheduler.pending()).toBeGreaterThan(0);

    h.capture.stop();

    await expect(running).rejects.toBeInstanceOf(ProgressiveCaptureCancelledError);
    expect(h.disconnect).toHaveBeenCalledTimes(1);
    expect(h.scheduler.pending()).toBe(0);
  });
});
