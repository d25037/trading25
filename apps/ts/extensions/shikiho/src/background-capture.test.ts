import { afterEach, describe, expect, mock, test } from 'bun:test';
import {
  type BackgroundCaptureDeps,
  createBackgroundCaptureCoordinator,
  resolvePublicShikihoState,
  SHIKIHO_CACHE_TTL_MS,
  SHIKIHO_QUOTE_TTL_MS,
  SHIKIHO_RETRY_SUPPRESSION_MS,
  type StoredShikihoState,
} from './background-capture';
import type { ShikihoCaptureDiagnosticV1, ShikihoCaptureTraceV1, ShikihoSnapshotV1 } from './contract';
import type { ShikihoExtractionResult } from './extractor';
import type { AcquiredShikihoResult } from './tab-acquisition';

const NOW = Date.parse('2026-07-12T12:00:00.000Z');

function snapshot(code: string, ageMs = 0, quoteAgeMs: number | null = ageMs): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: 'test',
    code,
    companyName: null,
    sourceUrl: `https://shikiho.toyokeizai.net/stocks/${code}`,
    capturedAt: new Date(NOW - ageMs).toISOString(),
    pageUpdatedAt: null,
    editionLabel: null,
    contentHash: `hash-${code}-${ageMs}`,
    status: 'captured',
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
    ...(quoteAgeMs === null
      ? {}
      : {
          quote: {
            tradingDate: '2026-07-12',
            observedAt: new Date(NOW - quoteAgeMs).toISOString(),
            delayMinutes: 15 as const,
            currentPrice: 102,
            open: 100,
            high: 105,
            low: 98,
            previousClose: 99,
            volume: 12_300,
            openTime: '09:00',
            highTime: '13:20',
            lowTime: null,
            sourceLabel: '会社四季報オンライン' as const,
          },
        }),
    missingFields: [],
  };
}

function quote(ageMs = 1): NonNullable<ShikihoSnapshotV1['quote']> {
  const value = snapshot('7203', 1, ageMs).quote;
  if (value === undefined) throw new Error('expected quote fixture');
  return value;
}

function diagnostic(code: string, ageMs = 0): ShikihoCaptureDiagnosticV1 {
  return { schemaVersion: 1, code, observedAt: new Date(NOW - ageMs).toISOString(), status: 'login_required' };
}

function acquired(result: ShikihoExtractionResult): AcquiredShikihoResult {
  return {
    result,
    trace: captureTrace(result.kind === 'success' ? result.snapshot.code : result.code),
    timing: {
      event: 'shikiho_capture_timing',
      mode: 'exact_user_tab',
      outcome: result.kind === 'success' ? 'success' : 'diagnostic',
      probeMs: 0,
      navigationMs: 0,
      captureMs: 0,
      totalMs: 0,
    },
  };
}

function captureTrace(code = '7203'): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId: 'attempt-1',
    code,
    mode: 'new_owned_tab',
    phase: 'complete',
    startedAt: '2026-07-12T12:00:00.000Z',
    updatedAt: '2026-07-12T12:00:01.000Z',
    outcome: 'success',
    waitEndReason: 'field_stable',
    receiverAttempts: 1,
    receiverReadyMs: 0,
    documentReadyState: 'complete',
    navigation: { responseStartMs: null, domInteractiveMs: null, domContentLoadedMs: null, loadEndMs: null },
    dom: {
      firstSampleMs: 0,
      mutationBatches: 0,
      meaningfulChanges: 0,
      samples: 1,
      presentFields: ['identity'],
      missingFields: [],
      firstSeenMs: {
        identity: 0,
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
      },
    },
    extraction: { samples: 1, lastMs: 0, maxMs: 0, totalMs: 0 },
    timings: { probeMs: 0, acquisitionMs: 0, receiverMs: 0, domObservationMs: 1, storageMs: 0, totalMs: 1 },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function waitForCalls(fn: unknown, count: number): Promise<void> {
  const mocked = fn as unknown as { mock: { calls: unknown[][] } };
  for (let attempt = 0; attempt < 20 && mocked.mock.calls.length < count; attempt += 1) await Promise.resolve();
  expect(mocked.mock.calls.length).toBe(count);
}

function harness(
  initial: Record<string, StoredShikihoState> = {},
  captureImpl: (code: string) => Promise<AcquiredShikihoResult> = async (code) =>
    acquired({ kind: 'success', snapshot: snapshot(code) })
) {
  const states = new Map(Object.entries(initial));
  const deps: BackgroundCaptureDeps = {
    now: () => NOW,
    get: mock(async (code: string) => states.get(code) ?? { snapshot: null, diagnostic: null }),
    getTrace: mock(async (code: string) => states.get(code)?.trace ?? null),
    saveSnapshot: mock(async (value: ShikihoSnapshotV1) => {
      states.set(value.code, { snapshot: value, diagnostic: null });
    }),
    saveDiagnostic: mock(async (value: ShikihoCaptureDiagnosticV1) => {
      states.set(value.code, { snapshot: states.get(value.code)?.snapshot ?? null, diagnostic: value });
    }),
    capture: mock(captureImpl),
  };
  return { deps, states, coordinator: createBackgroundCaptureCoordinator(deps) };
}

afterEach(() => mock.restore());

describe('background capture freshness', () => {
  test('hides an older diagnostic after a newer successful observation of unchanged content', async () => {
    const storedSnapshot = snapshot('7203', 60_000, 15 * 60_000);
    const staleDiagnostic = diagnostic('7203', 30_000);
    const successfulObservedAt = new Date(NOW - 1_000).toISOString();
    const state: StoredShikihoState = {
      snapshot: storedSnapshot,
      diagnostic: staleDiagnostic,
      successfulObservedAt,
    };

    expect(await resolvePublicShikihoState(async () => state, '7203', false)).toEqual({
      snapshot: { ...storedSnapshot, capturedAt: successfulObservedAt },
      diagnostic: null,
      trace: null,
    });
  });

  test('publishes the latest successful observation as the effective capture time for unchanged content', async () => {
    const storedSnapshot = snapshot('7203', SHIKIHO_QUOTE_TTL_MS + 1, 15 * 60_000);
    const successfulObservedAt = new Date(NOW - 1).toISOString();

    const publicState = await resolvePublicShikihoState(
      async () => ({ snapshot: storedSnapshot, diagnostic: null, successfulObservedAt }),
      '7203',
      false
    );

    expect(publicState.snapshot?.capturedAt).toBe(successfulObservedAt);
    expect(publicState.snapshot?.contentHash).toBe(storedSnapshot.contentHash);
  });

  test('normalizes a parseable legacy observation before publishing it as a contract timestamp', async () => {
    const storedSnapshot = snapshot('7203', 60_000, 15 * 60_000);
    const successfulObservedAt = 'July 12, 2026 11:59:59 GMT';

    const publicState = await resolvePublicShikihoState(
      async () => ({ snapshot: storedSnapshot, diagnostic: null, successfulObservedAt }),
      '7203',
      false
    );

    expect(publicState.snapshot?.capturedAt).toBe('2026-07-12T11:59:59.000Z');
  });

  test('uses the persisted successful observation time without rewriting capturedAt', async () => {
    const oldSnapshot = snapshot('7203', SHIKIHO_CACHE_TTL_MS + 1, 1);
    const { coordinator, deps } = harness({
      '7203': { snapshot: oldSnapshot, diagnostic: null, successfulObservedAt: new Date(NOW - 1).toISOString() },
    });

    expect(await coordinator.resolve('7203', false)).toEqual({
      snapshot: oldSnapshot,
      diagnostic: null,
      successfulObservedAt: new Date(NOW - 1).toISOString(),
      trace: null,
    });
    expect(deps.capture).toHaveBeenCalledTimes(0);
  });

  test('returns a snapshot one millisecond inside the TTL without creating a tab', async () => {
    const fresh = snapshot('7203', SHIKIHO_CACHE_TTL_MS - 1, 1);
    const successfulObservedAt = new Date(NOW - 1).toISOString();
    const { coordinator, deps } = harness({
      '7203': { snapshot: fresh, diagnostic: null, successfulObservedAt },
    });

    expect(await coordinator.resolve('7203', false)).toEqual({
      snapshot: fresh,
      diagnostic: null,
      successfulObservedAt,
      trace: null,
    });
    expect(deps.capture).toHaveBeenCalledTimes(0);
  });

  test('refreshes a snapshot exactly at the TTL boundary', async () => {
    const stale = snapshot('7203', SHIKIHO_CACHE_TTL_MS);
    const { coordinator, deps } = harness({ '7203': { snapshot: stale, diagnostic: null } });

    await expect(coordinator.resolve('7203', false)).resolves.toEqual({
      snapshot: snapshot('7203'),
      diagnostic: null,
      trace: null,
    });
    expect(deps.capture).toHaveBeenCalledWith('7203');
  });

  test('uses the successful capture time for quote TTL instead of the delayed market timestamp', async () => {
    const delayed = snapshot('7203', 1, SHIKIHO_QUOTE_TTL_MS);
    const insideHarness = harness({
      '7203': {
        snapshot: delayed,
        diagnostic: null,
        successfulObservedAt: new Date(NOW - SHIKIHO_QUOTE_TTL_MS + 1).toISOString(),
      },
    });
    expect(await insideHarness.coordinator.resolve('7203', false)).toEqual({
      snapshot: delayed,
      diagnostic: null,
      successfulObservedAt: new Date(NOW - SHIKIHO_QUOTE_TTL_MS + 1).toISOString(),
      trace: null,
    });
    expect(insideHarness.deps.capture).toHaveBeenCalledTimes(0);

    const boundary = snapshot('7203', 1, 1);
    const boundaryHarness = harness({
      '7203': {
        snapshot: boundary,
        diagnostic: null,
        successfulObservedAt: new Date(NOW - SHIKIHO_QUOTE_TTL_MS).toISOString(),
      },
    });
    await boundaryHarness.coordinator.resolve('7203', false);
    expect(boundaryHarness.deps.capture).toHaveBeenCalledTimes(1);
  });

  test('refreshes a fresh article when its quote is missing, from another JST date, or in the future', async () => {
    const cases = [
      snapshot('7203', 1, null),
      {
        ...snapshot('7203', 1, 1),
        quote: { ...quote(), tradingDate: '2026-07-11', observedAt: '2026-07-11T20:59:59.999+09:00' },
      },
      { ...snapshot('7203', 1, 1), quote: { ...quote(), observedAt: new Date(NOW + 1).toISOString() } },
    ];

    for (const storedSnapshot of cases) {
      const { coordinator, deps } = harness({ '7203': { snapshot: storedSnapshot, diagnostic: null } });
      await coordinator.resolve('7203', false);
      expect(deps.capture).toHaveBeenCalledTimes(1);
    }
  });

  test('recent diagnostics suppress automatic quote retry while preserving the stored article', async () => {
    const article = snapshot('7203', 1, null);
    const recent = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS - 1);
    const { coordinator, deps } = harness({ '7203': { snapshot: article, diagnostic: recent } });

    expect(await coordinator.resolve('7203', false)).toEqual({ snapshot: article, diagnostic: recent, trace: null });
    expect(deps.capture).toHaveBeenCalledTimes(0);
  });

  test('preserves the prior article when a quote-only refresh ends in a diagnostic', async () => {
    const article = snapshot('7203', 1, null);
    const { coordinator, deps } = harness({ '7203': { snapshot: article, diagnostic: null } }, async (code) =>
      acquired({ kind: 'login_required', code })
    );

    expect(await coordinator.resolve('7203', false)).toEqual({
      snapshot: article,
      diagnostic: diagnostic('7203'),
      trace: null,
    });
    expect(deps.saveDiagnostic).toHaveBeenCalledWith(diagnostic('7203'));
  });

  test('suppresses automatic retry for a diagnostic inside 60 seconds', async () => {
    const recent = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS - 1);
    const { coordinator, deps } = harness({ '7203': { snapshot: null, diagnostic: recent } });

    expect(await coordinator.resolve('7203', false)).toEqual({ snapshot: null, diagnostic: recent, trace: null });
    expect(deps.capture).toHaveBeenCalledTimes(0);
  });

  test('retries a diagnostic exactly at the 60 second boundary', async () => {
    const boundary = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS);
    const { coordinator, deps } = harness({ '7203': { snapshot: null, diagnostic: boundary } });

    await coordinator.resolve('7203', false);
    expect(deps.capture).toHaveBeenCalledTimes(1);
  });

  test('manual refresh bypasses both a fresh snapshot and a recent diagnostic', async () => {
    for (const state of [
      { snapshot: snapshot('7203', 1), diagnostic: null },
      { snapshot: null, diagnostic: diagnostic('7203', 1) },
    ]) {
      const { coordinator, deps } = harness({ '7203': state });
      await coordinator.resolve('7203', true);
      expect(deps.capture).toHaveBeenCalledTimes(1);
    }
  });
});

describe('background direct capture concurrency and storage', () => {
  test('returns a fresh different-code request before an active capture completes', async () => {
    const gate = deferred<AcquiredShikihoResult>();
    const fresh = snapshot('6758', 1, 1);
    const { coordinator, deps } = harness(
      { '6758': { snapshot: fresh, diagnostic: null, successfulObservedAt: fresh.capturedAt } },
      () => gate.promise
    );
    const active = coordinator.resolve('7203', false);
    await waitForCalls(deps.capture, 1);

    await expect(coordinator.resolve('6758', false)).resolves.toEqual({
      snapshot: fresh,
      diagnostic: null,
      successfulObservedAt: fresh.capturedAt,
      trace: null,
    });
    expect(deps.capture).toHaveBeenCalledTimes(1);

    gate.resolve(acquired({ kind: 'success', snapshot: snapshot('7203') }));
    await active;
  });

  test('returns the same promise and captures once for duplicate code requests', async () => {
    const gate = deferred<AcquiredShikihoResult>();
    const { coordinator, deps } = harness({}, () => gate.promise);
    const first = coordinator.resolve('7203', false);
    const duplicate = coordinator.resolve('7203', false);

    expect(first).toBe(duplicate);
    await waitForCalls(deps.capture, 1);
    gate.resolve(acquired({ kind: 'success', snapshot: snapshot('7203') }));
    await first;
    expect(deps.capture).toHaveBeenCalledTimes(1);
  });

  test('serializes different codes in FIFO order', async () => {
    const firstGate = deferred<AcquiredShikihoResult>();
    const secondGate = deferred<AcquiredShikihoResult>();
    const { coordinator, deps } = harness({}, (code) => (code === '7203' ? firstGate.promise : secondGate.promise));
    const first = coordinator.resolve('7203', false);
    const second = coordinator.resolve('6758', false);
    await waitForCalls(deps.capture, 1);

    expect(deps.capture).toHaveBeenNthCalledWith(1, '7203');
    firstGate.resolve(acquired({ kind: 'success', snapshot: snapshot('7203') }));
    await first;
    await waitForCalls(deps.capture, 2);
    expect(deps.capture).toHaveBeenNthCalledWith(2, '6758');
    secondGate.resolve(acquired({ kind: 'success', snapshot: snapshot('6758') }));
    await second;
  });

  test('saves acquired success and returns the repository state', async () => {
    const captured = snapshot('7203');
    const { coordinator, deps } = harness({}, async () => acquired({ kind: 'success', snapshot: captured }));

    await expect(coordinator.resolve('7203', false)).resolves.toEqual({
      snapshot: captured,
      diagnostic: null,
      trace: null,
    });
    expect(deps.saveSnapshot).toHaveBeenCalledWith(captured);
    expect(deps.get).toHaveBeenCalledTimes(2);
  });

  test('saves acquired diagnostics with the coordinator clock and returns the repository state', async () => {
    const { coordinator, deps } = harness({}, async (code) => acquired({ kind: 'page_changed', code }));

    await expect(coordinator.resolve('7203', false)).resolves.toEqual({
      snapshot: null,
      diagnostic: { schemaVersion: 1, code: '7203', observedAt: new Date(NOW).toISOString(), status: 'page_changed' },
      trace: null,
    });
    expect(deps.saveDiagnostic).toHaveBeenCalledTimes(1);
  });

  test('passive captures only save and never complete an explicit direct capture', async () => {
    const gate = deferred<AcquiredShikihoResult>();
    const { coordinator, deps } = harness({}, () => gate.promise);
    const resolving = coordinator.resolve('7203', false);
    let settled = false;
    void resolving.then(() => {
      settled = true;
    });
    await waitForCalls(deps.capture, 1);

    await coordinator.acceptSnapshot(snapshot('7203'), 999);
    await coordinator.acceptDiagnostic(diagnostic('6758'), 100);
    await Promise.resolve();
    expect(deps.saveSnapshot).toHaveBeenCalledTimes(1);
    expect(deps.saveDiagnostic).toHaveBeenCalledTimes(1);
    expect(settled).toBe(false);

    gate.resolve(acquired({ kind: 'success', snapshot: snapshot('7203') }));
    await resolving;
  });

  test('propagates acquired success storage errors', async () => {
    const { coordinator, deps } = harness();
    const storageError = new Error('storage failed');
    (deps.saveSnapshot as ReturnType<typeof mock>).mockRejectedValueOnce(storageError);

    await expect(coordinator.resolve('7203', false)).rejects.toBe(storageError);
  });

  test('propagates passive storage errors without affecting a direct capture', async () => {
    const gate = deferred<AcquiredShikihoResult>();
    const { coordinator, deps } = harness({}, () => gate.promise);
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.capture, 1);
    const storageError = new Error('passive storage failed');
    (deps.saveSnapshot as ReturnType<typeof mock>).mockRejectedValueOnce(storageError);

    await expect(coordinator.acceptSnapshot(snapshot('6758'), 999)).rejects.toBe(storageError);
    gate.resolve(acquired({ kind: 'success', snapshot: snapshot('7203') }));
    await expect(resolving).resolves.toEqual({ snapshot: snapshot('7203'), diagnostic: null, trace: null });
  });

  test('propagates repository read errors after saving a direct capture', async () => {
    const { coordinator, deps } = harness();
    (deps.get as ReturnType<typeof mock>)
      .mockResolvedValueOnce({ snapshot: null, diagnostic: null })
      .mockRejectedValueOnce(new Error('read failed'));

    await expect(coordinator.resolve('7203', false)).rejects.toThrow('read failed');
    expect(deps.saveSnapshot).toHaveBeenCalledTimes(1);
  });

  test('returns the repository snapshot and terminal trace when a forced refresh fails', async () => {
    const article = snapshot('7203', 1, null);
    const trace = captureTrace();
    const { coordinator } = harness({ '7203': { snapshot: article, diagnostic: null, trace } }, async () => {
      throw new Error('capture failed');
    });
    await expect(coordinator.resolve('7203', true)).resolves.toEqual({ snapshot: article, diagnostic: null, trace });
  });
});
