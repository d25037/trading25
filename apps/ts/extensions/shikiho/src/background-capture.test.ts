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
import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from './contract';

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

function pageChanged(code: string): ShikihoCaptureDiagnosticV1 {
  return { ...diagnostic(code), status: 'page_changed' };
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

function harness(initial: Record<string, StoredShikihoState> = {}) {
  const states = new Map(Object.entries(initial));
  const timers = new Map<unknown, () => void>();
  let timerId = 0;
  let nextTabId = 100;
  const deps: BackgroundCaptureDeps = {
    now: () => NOW,
    get: mock(async (code: string) => states.get(code) ?? { snapshot: null, diagnostic: null }),
    saveSnapshot: mock(async (value: ShikihoSnapshotV1) => {
      states.set(value.code, { snapshot: value, diagnostic: null });
    }),
    saveDiagnostic: mock(async (value: ShikihoCaptureDiagnosticV1) => {
      states.set(value.code, { snapshot: states.get(value.code)?.snapshot ?? null, diagnostic: value });
    }),
    createTab: mock(async () => ({ id: nextTabId++ })),
    closeTab: mock(async () => undefined),
    setTimer: mock((callback: () => void, _delayMs: number) => {
      const id = ++timerId;
      timers.set(id, callback);
      return id;
    }),
    clearTimer: mock((timer: unknown) => {
      timers.delete(timer);
    }),
  };
  return { deps, states, timers, coordinator: createBackgroundCaptureCoordinator(deps) };
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
    });
    expect(deps.createTab).toHaveBeenCalledTimes(0);
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
    });
    expect(deps.createTab).toHaveBeenCalledTimes(0);
  });

  test('refreshes a snapshot exactly at the TTL boundary', async () => {
    const stale = snapshot('7203', SHIKIHO_CACHE_TTL_MS);
    const { coordinator, deps } = harness({ '7203': { snapshot: stale, diagnostic: null } });
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await resolving;
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
    });
    expect(insideHarness.deps.createTab).toHaveBeenCalledTimes(0);

    const boundary = snapshot('7203', 1, 1);
    const boundaryHarness = harness({
      '7203': {
        snapshot: boundary,
        diagnostic: null,
        successfulObservedAt: new Date(NOW - SHIKIHO_QUOTE_TTL_MS).toISOString(),
      },
    });
    const resolving = boundaryHarness.coordinator.resolve('7203', false);
    await waitForCalls(boundaryHarness.deps.setTimer, 1);
    await boundaryHarness.coordinator.acceptSnapshot(snapshot('7203'), 100);
    await resolving;
    expect(boundaryHarness.deps.createTab).toHaveBeenCalledTimes(1);
  });

  test('refreshes a fresh article when its quote is missing, from another JST date, or in the future', async () => {
    const cases = [
      snapshot('7203', 1, null),
      {
        ...snapshot('7203', 1, 1),
        quote: {
          ...quote(),
          tradingDate: '2026-07-11',
          observedAt: '2026-07-11T20:59:59.999+09:00',
        },
      },
      {
        ...snapshot('7203', 1, 1),
        quote: { ...quote(), observedAt: new Date(NOW + 1).toISOString() },
      },
    ];

    for (const storedSnapshot of cases) {
      const { coordinator, deps } = harness({ '7203': { snapshot: storedSnapshot, diagnostic: null } });
      const resolving = coordinator.resolve('7203', false);
      await waitForCalls(deps.setTimer, 1);
      await coordinator.acceptSnapshot(snapshot('7203'), 100);
      await resolving;
      expect(deps.createTab).toHaveBeenCalledTimes(1);
    }
  });

  test('recent diagnostics suppress automatic quote retry while preserving the stored article', async () => {
    const article = snapshot('7203', 1, null);
    const recent = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS - 1);
    const { coordinator, deps } = harness({ '7203': { snapshot: article, diagnostic: recent } });

    expect(await coordinator.resolve('7203', false)).toEqual({ snapshot: article, diagnostic: recent });
    expect(deps.createTab).toHaveBeenCalledTimes(0);
  });

  test('preserves the prior article when a quote-only refresh ends in a diagnostic', async () => {
    const article = snapshot('7203', 1, null);
    const { coordinator, deps } = harness({ '7203': { snapshot: article, diagnostic: null } });
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(diagnostic('7203'), 100);

    expect(await resolving).toEqual({ snapshot: article, diagnostic: diagnostic('7203') });
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('suppresses automatic retry for a diagnostic inside 60 seconds', async () => {
    const recent = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS - 1);
    const { coordinator, deps } = harness({ '7203': { snapshot: null, diagnostic: recent } });

    expect(await coordinator.resolve('7203', false)).toEqual({ snapshot: null, diagnostic: recent });
    expect(deps.createTab).toHaveBeenCalledTimes(0);
  });

  test('retries a diagnostic exactly at the 60 second boundary', async () => {
    const boundary = diagnostic('7203', SHIKIHO_RETRY_SUPPRESSION_MS);
    const { coordinator, deps } = harness({ '7203': { snapshot: null, diagnostic: boundary } });
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(diagnostic('7203'), 100);
    await resolving;
    expect(deps.createTab).toHaveBeenCalledTimes(1);
  });

  test('manual refresh bypasses both a fresh snapshot and a recent diagnostic', async () => {
    for (const state of [
      { snapshot: snapshot('7203', 1), diagnostic: null },
      { snapshot: null, diagnostic: diagnostic('7203', 1) },
    ]) {
      const { coordinator, deps } = harness({ '7203': state });
      const resolving = coordinator.resolve('7203', true);
      await waitForCalls(deps.setTimer, 1);
      await coordinator.acceptSnapshot(snapshot('7203'), 100);
      await resolving;
    }
  });
});

describe('background capture concurrency and lifecycle', () => {
  test('returns a fresh different-code request before an active capture completes', async () => {
    const fresh = snapshot('6758', 1, 1);
    const { coordinator, deps } = harness({
      '6758': { snapshot: fresh, diagnostic: null, successfulObservedAt: fresh.capturedAt },
    });
    const active = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);

    await expect(coordinator.resolve('6758', false)).resolves.toEqual({
      snapshot: fresh,
      diagnostic: null,
      successfulObservedAt: fresh.capturedAt,
    });
    expect(deps.createTab).toHaveBeenCalledTimes(1);

    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await active;
  });

  test('returns the same promise and creates one tab for duplicate code requests', async () => {
    const { coordinator, deps } = harness();
    const first = coordinator.resolve('7203', false);
    const duplicate = coordinator.resolve('7203', false);

    expect(first).toBe(duplicate);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await first;
  });

  test('serializes different codes in FIFO order', async () => {
    const { coordinator, deps } = harness();
    const first = coordinator.resolve('7203', false);
    const second = coordinator.resolve('6758', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await first;
    await waitForCalls(deps.setTimer, 2);
    expect(deps.createTab).toHaveBeenNthCalledWith(1, 'https://shikiho.toyokeizai.net/stocks/7203');
    expect(deps.createTab).toHaveBeenNthCalledWith(2, 'https://shikiho.toyokeizai.net/stocks/6758');
    await coordinator.acceptSnapshot(snapshot('6758'), 101);
    await second;
  });

  test('saves passive captures but only matching code and owned tab complete the job', async () => {
    const { coordinator, deps } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptSnapshot(snapshot('7203'), 999);
    await coordinator.acceptSnapshot(snapshot('6758'), 100);
    expect(deps.saveSnapshot).toHaveBeenCalledTimes(2);
    expect(deps.closeTab).toHaveBeenCalledTimes(0);

    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await resolving;
    expect(deps.closeTab).toHaveBeenCalledTimes(1);
    expect(deps.closeTab).toHaveBeenCalledWith(100);
    expect(deps.closeTab).not.toHaveBeenCalledWith(999);
  });

  test('closes the owned tab after a matching diagnostic', async () => {
    const { coordinator, deps } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.acceptDiagnostic(diagnostic('7203'), 100);
    expect((await resolving).diagnostic).toEqual(diagnostic('7203'));
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('keeps an owned job pending after transient page_changed until matching login_required arrives', async () => {
    const { coordinator, deps, timers } = harness();
    const resolving = coordinator.resolve('7203', false);
    let settled = false;
    void resolving.then(() => {
      settled = true;
    });
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(pageChanged('7203'), 100);
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(deps.saveDiagnostic).toHaveBeenCalledWith(pageChanged('7203'));
    expect(deps.closeTab).toHaveBeenCalledTimes(0);
    expect(timers.size).toBe(1);

    await coordinator.acceptDiagnostic(diagnostic('7203'), 100);
    expect((await resolving).diagnostic).toEqual(diagnostic('7203'));
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('keeps an owned job pending after transient page_changed until matching success arrives', async () => {
    const { coordinator, deps, timers } = harness();
    const resolving = coordinator.resolve('7203', false);
    let settled = false;
    void resolving.then(() => {
      settled = true;
    });
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(pageChanged('7203'), 100);
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(deps.closeTab).toHaveBeenCalledTimes(0);
    expect(timers.size).toBe(1);

    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    expect((await resolving).snapshot).toEqual(snapshot('7203'));
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('times out and suppresses retry when transient page_changed has no later terminal result', async () => {
    const { coordinator, deps, timers } = harness();
    const resolving = coordinator.resolve('7203', false);
    let settled = false;
    void resolving.then(() => {
      settled = true;
    });
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(pageChanged('7203'), 100);
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(deps.closeTab).toHaveBeenCalledTimes(0);
    [...timers.values()][0]?.();

    const resolved = await resolving;
    expect(resolved.diagnostic?.status).toBe('page_changed');
    expect(deps.closeTab).toHaveBeenCalledWith(100);
    expect(await coordinator.resolve('7203', false)).toEqual(resolved);
    expect(deps.createTab).toHaveBeenCalledTimes(1);
  });

  test('stores passive user-tab page_changed without completing or closing the owned job', async () => {
    const { coordinator, deps } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.acceptDiagnostic(pageChanged('7203'), 999);
    expect(deps.saveDiagnostic).toHaveBeenCalledWith(pageChanged('7203'));
    expect(deps.closeTab).not.toHaveBeenCalledWith(999);
    expect(deps.closeTab).toHaveBeenCalledTimes(0);

    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await resolving;
  });

  test('records timeout diagnostics, closes the owned tab, and suppresses the next automatic retry', async () => {
    const { coordinator, deps, timers } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    expect(deps.setTimer).toHaveBeenCalledWith(expect.any(Function), 25_000);
    [...timers.values()][0]?.();
    const resolved = await resolving;
    expect(resolved.diagnostic).toEqual({
      schemaVersion: 1,
      code: '7203',
      observedAt: new Date(NOW).toISOString(),
      status: 'page_changed',
    });
    expect(deps.closeTab).toHaveBeenCalledWith(100);
    expect(await coordinator.resolve('7203', false)).toEqual(resolved);
    expect(deps.createTab).toHaveBeenCalledTimes(1);
  });

  test('records a user-closed owned tab, releases FIFO, and does not close that tab again', async () => {
    const { coordinator, deps } = harness();
    const first = coordinator.resolve('7203', false);
    const second = coordinator.resolve('6758', false);
    await waitForCalls(deps.setTimer, 1);

    await coordinator.onTabRemoved(100);
    expect((await first).diagnostic?.status).toBe('page_changed');
    expect(deps.closeTab).not.toHaveBeenCalledWith(100);
    await waitForCalls(deps.setTimer, 2);
    await coordinator.acceptSnapshot(snapshot('6758'), 101);
    await second;
  });

  test('ignores removal of a user-owned tab', async () => {
    const { coordinator, deps } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.onTabRemoved(999);
    expect(deps.saveDiagnostic).toHaveBeenCalledTimes(0);
    expect(deps.closeTab).not.toHaveBeenCalledWith(999);
    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await resolving;
  });

  test('tolerates an already user-closed owned tab', async () => {
    const { coordinator, deps } = harness();
    (deps.closeTab as ReturnType<typeof mock>).mockRejectedValueOnce(new Error('No tab with id'));
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    await expect(resolving).resolves.toEqual({ snapshot: snapshot('7203'), diagnostic: null });
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('closes the owned tab when capture storage throws', async () => {
    const { coordinator, deps } = harness();
    const storageError = new Error('storage failed');
    (deps.saveSnapshot as ReturnType<typeof mock>).mockRejectedValueOnce(storageError);
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await expect(coordinator.acceptSnapshot(snapshot('7203'), 100)).rejects.toBe(storageError);
    await expect(resolving).rejects.toBe(storageError);
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });

  test('closes a created tab when repository read throws', async () => {
    const gate = deferred<StoredShikihoState>();
    const { coordinator, deps } = harness();
    (deps.get as ReturnType<typeof mock>)
      .mockResolvedValueOnce({ snapshot: null, diagnostic: null })
      .mockImplementationOnce(() => gate.promise);
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    await coordinator.acceptSnapshot(snapshot('7203'), 100);
    gate.reject(new Error('read failed'));
    await expect(resolving).rejects.toThrow('read failed');
    expect(deps.closeTab).toHaveBeenCalledWith(100);
  });
});
