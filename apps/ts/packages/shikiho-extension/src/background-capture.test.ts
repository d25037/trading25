import { afterEach, describe, expect, mock, test } from 'bun:test';
import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from './contract';
import {
  createBackgroundCaptureCoordinator,
  SHIKIHO_CACHE_TTL_MS,
  SHIKIHO_CAPTURE_TIMEOUT_MS,
  SHIKIHO_RETRY_SUPPRESSION_MS,
  type BackgroundCaptureDeps,
  type StoredShikihoState,
} from './background-capture';

const NOW = Date.parse('2026-07-12T12:00:00.000Z');

function snapshot(code: string, ageMs = 0): ShikihoSnapshotV1 {
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
    score: { overall: null, growth: null, profitability: null, safety: null, scale: null, value: null, priceMomentum: null },
    comparisonCompanies: [],
    industries: [],
    marketThemes: [],
    profile: [],
    missingFields: [],
  };
}

function diagnostic(code: string, ageMs = 0): ShikihoCaptureDiagnosticV1 {
  return { schemaVersion: 1, code, observedAt: new Date(NOW - ageMs).toISOString(), status: 'login_required' };
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
  test('returns a snapshot one millisecond inside the TTL without creating a tab', async () => {
    const fresh = snapshot('7203', SHIKIHO_CACHE_TTL_MS - 1);
    const { coordinator, deps } = harness({ '7203': { snapshot: fresh, diagnostic: null } });

    expect(await coordinator.resolve('7203', false)).toEqual({ snapshot: fresh, diagnostic: null });
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

  test('closes the owned tab after timeout', async () => {
    const { coordinator, deps, timers } = harness();
    const resolving = coordinator.resolve('7203', false);
    await waitForCalls(deps.setTimer, 1);
    expect(deps.setTimer).toHaveBeenCalledWith(expect.any(Function), SHIKIHO_CAPTURE_TIMEOUT_MS);
    [...timers.values()][0]?.();
    await resolving;
    expect(deps.closeTab).toHaveBeenCalledWith(100);
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
