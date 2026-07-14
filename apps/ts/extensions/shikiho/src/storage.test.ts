import { describe, expect, test } from 'bun:test';
import type { ShikihoCaptureDiagnosticV1, ShikihoCaptureTraceV1, ShikihoSnapshotV1 } from './contract';
import {
  createShikihoRepository,
  SHIKIHO_DIAGNOSTICS_STORAGE_KEY,
  SHIKIHO_SNAPSHOTS_STORAGE_KEY,
  SHIKIHO_SUCCESSFUL_OBSERVATIONS_STORAGE_KEY,
  SHIKIHO_TRACES_STORAGE_KEY,
  type StorageArea,
} from './storage';

const LATER = '2026-07-10T02:02:03.000Z';

function snapshot(code: string, capturedAt = '2026-07-10T01:02:03.000Z'): ShikihoSnapshotV1 {
  return {
    schemaVersion: 1,
    extractorVersion: '1.0.0',
    code,
    companyName: `Company ${code}`,
    sourceUrl: `https://shikiho.toyokeizai.net/stocks/${code}`,
    capturedAt,
    pageUpdatedAt: null,
    editionLabel: null,
    contentHash: `sha256:${code}`,
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
    missingFields: [],
  };
}

function trace(code: string, updatedAt: string): ShikihoCaptureTraceV1 {
  return {
    schemaVersion: 1,
    attemptId: `attempt-${code}`,
    code,
    mode: 'new_owned_tab',
    phase: 'complete',
    startedAt: '2026-07-14T00:00:00.000Z',
    updatedAt,
    outcome: 'success',
    waitEndReason: 'field_stable',
    receiverAttempts: 1,
    receiverReadyMs: 10,
    documentReadyState: 'complete',
    navigation: {
      responseStartMs: 1,
      domInteractiveMs: 2,
      domContentLoadedMs: 3,
      loadEndMs: 4,
    },
    dom: {
      firstSampleMs: 5,
      mutationBatches: 0,
      meaningfulChanges: 1,
      samples: 1,
      presentFields: ['identity'],
      missingFields: [],
      firstSeenMs: {
        identity: 5,
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
    extraction: { samples: 1, lastMs: 1, maxMs: 1, totalMs: 1 },
    timings: {
      probeMs: 1,
      acquisitionMs: 2,
      receiverMs: 3,
      domObservationMs: 4,
      storageMs: 5,
      totalMs: 15,
    },
  };
}

function memoryStorage(initial: Record<string, unknown> = {}): StorageArea & {
  values: Record<string, unknown>;
  setCalls: Array<Record<string, unknown>>;
} {
  const values = structuredClone(initial);
  const setCalls: Array<Record<string, unknown>> = [];
  return {
    values,
    setCalls,
    async get(keys) {
      if (keys === null) return structuredClone(values);
      const selected = Array.isArray(keys) ? keys : [keys];
      return Object.fromEntries(
        selected.filter((key) => key in values).map((key) => [key, structuredClone(values[key])])
      );
    },
    async set(items) {
      setCalls.push(structuredClone(items));
      Object.assign(values, structuredClone(items));
    },
  };
}

describe('Shikiho storage repository', () => {
  test('keeps the newest terminal trace and stores it under a dedicated key', async () => {
    const area = memoryStorage();
    const repository = createShikihoRepository(area);
    const newer = trace('7203', '2026-07-14T00:00:02.000Z');

    await repository.saveTrace(newer);
    await repository.saveTrace(trace('7203', '2026-07-14T00:00:01.000Z'));

    expect(await repository.getTrace('7203')).toEqual(newer);
    expect(area.setCalls).toEqual([{ [SHIKIHO_TRACES_STORAGE_KEY]: { '7203': newer } }]);
    expect(area.values).not.toHaveProperty(SHIKIHO_SNAPSHOTS_STORAGE_KEY);
    expect(area.values).not.toHaveProperty(SHIKIHO_DIAGNOSTICS_STORAGE_KEY);
    expect(area.values).not.toHaveProperty(SHIKIHO_SUCCESSFUL_OBSERVATIONS_STORAGE_KEY);
  });

  test('serializes concurrent terminal trace writes and preserves the newest updatedAt', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const newer = trace('7203', '2026-07-14T00:00:02.000Z');
    const older = trace('7203', '2026-07-14T00:00:01.000Z');

    await Promise.all([repository.saveTrace(newer), repository.saveTrace(older)]);

    expect(await repository.getTrace('7203')).toEqual(newer);
  });

  test('evicts the least-recently-updated terminal trace above 200 symbols', async () => {
    const repository = createShikihoRepository(memoryStorage());
    for (let index = 0; index < 201; index += 1) {
      const code = String(1000 + index);
      await repository.saveTrace(trace(code, new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString()));
    }

    expect(await repository.getTrace('1000')).toBeNull();
    expect((await repository.getTrace('1200'))?.code).toBe('1200');
  });

  test('same-hash T3 observation blocks a delayed changed T2 snapshot without rewriting the snapshot map', async () => {
    const area = memoryStorage();
    const repository = createShikihoRepository(area);
    const t1 = { ...snapshot('7203', '2026-07-10T01:00:00.000Z'), contentHash: 'sha256:A' };
    const t3Same = { ...t1, capturedAt: '2026-07-10T03:00:00.000Z' };
    const delayedT2 = {
      ...t1,
      capturedAt: '2026-07-10T02:00:00.000Z',
      contentHash: 'sha256:B',
    };

    await repository.saveSnapshot(t1);
    const snapshotWritesAfterT1 = area.setCalls.filter((call) => 'shikihoSnapshotsV1' in call).length;
    await repository.saveSnapshot(t3Same);
    expect(area.setCalls.filter((call) => 'shikihoSnapshotsV1' in call)).toHaveLength(snapshotWritesAfterT1);
    expect(area.values[SHIKIHO_SUCCESSFUL_OBSERVATIONS_STORAGE_KEY]).toEqual({
      '7203': t3Same.capturedAt,
    });
    expect(await repository.get('7203')).toEqual({
      snapshot: t1,
      diagnostic: null,
      successfulObservedAt: t3Same.capturedAt,
    });
    await repository.saveSnapshot(delayedT2);

    expect((await repository.get('7203')).snapshot).toEqual(t1);
  });

  test('same-hash T3 success blocks a delayed T2 diagnostic but permits a newer diagnostic', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const t1 = { ...snapshot('7203', '2026-07-10T01:00:00.000Z'), contentHash: 'sha256:A' };
    const t2Diagnostic: ShikihoCaptureDiagnosticV1 = {
      schemaVersion: 1,
      code: '7203',
      observedAt: '2026-07-10T02:00:00.000Z',
      status: 'page_changed',
    };

    await repository.saveSnapshot(t1);
    await repository.saveDiagnostic(t2Diagnostic);
    await repository.saveSnapshot({ ...t1, capturedAt: '2026-07-10T03:00:00.000Z' });
    expect((await repository.get('7203')).diagnostic).toBeNull();
    await repository.saveDiagnostic(t2Diagnostic);
    expect((await repository.get('7203')).diagnostic).toBeNull();

    const t4Diagnostic = { ...t2Diagnostic, observedAt: '2026-07-10T04:00:00.000Z' };
    await repository.saveDiagnostic(t4Diagnostic);
    expect((await repository.get('7203')).diagnostic).toEqual(t4Diagnostic);
  });

  test('keeps the newer snapshot when an older capture arrives later', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const newer = { ...snapshot('7203', LATER), contentHash: 'sha256:newer' };
    const older = { ...snapshot('7203'), contentHash: 'sha256:older' };

    await repository.saveSnapshot(newer);
    await repository.saveSnapshot(older);

    expect((await repository.get('7203')).snapshot).toEqual(newer);
  });

  test('keeps the newer diagnostic when an older observation arrives later', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const newer: ShikihoCaptureDiagnosticV1 = {
      schemaVersion: 1,
      code: '7203',
      observedAt: LATER,
      status: 'page_changed',
    };
    const older: ShikihoCaptureDiagnosticV1 = {
      ...newer,
      observedAt: '2026-07-10T01:02:03.000Z',
      status: 'login_required',
    };

    await repository.saveDiagnostic(newer);
    await repository.saveDiagnostic(older);

    expect((await repository.get('7203')).diagnostic).toEqual(newer);
  });

  test('serializes delayed concurrent writes and still keeps the newer records', async () => {
    const base = memoryStorage();
    let releaseFirstSet: () => void = () => undefined;
    const firstSetGate = new Promise<void>((resolve) => {
      releaseFirstSet = resolve;
    });
    let firstSet = true;
    const delayedArea: StorageArea = {
      get: base.get,
      async set(items) {
        if (firstSet) {
          firstSet = false;
          await firstSetGate;
        }
        await base.set(items);
      },
    };
    const repository = createShikihoRepository(delayedArea);
    const newer = { ...snapshot('7203', LATER), contentHash: 'sha256:newer' };
    const older = { ...snapshot('7203'), contentHash: 'sha256:older' };

    const newerWrite = repository.saveSnapshot(newer);
    await Promise.resolve();
    await Promise.resolve();
    const olderWrite = repository.saveSnapshot(older);
    releaseFirstSet();
    await Promise.all([newerWrite, olderWrite]);

    expect((await repository.get('7203')).snapshot).toEqual(newer);
  });

  test('keeps a newer diagnostic across concurrent writes', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const newer: ShikihoCaptureDiagnosticV1 = {
      schemaVersion: 1,
      code: '7203',
      observedAt: LATER,
      status: 'page_changed',
    };
    const older: ShikihoCaptureDiagnosticV1 = {
      ...newer,
      observedAt: '2026-07-10T01:02:03.000Z',
      status: 'login_required',
    };

    await Promise.all([repository.saveDiagnostic(newer), repository.saveDiagnostic(older)]);

    expect((await repository.get('7203')).diagnostic).toEqual(newer);
  });

  test('keeps a valid snapshot when a newer diagnostic is recorded', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const snapshot7203 = snapshot('7203');

    await repository.saveSnapshot(snapshot7203);
    await repository.saveDiagnostic({ schemaVersion: 1, code: '7203', observedAt: LATER, status: 'page_changed' });

    expect(await repository.get('7203')).toEqual({
      snapshot: snapshot7203,
      diagnostic: { schemaVersion: 1, code: '7203', observedAt: LATER, status: 'page_changed' },
      successfulObservedAt: snapshot7203.capturedAt,
    });
  });

  test('ignores an unchanged content hash and clears an older diagnostic after capture', async () => {
    const area = memoryStorage();
    const repository = createShikihoRepository(area);
    const original = snapshot('7203');
    const diagnostic: ShikihoCaptureDiagnosticV1 = {
      schemaVersion: 1,
      code: '7203',
      observedAt: '2026-07-10T00:00:00.000Z',
      status: 'page_changed',
    };
    await repository.saveSnapshot(original);
    await repository.saveDiagnostic(diagnostic);

    await repository.saveSnapshot({ ...original, capturedAt: LATER });

    expect(await repository.get('7203')).toEqual({
      snapshot: original,
      diagnostic: null,
      successfulObservedAt: LATER,
    });
  });

  test('evicts the least-recently-captured snapshot above 200 symbols', async () => {
    const repository = createShikihoRepository(memoryStorage());
    for (let index = 0; index < 201; index += 1) {
      const code = String(1000 + index);
      await repository.saveSnapshot(snapshot(code, new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString()));
    }

    expect((await repository.get('1000')).snapshot).toBeNull();
    expect((await repository.get('1200')).snapshot?.code).toBe('1200');
  });
});
