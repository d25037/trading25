import { describe, expect, test } from 'bun:test';
import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from './contract';
import { createShikihoRepository, type StorageArea } from './storage';

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

function memoryStorage(initial: Record<string, unknown> = {}): StorageArea & { values: Record<string, unknown> } {
  const values = structuredClone(initial);
  return {
    values,
    async get(keys) {
      if (keys === null) return structuredClone(values);
      const selected = Array.isArray(keys) ? keys : [keys];
      return Object.fromEntries(
        selected.filter((key) => key in values).map((key) => [key, structuredClone(values[key])])
      );
    },
    async set(items) {
      Object.assign(values, structuredClone(items));
    },
  };
}

describe('Shikiho storage repository', () => {
  test('keeps a valid snapshot when a newer diagnostic is recorded', async () => {
    const repository = createShikihoRepository(memoryStorage());
    const snapshot7203 = snapshot('7203');

    await repository.saveSnapshot(snapshot7203);
    await repository.saveDiagnostic({ schemaVersion: 1, code: '7203', observedAt: LATER, status: 'page_changed' });

    expect(await repository.get('7203')).toEqual({
      snapshot: snapshot7203,
      diagnostic: { schemaVersion: 1, code: '7203', observedAt: LATER, status: 'page_changed' },
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

    expect(await repository.get('7203')).toEqual({ snapshot: original, diagnostic: null });
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
