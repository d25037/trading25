import {
  normalizeShikihoCode,
  parseShikihoDiagnostic,
  parseShikihoSnapshot,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoSnapshotV1,
} from './contract';

export const SHIKIHO_SNAPSHOTS_STORAGE_KEY = 'shikihoSnapshotsV1';
export const SHIKIHO_DIAGNOSTICS_STORAGE_KEY = 'shikihoDiagnosticsV1';

const MAX_SNAPSHOTS = 200;

export interface StorageArea {
  get(keys: string | string[] | null): Promise<Record<string, unknown>>;
  set(items: Record<string, unknown>): Promise<void>;
}

type SnapshotMap = Record<string, ShikihoSnapshotV1>;
type DiagnosticMap = Record<string, ShikihoCaptureDiagnosticV1>;

function record(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function snapshotMap(value: unknown): SnapshotMap {
  const snapshots: SnapshotMap = {};
  for (const [code, candidate] of Object.entries(record(value))) {
    const snapshot = parseShikihoSnapshot(candidate);
    if (snapshot !== null && snapshot.code === code) snapshots[code] = snapshot;
  }
  return snapshots;
}

function diagnosticMap(value: unknown): DiagnosticMap {
  const diagnostics: DiagnosticMap = {};
  for (const [code, candidate] of Object.entries(record(value))) {
    const diagnostic = parseShikihoDiagnostic(candidate);
    if (diagnostic !== null && diagnostic.code === code) diagnostics[code] = diagnostic;
  }
  return diagnostics;
}

function isOlderOrEqual(left: string, right: string): boolean {
  return Date.parse(left) <= Date.parse(right);
}

function clearOlderDiagnostic(diagnostics: DiagnosticMap, snapshot: ShikihoSnapshotV1): boolean {
  const diagnostic = diagnostics[snapshot.code];
  if (diagnostic === undefined || !isOlderOrEqual(diagnostic.observedAt, snapshot.capturedAt)) return false;
  delete diagnostics[snapshot.code];
  return true;
}

function evictOldestSnapshot(snapshots: SnapshotMap, diagnostics: DiagnosticMap): void {
  const codes = Object.keys(snapshots);
  if (codes.length <= MAX_SNAPSHOTS) return;
  const oldestCode = codes.reduce((oldest, code) =>
    isOlderOrEqual(snapshots[code]?.capturedAt ?? '', snapshots[oldest]?.capturedAt ?? '') ? code : oldest
  );
  delete snapshots[oldestCode];
  delete diagnostics[oldestCode];
}

export function createShikihoRepository(area: StorageArea = chrome.storage.local) {
  let pendingWrite = Promise.resolve();
  const latestObservationByCode = new Map<string, number>();

  function latestObservation(code: string, snapshots: SnapshotMap, diagnostics: DiagnosticMap): number {
    const storedSnapshotTime = Date.parse(snapshots[code]?.capturedAt ?? '');
    const storedDiagnosticTime = Date.parse(diagnostics[code]?.observedAt ?? '');
    const latest = Math.max(
      latestObservationByCode.get(code) ?? Number.NEGATIVE_INFINITY,
      Number.isNaN(storedSnapshotTime) ? Number.NEGATIVE_INFINITY : storedSnapshotTime,
      Number.isNaN(storedDiagnosticTime) ? Number.NEGATIVE_INFINITY : storedDiagnosticTime
    );
    if (Number.isFinite(latest)) latestObservationByCode.set(code, latest);
    return latest;
  }

  function recordObservation(code: string, observedAt: string): void {
    latestObservationByCode.set(code, Date.parse(observedAt));
  }

  function writeSerially(operation: () => Promise<void>): Promise<void> {
    const result = pendingWrite.then(operation, operation);
    pendingWrite = result.catch(() => undefined);
    return result;
  }

  return {
    async get(codeValue: string): Promise<{
      snapshot: ShikihoSnapshotV1 | null;
      diagnostic: ShikihoCaptureDiagnosticV1 | null;
    }> {
      await pendingWrite;
      const code = normalizeShikihoCode(codeValue);
      if (code === null) return { snapshot: null, diagnostic: null };
      const stored = await area.get([SHIKIHO_SNAPSHOTS_STORAGE_KEY, SHIKIHO_DIAGNOSTICS_STORAGE_KEY]);
      return {
        snapshot: snapshotMap(stored[SHIKIHO_SNAPSHOTS_STORAGE_KEY])[code] ?? null,
        diagnostic: diagnosticMap(stored[SHIKIHO_DIAGNOSTICS_STORAGE_KEY])[code] ?? null,
      };
    },

    saveSnapshot(snapshot: ShikihoSnapshotV1): Promise<void> {
      return writeSerially(async () => {
        const stored = await area.get([SHIKIHO_SNAPSHOTS_STORAGE_KEY, SHIKIHO_DIAGNOSTICS_STORAGE_KEY]);
        const snapshots = snapshotMap(stored[SHIKIHO_SNAPSHOTS_STORAGE_KEY]);
        const diagnostics = diagnosticMap(stored[SHIKIHO_DIAGNOSTICS_STORAGE_KEY]);
        const current = snapshots[snapshot.code];
        if (Date.parse(snapshot.capturedAt) <= latestObservation(snapshot.code, snapshots, diagnostics)) return;
        const snapshotChanged = current?.contentHash !== snapshot.contentHash;
        if (snapshotChanged) snapshots[snapshot.code] = snapshot;

        const diagnosticCleared = clearOlderDiagnostic(diagnostics, snapshot);

        if (!snapshotChanged && !diagnosticCleared) {
          recordObservation(snapshot.code, snapshot.capturedAt);
          return;
        }
        evictOldestSnapshot(snapshots, diagnostics);

        const updates: Record<string, unknown> = { [SHIKIHO_DIAGNOSTICS_STORAGE_KEY]: diagnostics };
        if (snapshotChanged) updates[SHIKIHO_SNAPSHOTS_STORAGE_KEY] = snapshots;
        await area.set(updates);
        recordObservation(snapshot.code, snapshot.capturedAt);
      });
    },

    saveDiagnostic(diagnostic: ShikihoCaptureDiagnosticV1): Promise<void> {
      return writeSerially(async () => {
        const stored = await area.get([SHIKIHO_SNAPSHOTS_STORAGE_KEY, SHIKIHO_DIAGNOSTICS_STORAGE_KEY]);
        const snapshots = snapshotMap(stored[SHIKIHO_SNAPSHOTS_STORAGE_KEY]);
        const diagnostics = diagnosticMap(stored[SHIKIHO_DIAGNOSTICS_STORAGE_KEY]);
        if (Date.parse(diagnostic.observedAt) <= latestObservation(diagnostic.code, snapshots, diagnostics)) return;
        diagnostics[diagnostic.code] = diagnostic;
        await area.set({ [SHIKIHO_DIAGNOSTICS_STORAGE_KEY]: diagnostics });
        recordObservation(diagnostic.code, diagnostic.observedAt);
      });
    },
  };
}
