import { normalizeShikihoCode, parseShikihoDiagnostic, parseShikihoSnapshot } from './contract';
import { createShikihoRepository } from './storage';

const repository = createShikihoRepository();

type BackgroundMessage =
  | { type: 'capture_success'; snapshot: unknown }
  | { type: 'capture_diagnostic'; diagnostic: unknown }
  | { type: 'get_snapshot'; code: unknown };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function parseBackgroundMessage(value: unknown): BackgroundMessage | null {
  if (!isRecord(value)) return null;
  if (value.type === 'capture_success' && hasExactKeys(value, ['type', 'snapshot'])) {
    return { type: value.type, snapshot: value.snapshot };
  }
  if (value.type === 'capture_diagnostic' && hasExactKeys(value, ['type', 'diagnostic'])) {
    return { type: value.type, diagnostic: value.diagnostic };
  }
  if (value.type === 'get_snapshot' && hasExactKeys(value, ['type', 'code'])) {
    return { type: value.type, code: value.code };
  }
  return null;
}

async function handleBackgroundMessage(message: BackgroundMessage): Promise<unknown> {
  if (message.type === 'capture_success') {
    const snapshot = parseShikihoSnapshot(message.snapshot);
    if (snapshot === null) return { ok: false };
    try {
      await repository.saveSnapshot(snapshot);
    } catch {
      await repository
        .saveDiagnostic({
          schemaVersion: 1,
          code: snapshot.code,
          observedAt: new Date().toISOString(),
          status: 'storage_error',
        })
        .catch(() => undefined);
      return { ok: false };
    }
    return { ok: true };
  }
  if (message.type === 'capture_diagnostic') {
    const diagnostic = parseShikihoDiagnostic(message.diagnostic);
    if (diagnostic === null) return { ok: false };
    await repository.saveDiagnostic(diagnostic);
    return { ok: true };
  }
  const code = normalizeShikihoCode(message.code);
  if (code === null || code !== message.code) return { snapshot: null, diagnostic: null };
  return repository.get(code);
}

chrome.runtime.onMessage.addListener((rawMessage: unknown, _sender, sendResponse) => {
  const message = parseBackgroundMessage(rawMessage);
  if (message === null) return false;

  void handleBackgroundMessage(message)
    .then(sendResponse)
    .catch(() => sendResponse({ ok: false }));

  return true;
});
