import { createCaptureController } from './capture-controller';
import { normalizeShikihoCode, type ShikihoCaptureDiagnosticV1 } from './contract';
import { extractShikihoPage } from './extractor';

const EXTRACTOR_VERSION = '1.0.0';

function currentCode(): string | null {
  return normalizeShikihoCode(/^\/stocks\/([^/]+)/.exec(window.location.pathname)?.[1]);
}

async function capture(code: string): Promise<void> {
  const observedAt = new Date();
  const result = extractShikihoPage(document, new URL(window.location.href), observedAt, EXTRACTOR_VERSION);
  if (result.kind === 'success') {
    await chrome.runtime.sendMessage({ type: 'capture_success', snapshot: result.snapshot });
    return;
  }
  const normalizedCode = normalizeShikihoCode(result.code) ?? normalizeShikihoCode(code);
  if (normalizedCode === null) return;
  const diagnostic: ShikihoCaptureDiagnosticV1 = {
    schemaVersion: 1,
    code: normalizedCode,
    observedAt: observedAt.toISOString(),
    status: result.kind,
  };
  await chrome.runtime.sendMessage({ type: 'capture_diagnostic', diagnostic });
}

const controller = createCaptureController({ capture, getCode: currentCode });
controller.start();
