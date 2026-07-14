import { createCaptureController } from './capture-controller';
import { normalizeShikihoCode, type ShikihoCaptureDiagnosticV1 } from './contract';
import { extractShikihoPage, type ShikihoExtractionResult } from './extractor';
import { startShikihoTabBridge } from './shikiho-tab-bridge';

const EXTRACTOR_VERSION = '1.0.0';

function currentCode(): string | null {
  return normalizeShikihoCode(/^\/stocks\/([^/]+)/.exec(window.location.pathname)?.[1]);
}

function extractCurrentPage(observedAt = new Date()): ShikihoExtractionResult {
  return extractShikihoPage(document, new URL(window.location.href), observedAt, EXTRACTOR_VERSION);
}

async function publishPassiveResult(
  result: ShikihoExtractionResult,
  fallbackCode: string,
  observedAt: Date
): Promise<void> {
  if (result.kind === 'success') {
    await chrome.runtime.sendMessage({ type: 'capture_success', snapshot: result.snapshot });
    return;
  }
  const normalizedCode = normalizeShikihoCode(result.code) ?? normalizeShikihoCode(fallbackCode);
  if (normalizedCode === null) return;
  const diagnostic: ShikihoCaptureDiagnosticV1 = {
    schemaVersion: 1,
    code: normalizedCode,
    observedAt: observedAt.toISOString(),
    status: result.kind,
  };
  await chrome.runtime.sendMessage({ type: 'capture_diagnostic', diagnostic });
}

async function capture(code: string): Promise<void> {
  const observedAt = new Date();
  await publishPassiveResult(extractCurrentPage(observedAt), code, observedAt);
}

function waitForDomQuiet(root: Document, quietMs: number, maximumMs: number): Promise<void> {
  return new Promise((resolve) => {
    let quietTimer: ReturnType<typeof setTimeout>;
    let maximumTimer: ReturnType<typeof setTimeout>;
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      observer.disconnect();
      clearTimeout(quietTimer);
      clearTimeout(maximumTimer);
      resolve();
    };
    const restartQuietTimer = () => {
      clearTimeout(quietTimer);
      quietTimer = setTimeout(finish, quietMs);
    };
    const observer = new MutationObserver(restartQuietTimer);
    observer.observe(root, { childList: true, subtree: true, attributes: true, characterData: true });
    restartQuietTimer();
    maximumTimer = setTimeout(finish, maximumMs);
  });
}

const controller = createCaptureController({ capture, getCode: currentCode });
controller.start();

startShikihoTabBridge({
  getCode: currentCode,
  capture: extractCurrentPage,
  waitUntilReady: () => waitForDomQuiet(document, 500, 10_000),
  addMessageListener: (listener) => chrome.runtime.onMessage.addListener(listener),
  removeMessageListener: (listener) => chrome.runtime.onMessage.removeListener(listener),
});
