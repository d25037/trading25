import { createCaptureController } from './capture-controller';
import { normalizeShikihoCode, type ShikihoCaptureDiagnosticV1 } from './contract';
import { extractShikihoPage, type ShikihoExtractionResult } from './extractor';
import { createProgressiveShikihoCapture, type ProgressiveNavigationTiming } from './progressive-capture';
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

function navigationTiming(): ProgressiveNavigationTiming {
  const entry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
  const observed = (value: number | undefined): number | null =>
    value !== undefined && Number.isFinite(value) && value > 0 ? value : null;
  return {
    responseStartMs: observed(entry?.responseStart),
    domInteractiveMs: observed(entry?.domInteractive),
    domContentLoadedMs: observed(entry?.domContentLoadedEventEnd),
    loadEndMs: observed(entry?.loadEventEnd),
  };
}

const controller = createCaptureController({ capture, getCode: currentCode });

function hasRecognizableIdentity(): boolean {
  const code = currentCode();
  if (code === null) return false;
  return Array.from(document.querySelectorAll('h1')).some((heading) => {
    const text = heading.textContent ?? '';
    return text.includes(code) && text.replace(code, '').trim().length > 0;
  });
}

function startPassiveCaptureWhenReady(): void {
  let started = false;
  let identityObserver: MutationObserver | null = null;
  const start = () => {
    if (started) return;
    started = true;
    identityObserver?.disconnect();
    identityObserver = null;
    document.removeEventListener('DOMContentLoaded', start);
    controller.start();
  };
  if (document.readyState !== 'loading' || hasRecognizableIdentity()) {
    start();
    return;
  }
  document.addEventListener('DOMContentLoaded', start, { once: true });
  identityObserver = new MutationObserver(() => {
    if (hasRecognizableIdentity()) start();
  });
  identityObserver.observe(document, { childList: true, subtree: true, characterData: true });
}

const progressiveCapture = createProgressiveShikihoCapture({
  now: () => Date.now(),
  setTimeout: (callback, delay) => window.setTimeout(callback, delay),
  clearTimeout: (handle) => window.clearTimeout(handle),
  observe: (callback) => {
    const observer = new MutationObserver(callback);
    observer.observe(document, { childList: true, subtree: true, attributes: true, characterData: true });
    return observer;
  },
  getCode: currentCode,
  getReadyState: () => document.readyState,
  getNavigationTiming: navigationTiming,
  extract: extractCurrentPage,
  onProgress: (progress) => {
    void chrome.runtime.sendMessage({ type: 'capture_progress', progress }).catch(() => undefined);
  },
});

startShikihoTabBridge({
  getCode: currentCode,
  capture: (request) => progressiveCapture.run(request),
  addMessageListener: (listener) => chrome.runtime.onMessage.addListener(listener),
  removeMessageListener: (listener) => chrome.runtime.onMessage.removeListener(listener),
});

startPassiveCaptureWhenReady();
