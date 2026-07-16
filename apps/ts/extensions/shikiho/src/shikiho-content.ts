import { createCaptureController } from './capture-controller';
import { normalizeShikihoCode } from './contract';
import { extractShikihoPage, inspectShikihoPage, type ShikihoExtractionResult } from './extractor';
import { createProgressiveShikihoCapture } from './progressive-capture';
import {
  createShikihoCaptureLanes,
  publishPassiveShikihoResult,
  readNavigationTiming,
  startPassiveCaptureWhenReady,
} from './shikiho-passive-capture';
import { startShikihoTabBridge } from './shikiho-tab-bridge';

const EXTRACTOR_VERSION = '1.0.0';
const captureLanes = createShikihoCaptureLanes();

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
  await publishPassiveShikihoResult(captureLanes, result, fallbackCode, observedAt, (message) =>
    chrome.runtime.sendMessage(message)
  );
}

async function capture(code: string): Promise<void> {
  const observedAt = new Date();
  await publishPassiveResult(extractCurrentPage(observedAt), code, observedAt);
}

const controller = createCaptureController({ capture, getCode: currentCode });

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
  getNavigationTiming: () => readNavigationTiming(performance),
  inspect: () => inspectShikihoPage(document, new URL(window.location.href), new Date(), EXTRACTOR_VERSION),
  onProgress: (progress) => {
    void chrome.runtime.sendMessage({ type: 'capture_progress', progress }).catch(() => undefined);
  },
});

startShikihoTabBridge({
  getCode: currentCode,
  capture: (request) => captureLanes.runExplicit(request.attemptId, () => progressiveCapture.run(request)),
  addMessageListener: (listener) => chrome.runtime.onMessage.addListener(listener),
  removeMessageListener: (listener) => chrome.runtime.onMessage.removeListener(listener),
});

startPassiveCaptureWhenReady({
  document,
  getCode: currentCode,
  getReadyState: () => document.readyState,
  start: () => controller.start(),
  observe: (callback) => {
    const observer = new MutationObserver(callback);
    observer.observe(document, { childList: true, subtree: true, characterData: true });
    return observer;
  },
  addDOMContentLoadedListener: (listener) => document.addEventListener('DOMContentLoaded', listener, { once: true }),
  removeDOMContentLoadedListener: (listener) => document.removeEventListener('DOMContentLoaded', listener),
});
