import { normalizeShikihoCode, type ShikihoCaptureDiagnosticV1 } from './contract';
import { findExactLabel, isElementVisible, normalizeText, type ShikihoExtractionResult } from './extractor';
import type { ProgressiveNavigationTiming } from './progressive-capture';

interface ObserverHandle {
  disconnect(): void;
}

interface NavigationPerformance {
  getEntriesByType(type: string): ArrayLike<unknown>;
}

interface NavigationEntry {
  responseStart?: number;
  domInteractive?: number;
  domContentLoadedEventEnd?: number;
  loadEventEnd?: number;
}

export interface PassiveCaptureGateOptions {
  document: Document;
  getCode(): string | null;
  getReadyState(): DocumentReadyState;
  start(): void;
  observe(callback: () => void): ObserverHandle;
  addDOMContentLoadedListener(listener: () => void): void;
  removeDOMContentLoadedListener(listener: () => void): void;
}

export interface ShikihoCaptureLanes {
  runExplicit<T>(attemptId: string, capture: () => Promise<T>): Promise<T>;
  publishPassive(publish: () => Promise<void>): Promise<boolean>;
}

const LOADING_IDENTITY = /^(?:読み込み中|読込中|loading(?:\.{3})?)$/i;

export function createShikihoCaptureLanes(): ShikihoCaptureLanes {
  const activeAttempts = new Set<symbol>();

  return {
    async runExplicit<T>(attemptId: string, capture: () => Promise<T>): Promise<T> {
      const token = Symbol(attemptId);
      activeAttempts.add(token);
      try {
        return await capture();
      } finally {
        activeAttempts.delete(token);
      }
    },

    async publishPassive(publish: () => Promise<void>): Promise<boolean> {
      if (activeAttempts.size > 0) return false;
      await publish();
      return true;
    },
  };
}

export function publishPassiveShikihoResult(
  lanes: ShikihoCaptureLanes,
  result: ShikihoExtractionResult,
  fallbackCode: string,
  observedAt: Date,
  sendMessage: (message: unknown) => Promise<unknown>
): Promise<boolean> {
  return lanes.publishPassive(async () => {
    if (result.kind === 'success') {
      await sendMessage({ type: 'capture_success', snapshot: result.snapshot });
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
    await sendMessage({ type: 'capture_diagnostic', diagnostic });
  });
}

export function hasRecognizableShikihoIdentity(document: Document, code: string): boolean {
  const codePattern = new RegExp(`(^|\\s)${code}(?=\\s|$)`);
  return Array.from(document.querySelectorAll('h1, [itemprop="name"]')).some((heading) => {
    if (!isElementVisible(heading)) return false;
    const headingText = normalizeText(heading.textContent);
    if (headingText === '' || headingText === 'ログイン') return false;
    const identityRoot = heading.closest('header, main, article') ?? document;
    const hasExactCode = findExactLabel(identityRoot, code) !== null;
    const companyName = normalizeText(headingText.replace(codePattern, ' '));
    return hasExactCode && companyName !== '' && !LOADING_IDENTITY.test(companyName);
  });
}

export function startPassiveCaptureWhenReady(options: PassiveCaptureGateOptions): () => void {
  let started = false;
  let stopped = false;
  let observer: ObserverHandle | null = null;

  const cleanup = () => {
    observer?.disconnect();
    observer = null;
    options.removeDOMContentLoadedListener(start);
  };
  const start = () => {
    if (started || stopped) return;
    started = true;
    cleanup();
    options.start();
  };
  const identityReady = () => {
    const code = options.getCode();
    if (code !== null && hasRecognizableShikihoIdentity(options.document, code)) start();
  };

  if (options.getReadyState() !== 'loading') {
    start();
  } else {
    options.addDOMContentLoadedListener(start);
    observer = options.observe(identityReady);
    identityReady();
  }

  return () => {
    if (stopped) return;
    stopped = true;
    cleanup();
  };
}

export function readNavigationTiming(performance: NavigationPerformance): ProgressiveNavigationTiming {
  const entry = performance.getEntriesByType('navigation')[0] as NavigationEntry | undefined;
  const observed = (value: number | undefined): number | null =>
    value !== undefined && Number.isFinite(value) && value > 0 ? value : null;
  return {
    responseStartMs: observed(entry?.responseStart),
    domInteractiveMs: observed(entry?.domInteractive),
    domContentLoadedMs: observed(entry?.domContentLoadedEventEnd),
    loadEndMs: observed(entry?.loadEventEnd),
  };
}
