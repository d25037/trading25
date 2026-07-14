import { findExactLabel, isElementVisible, normalizeText } from './extractor';
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

const LOADING_IDENTITY = /^(?:読み込み中|読込中|loading(?:\.{3})?)$/i;

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
