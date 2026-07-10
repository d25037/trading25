import { normalizeShikihoCode } from './contract';

type TimerHandle = number;

interface ObserverHandle {
  disconnect(): void;
}

type HistoryMethod = (data: unknown, unused: string, url?: string | URL | null) => void;

interface NavigationTarget {
  history: { pushState: HistoryMethod; replaceState: HistoryMethod };
  addEventListener(type: 'popstate' | 'hashchange', listener: () => void): void;
  removeEventListener(type: 'popstate' | 'hashchange', listener: () => void): void;
}

export interface CaptureControllerOptions {
  capture(code: string): void | Promise<void>;
  getCode(): string | null;
  observe?(callback: () => void): ObserverHandle;
  navigation?: NavigationTarget;
  navigationPollMs?: number;
  quietPeriodMs?: number;
  initialMaxWaitMs?: number;
  setTimeout?(callback: () => void, delay: number): TimerHandle;
  clearTimeout?(handle: TimerHandle): void;
}

function observeDocument(callback: () => void): ObserverHandle {
  const observer = new MutationObserver(callback);
  const root = document.documentElement;
  if (root !== null) observer.observe(root, { childList: true, subtree: true, characterData: true });
  return observer;
}

function defaultNavigationTarget(): NavigationTarget {
  return {
    history: window.history,
    addEventListener: (type, listener) => window.addEventListener(type, listener),
    removeEventListener: (type, listener) => window.removeEventListener(type, listener),
  };
}

function observeNavigation(target: NavigationTarget, callback: () => void): () => void {
  const { history } = target;
  const originalPushState = history.pushState;
  const originalReplaceState = history.replaceState;
  const wrappedPushState: HistoryMethod = (...args) => {
    originalPushState.apply(history, args);
    callback();
  };
  const wrappedReplaceState: HistoryMethod = (...args) => {
    originalReplaceState.apply(history, args);
    callback();
  };
  history.pushState = wrappedPushState;
  history.replaceState = wrappedReplaceState;
  target.addEventListener('popstate', callback);
  target.addEventListener('hashchange', callback);

  return () => {
    target.removeEventListener('popstate', callback);
    target.removeEventListener('hashchange', callback);
    if (history.pushState === wrappedPushState) history.pushState = originalPushState;
    if (history.replaceState === wrappedReplaceState) history.replaceState = originalReplaceState;
  };
}

export function createCaptureController(options: CaptureControllerOptions): { start(): void; stop(): void } {
  const quietPeriodMs = options.quietPeriodMs ?? 500;
  const initialMaxWaitMs = options.initialMaxWaitMs ?? 10_000;
  const navigationPollMs = Math.max(50, options.navigationPollMs ?? 250);
  const scheduleTimeout = options.setTimeout ?? ((callback, delay) => window.setTimeout(callback, delay));
  const cancelTimeout = options.clearTimeout ?? ((handle) => window.clearTimeout(handle));
  const observe = options.observe ?? observeDocument;
  let observer: ObserverHandle | null = null;
  let stopObservingNavigation: (() => void) | null = null;
  let quietTimer: TimerHandle | null = null;
  let initialMaxTimer: TimerHandle | null = null;
  let navigationPollTimer: TimerHandle | null = null;
  let started = false;
  let initialCapturePending = true;
  let scheduledCode: string | null = null;
  let lastNavigationCode: string | null = null;

  function clearQuietTimer(): void {
    if (quietTimer === null) return;
    cancelTimeout(quietTimer);
    quietTimer = null;
  }

  function clearInitialMaxTimer(): void {
    if (initialMaxTimer === null) return;
    cancelTimeout(initialMaxTimer);
    initialMaxTimer = null;
  }

  function captureScheduledCode(): void {
    if (!started) return;
    clearQuietTimer();
    const currentCode = normalizeShikihoCode(options.getCode());
    if (currentCode !== scheduledCode) scheduledCode = currentCode;
    if (initialCapturePending) {
      initialCapturePending = false;
      clearInitialMaxTimer();
    }
    if (scheduledCode !== null) void options.capture(scheduledCode);
  }

  function scheduleCapture(): void {
    if (!started) return;
    scheduledCode = normalizeShikihoCode(options.getCode());
    clearQuietTimer();
    quietTimer = scheduleTimeout(captureScheduledCode, quietPeriodMs);
  }

  function navigationChanged(): void {
    lastNavigationCode = normalizeShikihoCode(options.getCode());
    scheduleCapture();
  }

  function pollNavigation(): void {
    if (!started) return;
    const currentCode = normalizeShikihoCode(options.getCode());
    if (currentCode !== lastNavigationCode) navigationChanged();
    navigationPollTimer = scheduleTimeout(pollNavigation, navigationPollMs);
  }

  return {
    start(): void {
      if (started) return;
      started = true;
      initialCapturePending = true;
      lastNavigationCode = normalizeShikihoCode(options.getCode());
      observer = observe(scheduleCapture);
      stopObservingNavigation = observeNavigation(options.navigation ?? defaultNavigationTarget(), navigationChanged);
      scheduleCapture();
      initialMaxTimer = scheduleTimeout(captureScheduledCode, initialMaxWaitMs);
      navigationPollTimer = scheduleTimeout(pollNavigation, navigationPollMs);
    },

    stop(): void {
      if (!started) return;
      started = false;
      observer?.disconnect();
      observer = null;
      stopObservingNavigation?.();
      stopObservingNavigation = null;
      clearQuietTimer();
      clearInitialMaxTimer();
      if (navigationPollTimer !== null) cancelTimeout(navigationPollTimer);
      navigationPollTimer = null;
      scheduledCode = null;
      lastNavigationCode = null;
    },
  };
}
