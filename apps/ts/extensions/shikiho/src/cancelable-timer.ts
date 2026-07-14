export interface CancelableTimer {
  promise: Promise<void>;
  cancel(): void;
}

export interface CancelableTimerScheduler<Handle = ReturnType<typeof globalThis.setTimeout>> {
  setTimeout(callback: () => void, ms: number): Handle;
  clearTimeout(handle: Handle): void;
}

const defaultScheduler: CancelableTimerScheduler = {
  setTimeout: (callback, ms) => globalThis.setTimeout(callback, ms),
  clearTimeout: (handle) => globalThis.clearTimeout(handle),
};

export function createCancelableTimer<Handle = ReturnType<typeof globalThis.setTimeout>>(
  ms: number,
  scheduler: CancelableTimerScheduler<Handle> = defaultScheduler as CancelableTimerScheduler<Handle>
): CancelableTimer {
  let active = true;
  let resolveTimer: () => void = () => undefined;
  const promise = new Promise<void>((resolve) => {
    resolveTimer = resolve;
  });
  const handle = scheduler.setTimeout(() => {
    if (!active) return;
    active = false;
    resolveTimer();
  }, ms);

  return {
    promise,
    cancel() {
      if (!active) return;
      active = false;
      scheduler.clearTimeout(handle);
      resolveTimer();
    },
  };
}
