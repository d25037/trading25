import { describe, expect, mock, test } from 'bun:test';
import { createCancelableTimer } from './cancelable-timer';

describe('cancelable timer adapter', () => {
  test('cancel clears the native timeout, settles the promise, and is idempotent', async () => {
    const clearTimeout = mock((_handle: number) => undefined);
    const setTimeout = mock((_callback: () => void, _ms: number) => 41);
    const timer = createCancelableTimer(7_000, { setTimeout, clearTimeout });
    const settled = mock(() => undefined);
    void timer.promise.then(settled);

    timer.cancel();
    await timer.promise;
    timer.cancel();

    expect(setTimeout.mock.calls).toEqual([[expect.any(Function), 7_000]]);
    expect(clearTimeout.mock.calls).toEqual([[41]]);
    expect(settled).toHaveBeenCalledTimes(1);
  });

  test('a fired timer settles normally and no longer needs clearing', async () => {
    let fire: () => void = () => undefined;
    const clearTimeout = mock((_handle: number) => undefined);
    const timer = createCancelableTimer(25_000, {
      setTimeout: (callback) => {
        fire = callback;
        return 42;
      },
      clearTimeout,
    });

    fire();
    await timer.promise;
    timer.cancel();

    expect(clearTimeout).not.toHaveBeenCalled();
  });
});
