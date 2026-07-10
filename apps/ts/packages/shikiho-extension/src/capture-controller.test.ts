import { describe, expect, mock, test } from 'bun:test';
import { createCaptureController } from './capture-controller';

class FakeScheduler {
  now = 0;
  private nextId = 1;
  private tasks = new Map<number, { at: number; callback: () => void }>();

  setTimeout = (callback: () => void, delay: number): number => {
    const id = this.nextId++;
    this.tasks.set(id, { at: this.now + delay, callback });
    return id;
  };

  clearTimeout = (id: number): void => {
    this.tasks.delete(id);
  };

  advance(milliseconds: number): void {
    const target = this.now + milliseconds;
    while (true) {
      const next = [...this.tasks.entries()]
        .filter(([, task]) => task.at <= target)
        .sort((left, right) => left[1].at - right[1].at)[0];
      if (next === undefined) break;
      this.tasks.delete(next[0]);
      this.now = next[1].at;
      next[1].callback();
    }
    this.now = target;
  }
}

describe('capture controller', () => {
  test('debounces DOM mutations and recaptures after URL code change', () => {
    const scheduler = new FakeScheduler();
    const capture = mock((_code: string) => undefined);
    let code = '7203';
    let mutationCallback: () => void = () => undefined;
    const disconnect = mock(() => undefined);
    const controller = createCaptureController({
      capture,
      getCode: () => code,
      observe: (callback) => {
        mutationCallback = callback;
        return { disconnect };
      },
      quietPeriodMs: 100,
      initialMaxWaitMs: 10_000,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
    });

    controller.start();
    mutationCallback();
    mutationCallback();
    mutationCallback();
    scheduler.advance(100);
    expect(capture).toHaveBeenCalledTimes(1);
    expect(capture).toHaveBeenLastCalledWith('7203');

    code = '6758';
    mutationCallback();
    scheduler.advance(100);
    expect(capture).toHaveBeenCalledTimes(2);
    expect(capture).toHaveBeenLastCalledWith('6758');
  });

  test('runs an initial capture by the maximum wait and stop clears all activity', () => {
    const scheduler = new FakeScheduler();
    const capture = mock((_code: string) => undefined);
    let mutationCallback: () => void = () => undefined;
    const disconnect = mock(() => undefined);
    const controller = createCaptureController({
      capture,
      getCode: () => '7203',
      observe: (callback) => {
        mutationCallback = callback;
        return { disconnect };
      },
      quietPeriodMs: 100,
      initialMaxWaitMs: 10_000,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
    });

    controller.start();
    scheduler.advance(10_000);
    expect(capture).toHaveBeenCalledTimes(1);
    mutationCallback();
    controller.stop();
    scheduler.advance(100);
    expect(capture).toHaveBeenCalledTimes(1);
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
