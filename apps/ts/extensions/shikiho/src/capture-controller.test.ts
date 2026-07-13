import { describe, expect, mock, test } from 'bun:test';
import { Window } from 'happy-dom';
import { createCaptureController } from './capture-controller';
import { extractShikihoPage } from './extractor';

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

function noOpNavigation() {
  return {
    history: { pushState: () => undefined, replaceState: () => undefined },
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
  };
}

describe('capture controller', () => {
  test('recaptures after pushState changes the code without a DOM mutation and restores navigation hooks', () => {
    const scheduler = new FakeScheduler();
    const capture = mock((_code: string) => undefined);
    let code = '7203';
    const originalPushState = mock((_data: unknown, _unused: string, _url?: string | URL | null) => undefined);
    const originalReplaceState = mock((_data: unknown, _unused: string, _url?: string | URL | null) => undefined);
    const history = { pushState: originalPushState, replaceState: originalReplaceState };
    const listeners = new Map<string, Set<() => void>>();
    const navigation = {
      history,
      addEventListener(type: string, listener: () => void) {
        const registered = listeners.get(type) ?? new Set();
        registered.add(listener);
        listeners.set(type, registered);
      },
      removeEventListener(type: string, listener: () => void) {
        listeners.get(type)?.delete(listener);
      },
    };
    const controller = createCaptureController({
      capture,
      getCode: () => code,
      observe: () => ({ disconnect: () => undefined }),
      navigation,
      navigationPollMs: 50,
      quietPeriodMs: 100,
      initialMaxWaitMs: 10_000,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
    });

    controller.start();
    scheduler.advance(100);
    expect(capture).toHaveBeenLastCalledWith('7203');
    code = '6758';
    originalPushState({}, '', '/stocks/6758');
    scheduler.advance(150);
    expect(capture).toHaveBeenLastCalledWith('6758');
    expect(capture).toHaveBeenCalledTimes(2);

    controller.stop();
    expect(history.pushState).toBe(originalPushState);
    expect(history.replaceState).toBe(originalReplaceState);
    expect(listeners.get('popstate')?.size).toBe(0);
    expect(listeners.get('hashchange')?.size).toBe(0);
    code = '9984';
    scheduler.advance(1_000);
    expect(capture).toHaveBeenCalledTimes(2);
  });

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
      navigation: noOpNavigation(),
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

  test('maximum wait captures while mutations continuously reset the quiet timer and stop clears all activity', () => {
    const scheduler = new FakeScheduler();
    const captureTimes: number[] = [];
    const capture = mock((_code: string) => {
      captureTimes.push(scheduler.now);
    });
    let mutationCallback: () => void = () => undefined;
    const disconnect = mock(() => undefined);
    const controller = createCaptureController({
      capture,
      getCode: () => '7203',
      observe: (callback) => {
        mutationCallback = callback;
        return { disconnect };
      },
      navigation: noOpNavigation(),
      quietPeriodMs: 100,
      initialMaxWaitMs: 10_000,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
    });

    controller.start();
    for (let elapsed = 0; elapsed < 9_999; elapsed += 99) {
      scheduler.advance(99);
      mutationCallback();
    }
    scheduler.advance(10_000 - scheduler.now);
    expect(capture).toHaveBeenCalledTimes(1);
    expect(captureTimes).toEqual([10_000]);
    mutationCallback();
    controller.stop();
    scheduler.advance(100);
    expect(capture).toHaveBeenCalledTimes(1);
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  test('recaptures a complete score after the score region is inserted late', () => {
    const scheduler = new FakeScheduler();
    const window = new Window({ url: 'https://shikiho.toyokeizai.net/stocks/7203' });
    window.document.write(`
      <main>
        <h1><span>7203</span> 架空輸送</h1>
        <dl><dt>特色</dt><dd>架空の企業特色です。</dd><dt>連結事業</dt><dd>輸送80%、部品20%</dd></dl>
        <section><h2>会社四季報</h2><p>【概況】架空の短いコメントです。</p></section>
      </main>
    `);
    const document = window.document as unknown as Document;
    const snapshots: Array<ReturnType<typeof extractShikihoPage>> = [];
    let mutationCallback: () => void = () => undefined;
    const controller = createCaptureController({
      capture: () => {
        snapshots.push(
          extractShikihoPage(
            document,
            new URL('https://shikiho.toyokeizai.net/stocks/7203'),
            new Date('2026-07-10T01:02:03.000Z'),
            '1.0.0'
          )
        );
      },
      getCode: () => '7203',
      observe: (callback) => {
        mutationCallback = callback;
        return { disconnect: () => undefined };
      },
      navigation: noOpNavigation(),
      quietPeriodMs: 100,
      initialMaxWaitMs: 10_000,
      setTimeout: scheduler.setTimeout,
      clearTimeout: scheduler.clearTimeout,
    });

    controller.start();
    scheduler.advance(100);
    const initial = snapshots[0];
    expect(initial?.kind).toBe('success');
    if (initial?.kind !== 'success') throw new Error('expected initial success');
    expect(initial.snapshot.score.overall).toBeNull();
    expect(initial.snapshot.missingFields).toContain('score');

    document.querySelector('main')?.insertAdjacentHTML(
      'beforeend',
      `<section>
        <header><h2>四季報スコア</h2><strong>3</strong></header>
        <dl>
          <dt>成長性</dt><dd>0</dd><dt>収益性</dt><dd>1</dd><dt>安全性</dt><dd>2</dd>
          <dt>規模</dt><dd>3</dd><dt>割安度</dt><dd>4</dd><dt>値上がり</dt><dd>5</dd>
        </dl>
      </section>`
    );
    mutationCallback();
    scheduler.advance(100);

    const recaptured = snapshots[1];
    expect(recaptured?.kind).toBe('success');
    if (recaptured?.kind !== 'success') throw new Error('expected recapture success');
    expect(recaptured.snapshot.score).toEqual({
      overall: 3,
      growth: 0,
      profitability: 1,
      safety: 2,
      scale: 3,
      value: 4,
      priceMomentum: 5,
    });
    expect(recaptured.snapshot.missingFields).not.toContain('score');
  });
});
