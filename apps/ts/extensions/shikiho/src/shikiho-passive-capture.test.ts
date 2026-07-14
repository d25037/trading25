import { describe, expect, mock, test } from 'bun:test';
import { Window } from 'happy-dom';
import { readNavigationTiming, startPassiveCaptureWhenReady } from './shikiho-passive-capture';

function documentWith(body: string): Document {
  const window = new Window({ url: 'https://shikiho.toyokeizai.net/stocks/7203' });
  window.document.write(`<main>${body}</main>`);
  return window.document as unknown as Document;
}

function gateHarness(body: string) {
  const document = documentWith(body);
  let readyState: DocumentReadyState = 'loading';
  let mutation: () => void = () => undefined;
  let domContentLoaded: () => void = () => undefined;
  const disconnect = mock(() => undefined);
  const start = mock(() => undefined);
  const stop = startPassiveCaptureWhenReady({
    document,
    getCode: () => '7203',
    getReadyState: () => readyState,
    start,
    observe: (callback) => {
      mutation = callback;
      return { disconnect };
    },
    addDOMContentLoadedListener: (listener) => {
      domContentLoaded = listener;
    },
    removeDOMContentLoadedListener: () => undefined,
  });
  return {
    document,
    disconnect,
    start,
    stop,
    mutate() {
      mutation();
    },
    completeDOMContentLoaded() {
      readyState = 'interactive';
      domContentLoaded();
    },
  };
}

describe('passive Shikiho capture gate', () => {
  test.each([
    '<h1><span>7203</span> 読み込み中</h1>',
    '<h1><span>72030</span> 架空自動車</h1>',
    '<h1>7203 任意のダッシュボード</h1>',
  ])('does not start from an unrecognizable loading identity: %s', (body) => {
    const h = gateHarness(body);

    h.mutate();

    expect(h.start).not.toHaveBeenCalled();
    h.stop();
    expect(h.disconnect).toHaveBeenCalledTimes(1);
  });

  test('starts early for an exact stock code and non-placeholder company identity', () => {
    const h = gateHarness('<header><span>7203</span><h1>架空自動車</h1></header>');

    h.mutate();

    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.disconnect).toHaveBeenCalledTimes(1);
  });

  test('starts at DOMContentLoaded even when recognizable identity never appears', () => {
    const h = gateHarness('<h1>読み込み中</h1>');

    h.completeDOMContentLoaded();

    expect(h.start).toHaveBeenCalledTimes(1);
    expect(h.disconnect).toHaveBeenCalledTimes(1);
  });

  test('extracts only positive finite navigation milestones', () => {
    expect(
      readNavigationTiming({
        getEntriesByType: () => [
          {
            responseStart: 12,
            domInteractive: 0,
            domContentLoadedEventEnd: 34,
            loadEventEnd: Number.NaN,
          },
        ],
      })
    ).toEqual({
      responseStartMs: 12,
      domInteractiveMs: null,
      domContentLoadedMs: 34,
      loadEndMs: null,
    });
    expect(readNavigationTiming({ getEntriesByType: () => [] })).toEqual({
      responseStartMs: null,
      domInteractiveMs: null,
      domContentLoadedMs: null,
      loadEndMs: null,
    });
  });
});
