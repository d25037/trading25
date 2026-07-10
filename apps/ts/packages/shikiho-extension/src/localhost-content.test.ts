import { describe, expect, mock, test } from 'bun:test';
import { isAllowedTrading25Origin, startLocalhostBridge } from './localhost-content';

describe('localhost content bridge', () => {
  test('does not activate the localhost bridge on an unapproved port', () => {
    expect(isAllowedTrading25Origin(new URL('http://localhost:3002'))).toBe(false);
    expect(isAllowedTrading25Origin(new URL('http://localhost:5173'))).toBe(true);
    expect(isAllowedTrading25Origin(new URL('http://127.0.0.1:4173'))).toBe(true);
    expect(isAllowedTrading25Origin(new URL('https://localhost:5173'))).toBe(false);
  });

  test('returns before adding listeners on an unapproved port', () => {
    const addWindowListener = mock(() => undefined);
    const addStorageListener = mock(() => undefined);

    const stop = startLocalhostBridge({
      url: new URL('http://localhost:3002'),
      addWindowListener,
      removeWindowListener: () => undefined,
      addStorageListener,
      removeStorageListener: () => undefined,
      sendMessage: async () => null,
      postMessage: () => undefined,
      currentWindow: {},
    });

    expect(addWindowListener).not.toHaveBeenCalled();
    expect(addStorageListener).not.toHaveBeenCalled();
    stop();
  });
});
