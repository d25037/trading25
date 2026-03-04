import { afterEach, describe, expect, test } from 'bun:test';
import { BrowserTokenStorage } from './browser-token-storage';

interface MockLocalStorage {
  setItem(key: string, value: string): void;
  getItem(key: string): string | null;
  removeItem(key: string): void;
}

const originalGlobalThisRef = globalThis.globalThis;
const originalLocalStorageDescriptor = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');

function restoreGlobalBindings(): void {
  Object.defineProperty(globalThis, 'globalThis', {
    value: originalGlobalThisRef,
    configurable: true,
    writable: true,
  });

  if (originalLocalStorageDescriptor) {
    Object.defineProperty(globalThis, 'localStorage', originalLocalStorageDescriptor);
  } else {
    Reflect.deleteProperty(globalThis as Record<string, unknown>, 'localStorage');
  }
}

function createMockLocalStorage(): MockLocalStorage {
  const store = new Map<string, string>();

  return {
    setItem(key: string, value: string): void {
      store.set(key, value);
    },
    getItem(key: string): string | null {
      return store.get(key) ?? null;
    },
    removeItem(key: string): void {
      store.delete(key);
    },
  };
}

describe('BrowserTokenStorage', () => {
  afterEach(() => {
    restoreGlobalBindings();
  });

  test('saves, reads, validates, and clears API key in localStorage', async () => {
    Object.defineProperty(globalThis, 'localStorage', {
      value: createMockLocalStorage(),
      configurable: true,
      writable: true,
    });

    const messages: Array<{ message: string; level?: 'info' | 'warn' | 'error' }> = [];
    const storage = new BrowserTokenStorage({
      keyPrefix: 'test_',
      logger: (message, level) => messages.push({ message, level }),
    });

    await storage.saveTokens({ apiKey: 'secret-key' });
    expect(await storage.getTokens()).toEqual({ apiKey: 'secret-key' });
    expect(await storage.hasValidTokens()).toBe(true);

    await storage.clearTokens();
    expect(await storage.getTokens()).toEqual({ apiKey: undefined });
    expect(await storage.hasValidTokens()).toBe(false);
    expect(messages.some((entry) => entry.message.includes('Updated localStorage with API key'))).toBe(true);
    expect(messages.some((entry) => entry.message.includes('Cleared API key from localStorage'))).toBe(true);
  });

  test('logs warning and no-ops when localStorage is unavailable', async () => {
    Reflect.deleteProperty(globalThis as Record<string, unknown>, 'localStorage');
    const messages: Array<{ message: string; level?: 'info' | 'warn' | 'error' }> = [];
    const storage = new BrowserTokenStorage({
      logger: (message, level) => messages.push({ message, level }),
    });

    await storage.saveTokens({ apiKey: 'ignored' });

    expect(await storage.getTokens()).toEqual({});
    expect(messages).toContainEqual({
      message: 'localStorage not available',
      level: 'warn',
    });
  });

  test('reports localStorage availability check errors only once', () => {
    const messages: Array<{ message: string; level?: 'info' | 'warn' | 'error' }> = [];
    const storage = new BrowserTokenStorage({
      logger: (message, level) => messages.push({ message, level }),
    });

    const throwingGlobalThis = new Proxy(originalGlobalThisRef, {
      has(): boolean {
        throw new Error('probe failed');
      },
    });

    Object.defineProperty(globalThis, 'globalThis', {
      value: throwingGlobalThis,
      configurable: true,
      writable: true,
    });

    expect(storage.isAvailable()).toBe(false);
    expect(storage.isAvailable()).toBe(false);

    const warningMessages = messages.filter(
      (entry) => entry.level === 'warn' && entry.message.includes('localStorage availability check failed')
    );
    expect(warningMessages).toHaveLength(1);
  });
});
