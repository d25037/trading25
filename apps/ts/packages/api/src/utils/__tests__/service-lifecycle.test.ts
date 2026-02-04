import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test';
import {
  clearServiceRegistry,
  createManagedService,
  getRegisteredServiceCount,
  registerServiceForCleanup,
  unregisterService,
} from '../service-lifecycle';

beforeEach(() => {
  clearServiceRegistry();
});

afterEach(() => {
  clearServiceRegistry();
});

describe('registerServiceForCleanup', () => {
  test('registers a service', () => {
    const service = { close: mock() };
    registerServiceForCleanup('TestService', service);
    expect(getRegisteredServiceCount()).toBeGreaterThanOrEqual(1);
  });

  test('replaces service with same name', () => {
    const service1 = { close: mock() };
    const service2 = { close: mock() };
    registerServiceForCleanup('ReplaceTest', service1);
    const countAfterFirst = getRegisteredServiceCount();
    registerServiceForCleanup('ReplaceTest', service2);
    expect(getRegisteredServiceCount()).toBe(countAfterFirst);
  });
});

describe('unregisterService', () => {
  test('removes a registered service', () => {
    const service = { close: mock() };
    registerServiceForCleanup('UnregTest', service);
    const countAfterAdd = getRegisteredServiceCount();
    unregisterService('UnregTest');
    expect(getRegisteredServiceCount()).toBe(countAfterAdd - 1);
  });

  test('does nothing for unregistered name', () => {
    const before = getRegisteredServiceCount();
    unregisterService('NonExistent');
    expect(getRegisteredServiceCount()).toBe(before);
  });
});

describe('clearServiceRegistry', () => {
  test('clears all services', () => {
    registerServiceForCleanup('A', { close: mock() });
    registerServiceForCleanup('B', { close: mock() });
    clearServiceRegistry();
    expect(getRegisteredServiceCount()).toBe(0);
  });
});

describe('createManagedService', () => {
  test('creates service lazily', () => {
    const factory = mock(() => ({ close: mock() }));
    const getService = createManagedService('Lazy', { factory });

    expect(factory).not.toHaveBeenCalled();
    const instance = getService();
    expect(factory).toHaveBeenCalledTimes(1);
    expect(instance).toBeDefined();
  });

  test('returns same instance on subsequent calls', () => {
    const factory = mock(() => ({ close: mock() }));
    const getService = createManagedService('Singleton', { factory });

    const a = getService();
    const b = getService();
    expect(a).toBe(b);
    expect(factory).toHaveBeenCalledTimes(1);
  });

  test('registers service for cleanup on first call', () => {
    const initial = getRegisteredServiceCount();
    const factory = mock(() => ({ close: mock() }));
    const getService = createManagedService('AutoCleanup', { factory });

    getService();
    expect(getRegisteredServiceCount()).toBe(initial + 1);
  });

  test('calls setup function if provided', () => {
    const setupFn = mock();
    const service = { close: mock() };
    const getService = createManagedService('WithSetup', {
      factory: () => service,
      setup: setupFn,
    });

    getService();
    expect(setupFn).toHaveBeenCalledWith(service);
  });
});
