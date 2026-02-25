import { afterEach, beforeEach, describe, expect, spyOn, test } from 'bun:test';
import { ConsoleLogger, SilentLogger, createDefaultLogger, logger } from './logger';

const originalNodeEnv = process.env.NODE_ENV;
const originalLogLevel = process.env.LOG_LEVEL;
const originalGlobalThisRef = globalThis.globalThis;

const loggerState = logger as unknown as {
  level: string;
  isBrowser: boolean;
  correlationId?: string;
  detectBrowser: () => boolean;
};

function restoreEnvironment(): void {
  process.env.NODE_ENV = originalNodeEnv;
  process.env.LOG_LEVEL = originalLogLevel;
  Object.defineProperty(globalThis, 'globalThis', {
    value: originalGlobalThisRef,
    configurable: true,
    writable: true,
  });
}

describe('api-clients logger', () => {
  beforeEach(() => {
    loggerState.level = 'TRACE';
    loggerState.isBrowser = false;
    loggerState.correlationId = undefined;
  });

  afterEach(() => {
    restoreEnvironment();
    loggerState.level = 'SILENT';
    loggerState.isBrowser = false;
    loggerState.correlationId = undefined;
  });

  test('createDefaultLogger returns ConsoleLogger', () => {
    const instance = createDefaultLogger();
    expect(instance).toBeInstanceOf(ConsoleLogger);
  });

  test('SilentLogger methods are no-op', () => {
    const silent = new SilentLogger();
    expect(() => {
      silent.trace();
      silent.debug();
      silent.info();
      silent.warn();
      silent.error();
      silent.fatal();
    }).not.toThrow();
  });

  test('logs in server and browser mode using expected console channels', () => {
    const logSpy = spyOn(console, 'log').mockImplementation(() => {});
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = spyOn(console, 'error').mockImplementation(() => {});

    logger.trace('server-trace');
    logger.debug('server-debug');
    logger.info('server-info');
    logger.warn('server-warn');
    logger.error('server-error');
    logger.fatal('server-fatal');
    expect(logSpy).toHaveBeenCalled();
    expect(warnSpy).toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalled();

    loggerState.isBrowser = true;
    logger.info('browser-info');
    logger.warn('browser-warn');
    logger.error('browser-error');

    expect(String(logSpy.mock.calls.at(-1)?.[0])).toContain('%c');
    expect(String(warnSpy.mock.calls.at(-1)?.[0])).toContain('%c');
    expect(String(errorSpy.mock.calls.at(-1)?.[0])).toContain('%c');

    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test('ConsoleLogger delegates all methods to the api-clients singleton logger', () => {
    const compatibilityLogger = new ConsoleLogger();
    const logSpy = spyOn(console, 'log').mockImplementation(() => {});
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    const errorSpy = spyOn(console, 'error').mockImplementation(() => {});

    compatibilityLogger.trace('compat-trace');
    compatibilityLogger.debug('compat-debug');
    compatibilityLogger.info('compat-info');
    compatibilityLogger.warn('compat-warn');
    compatibilityLogger.error('compat-error');
    compatibilityLogger.fatal('compat-fatal');

    expect(logSpy.mock.calls.length).toBeGreaterThan(0);
    expect(warnSpy.mock.calls.length).toBeGreaterThan(0);
    expect(errorSpy.mock.calls.length).toBeGreaterThan(0);

    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test('supports correlation helpers and child logger inheritance', () => {
    process.env.LOG_LEVEL = 'trace';
    logger.setCorrelationId('12345678-1111-2222-3333-444444444444');
    const generated = logger.createCorrelationId();
    expect(generated.length).toBeGreaterThan(0);

    const child = logger.child({});
    const childState = child as unknown as { correlationId?: string };
    expect(childState.correlationId).toBe('12345678-1111-2222-3333-444444444444');
  });

  test('detectBrowser catches and reports probe failures once outside test env', () => {
    process.env.NODE_ENV = 'development';
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});

    const throwingGlobalThis = new Proxy(originalGlobalThisRef, {
      has(): boolean {
        throw new Error('browser detection failed');
      },
    });

    Object.defineProperty(globalThis, 'globalThis', {
      value: throwingGlobalThis,
      configurable: true,
      writable: true,
    });

    expect(loggerState.detectBrowser()).toBe(false);
    expect(loggerState.detectBrowser()).toBe(false);

    const detectionWarnings = warnSpy.mock.calls.filter(([message]) =>
      String(message).includes('[api-clients logger] Browser environment detection failed')
    );
    expect(detectionWarnings).toHaveLength(1);
    warnSpy.mockRestore();
  });
});
