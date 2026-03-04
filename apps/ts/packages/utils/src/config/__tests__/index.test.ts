import { afterEach, describe, expect, it } from 'bun:test';
import { getConfig, resetConfig, setConfig } from '../index';

describe('config', () => {
  afterEach(() => {
    resetConfig();
  });

  describe('getConfig', () => {
    it('returns default values', () => {
      const config = getConfig();
      expect(config.database.maxConnections).toBe(3);
      expect(config.database.statementCacheSize).toBe(100);
      expect(config.rateLimiter.requestsPerSecond).toBe(10);
      expect(config.dataset.defaultChunkSize).toBe(100);
      // logLevel depends on LOG_LEVEL env var; just verify it's a valid level
      expect(['debug', 'info', 'warn', 'error']).toContain(config.logLevel);
    });

    it('returns the same singleton instance', () => {
      const config1 = getConfig();
      const config2 = getConfig();
      expect(config1).toBe(config2);
    });
  });

  describe('resetConfig', () => {
    it('resets and reloads config', () => {
      const config1 = getConfig();
      resetConfig();
      const config2 = getConfig();
      // Different object reference after reset
      expect(config1).not.toBe(config2);
      // But same values
      expect(config2.database.maxConnections).toBe(3);
    });
  });

  describe('setConfig', () => {
    it('overrides specific values', () => {
      setConfig({ logLevel: 'debug' });
      const config = getConfig();
      expect(config.logLevel).toBe('debug');
      // Other values unchanged
      expect(config.database.maxConnections).toBe(3);
    });
  });
});
