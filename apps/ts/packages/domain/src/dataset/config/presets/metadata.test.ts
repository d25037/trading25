import { describe, expect, it } from 'bun:test';
import { getPresetConfig, getPresetEstimatedTime, getPresetStockRange } from './metadata';

describe('getPresetConfig', () => {
  it('returns config for primeMarket', () => {
    const config = getPresetConfig('primeMarket', '/tmp/test.db');
    expect(config.outputPath).toBe('/tmp/test.db');
    expect(config.preset).toBe('primeMarket');
    expect(config.markets).toContain('prime');
  });

  it('throws for unknown preset', () => {
    expect(() => getPresetConfig('nonexistent' as never, '/tmp/test.db')).toThrow('Unknown preset');
  });
});

describe('getPresetEstimatedTime', () => {
  it('returns estimated time for preset', () => {
    const time = getPresetEstimatedTime('primeMarket');
    expect(typeof time).toBe('string');
    expect(time.length).toBeGreaterThan(0);
  });
});

describe('getPresetStockRange', () => {
  it('returns stock range for preset', () => {
    const range = getPresetStockRange('primeMarket');
    expect(range).not.toBeNull();
    expect(range?.min).toBeGreaterThan(0);
    expect(range?.max).toBeGreaterThan(range?.min as number);
  });
});
