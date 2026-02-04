/**
 * Dataset Presets - Comprehensive Testing
 * Tests all preset configurations
 */

import { describe, expect, test } from 'bun:test';
import { createCustomConfig, createForDateRangeConfig, getMarketCodes, presets, validateConfig } from '../config';
import { randomSample, shuffleArray } from '../utils';

describe('Dataset Presets', () => {
  describe('Basic Market Coverage Presets', () => {
    test('fullMarket preset should match V1 configuration', () => {
      const config = presets.fullMarket('./test-full.db');

      expect(config.outputPath).toBe('./test-full.db');
      expect(config.markets).toEqual(['prime', 'standard', 'growth']);
      expect(config.includeMargin).toBe(true);
      expect(config.includeStatements).toBe(true);
      expect(config.includeTOPIX).toBe(true);
      expect(config.includeSectorIndices).toBe(true);
      // Date range is now omitted - JQuants API uses subscription plan defaults
      expect(config.startDate).toBeUndefined();
      expect(config.endDate).toBeUndefined();

      validateConfig(config);
    });

    test('primeMarket preset should have market cap filter', () => {
      const config = presets.primeMarket('./test-prime.db');

      expect(config.markets).toEqual(['prime']);
      expect(config.marketCapFilter).toBe(10_000_000_000);

      validateConfig(config);
    });

    test('standardMarket preset should target standard market', () => {
      const config = presets.standardMarket('./test-standard.db');

      expect(config.outputPath).toBe('./test-standard.db');
      expect(config.markets).toEqual(['standard']);
      expect(config.includeMargin).toBe(true);
      expect(config.includeStatements).toBe(true);
      expect(config.includeTOPIX).toBe(true);
      expect(config.includeSectorIndices).toBe(true);

      validateConfig(config);
    });

    test('growthMarket preset should target growth market', () => {
      const config = presets.growthMarket('./test-growth.db');

      expect(config.outputPath).toBe('./test-growth.db');
      expect(config.markets).toEqual(['growth']);
      expect(config.includeMargin).toBe(true);
      expect(config.includeStatements).toBe(true);
      expect(config.includeTOPIX).toBe(true);
      expect(config.includeSectorIndices).toBe(true);

      validateConfig(config);
    });
  });

  describe('Development & Testing Presets', () => {
    test('quickTesting preset should limit to 3 stocks', () => {
      const config = presets.quickTesting('./test-quick.db');

      expect(config.markets).toEqual(['prime']);
      expect(config.maxStocks).toBe(3);
      expect(config.includeMargin).toBe(true);

      validateConfig(config);
    });
  });

  describe('Index-Based Presets', () => {
    test('topix100 preset should include Core30 and Large70', () => {
      const config = presets.topix100('./test-topix100.db');

      expect(config.scaleCategories).toEqual(['TOPIX Core30', 'TOPIX Large70']);
      expect(config.markets).toEqual(['prime', 'standard', 'growth']);

      validateConfig(config);
    });

    test('mid400 preset should include Mid400 category', () => {
      const config = presets.mid400('./test-mid400.db');

      expect(config.scaleCategories).toEqual(['TOPIX Mid400']);
      expect(config.markets).toEqual(['prime', 'standard', 'growth']);

      validateConfig(config);
    });
  });

  describe('Preset Object Structure', () => {
    test('should export all preset functions via presets object', () => {
      expect(presets.fullMarket).toBeTypeOf('function');
      expect(presets.primeMarket).toBeTypeOf('function');
      expect(presets.standardMarket).toBeTypeOf('function');
      expect(presets.growthMarket).toBeTypeOf('function');
      expect(presets.quickTesting).toBeTypeOf('function');
      expect(presets.topix100).toBeTypeOf('function');
      expect(presets.topix500).toBeTypeOf('function');
      expect(presets.mid400).toBeTypeOf('function');
      expect(presets.primeExTopix500).toBeTypeOf('function');
    });

    test('preset functions should produce valid configurations', () => {
      const path = './test-comparison.db';

      // Test all main presets
      const config1 = presets.fullMarket(path);
      const config2 = presets.primeMarket(path);
      const config3 = presets.quickTesting(path);

      expect(config1.outputPath).toBe(path);
      expect(config2.outputPath).toBe(path);
      expect(config3.outputPath).toBe(path);

      // All configs should pass validation
      validateConfig(config1);
      validateConfig(config2);
      validateConfig(config3);
    });
  });

  describe('Custom Configuration Functions', () => {
    test('createCustomConfig should work with all options', () => {
      const startDate = new Date('2020-01-01');
      const endDate = new Date('2023-12-31');

      const config = createCustomConfig('./test-custom.db', startDate, endDate, {
        markets: ['prime', 'standard'],
        includeMargin: true,
        minMarketCap: 50_000_000_000,
        maxStockCount: 100,
      });

      expect(config.startDate).toEqual(startDate);
      expect(config.endDate).toEqual(endDate);
      expect(config.markets).toEqual(['prime', 'standard']);
      expect(config.includeMargin).toBe(true);
      expect(config.marketCapFilter).toBe(50_000_000_000);
      expect(config.maxStocks).toBe(100);

      validateConfig(config);
    });

    test('createForDateRangeConfig should support all presets', () => {
      const startDate = new Date('2021-01-01');
      const endDate = new Date('2022-12-31');

      // Test 'full' preset
      const fullConfig = createForDateRangeConfig('./test-full.db', startDate, endDate, 'full');
      expect(fullConfig.markets).toEqual(['prime', 'standard', 'growth']);
      validateConfig(fullConfig);

      // Test 'prime' preset
      const primeConfig = createForDateRangeConfig('./test-prime.db', startDate, endDate, 'prime');
      expect(primeConfig.markets).toEqual(['prime']);
      expect(primeConfig.marketCapFilter).toBe(10_000_000_000);
      validateConfig(primeConfig);
    });
  });

  describe('Market Code Mapping', () => {
    test('getMarketCodes should return correct codes', () => {
      expect(getMarketCodes(['prime'])).toEqual(['0111']);
      expect(getMarketCodes(['standard'])).toEqual(['0112']);
      expect(getMarketCodes(['growth'])).toEqual(['0113']);
      expect(getMarketCodes(['prime', 'standard', 'growth'])).toEqual(['0111', '0112', '0113']);
    });
  });

  describe('Configuration Validation', () => {
    test('all presets should pass validation', () => {
      const testPath = './validation-test.db';

      // Test all preset functions
      expect(() => validateConfig(presets.fullMarket(testPath))).not.toThrow();
      expect(() => validateConfig(presets.primeMarket(testPath))).not.toThrow();
      expect(() => validateConfig(presets.standardMarket(testPath))).not.toThrow();
      expect(() => validateConfig(presets.growthMarket(testPath))).not.toThrow();
      expect(() => validateConfig(presets.quickTesting(testPath))).not.toThrow();
      expect(() => validateConfig(presets.topix100(testPath))).not.toThrow();
      expect(() => validateConfig(presets.topix500(testPath))).not.toThrow();
      expect(() => validateConfig(presets.mid400(testPath))).not.toThrow();
      expect(() => validateConfig(presets.primeExTopix500(testPath))).not.toThrow();
    });

    test('invalid configurations should fail validation', () => {
      expect(() =>
        validateConfig({
          outputPath: '', // Invalid empty path
          markets: ['prime'],
          includeMargin: false,
          includeStatements: true,
          includeTOPIX: true,
          includeSectorIndices: true,
        })
      ).toThrow();

      expect(() =>
        validateConfig({
          outputPath: './test.db',
          markets: [], // Invalid empty markets
          includeMargin: false,
          includeStatements: true,
          includeTOPIX: true,
          includeSectorIndices: true,
        })
      ).toThrow();
    });
  });

  describe('Date Range', () => {
    test('all presets should omit date range to use API defaults', () => {
      // Date range is now intentionally omitted from all presets.
      // JQuants API automatically returns the maximum available data
      // based on subscription plan (e.g., 10 years for Standard plan).
      const configs = [
        presets.fullMarket('./test.db'),
        presets.primeMarket('./test.db'),
        presets.standardMarket('./test.db'),
        presets.growthMarket('./test.db'),
        presets.quickTesting('./test.db'),
        presets.topix100('./test.db'),
        presets.topix500('./test.db'),
        presets.mid400('./test.db'),
        presets.primeExTopix500('./test.db'),
      ];

      for (const config of configs) {
        expect(config.startDate).toBeUndefined();
        expect(config.endDate).toBeUndefined();
      }
    });
  });
});

describe('Preset Performance Characteristics', () => {
  test('should categorize presets by expected API call volume', () => {
    const smallDatasets = [presets.quickTesting('./test.db')];

    const mediumDatasets = [presets.topix100('./test.db'), presets.topix500('./test.db'), presets.mid400('./test.db')];

    const largeDatasets = [presets.primeMarket('./test.db'), presets.fullMarket('./test.db')];

    // Small datasets should have stock limits
    for (const config of smallDatasets) {
      expect(config.maxStocks).toBeDefined();
      if (config.maxStocks !== undefined) {
        expect(config.maxStocks).toBeLessThanOrEqual(10);
      }
    }

    // Medium datasets should use specific scale categories
    for (const config of mediumDatasets) {
      expect(config.scaleCategories !== undefined).toBe(true);
    }

    // Large datasets should include all markets or have market cap filters
    for (const config of largeDatasets) {
      expect(config.markets.length > 1 || config.marketCapFilter !== undefined).toBe(true);
    }
  });
});

describe('V1 to V2 Migration Compatibility', () => {
  test('CLI preset names should map correctly', () => {
    // These are the preset names used in CLI commands
    const cliPresetMappings = {
      full: 'fullMarket',
      prime: 'primeMarket',
      quick: 'quickTesting',
      topix100: 'topix100',
      topix500: 'topix500',
      mid400: 'mid400',
      primeExTopix500: 'primeExTopix500',
    };

    for (const [, presetName] of Object.entries(cliPresetMappings)) {
      // @ts-expect-error - accessing dynamic property
      const presetFn = presets[presetName];
      expect(presetFn).toBeTypeOf('function');

      const config = presetFn('./test.db');
      expect(config).toBeDefined();
      validateConfig(config);
    }
  });
});

describe('Random Sampling Utilities', () => {
  describe('shuffleArray', () => {
    test('should shuffle array without changing length', () => {
      const original = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      const shuffled = shuffleArray(original);

      expect(shuffled).toHaveLength(original.length);
      expect(shuffled).not.toBe(original); // Different references
      expect(shuffled.sort()).toEqual(original.sort()); // Same elements
    });

    test('should produce consistent results with same seed', () => {
      const original = ['A', 'B', 'C', 'D', 'E'];
      const seed = 12345;

      const shuffled1 = shuffleArray(original, seed);
      const shuffled2 = shuffleArray(original, seed);

      expect(shuffled1).toEqual(shuffled2);
    });

    test('should produce different results with different seeds', () => {
      const original = ['A', 'B', 'C', 'D', 'E'];

      const shuffled1 = shuffleArray(original, 123);
      const shuffled2 = shuffleArray(original, 456);

      expect(shuffled1).not.toEqual(shuffled2);
    });

    test('should handle empty array', () => {
      const empty: number[] = [];
      const shuffled = shuffleArray(empty);

      expect(shuffled).toEqual([]);
    });

    test('should handle single element array', () => {
      const single = ['only'];
      const shuffled = shuffleArray(single);

      expect(shuffled).toEqual(['only']);
    });
  });

  describe('randomSample', () => {
    test('should return correct sample size', () => {
      const data = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];
      const sample = randomSample(data, 5);

      expect(sample).toHaveLength(5);

      // All sampled items should be from original array
      for (const item of sample) {
        expect(data).toContain(item);
      }
    });

    test('should return empty array for size 0', () => {
      const data = [1, 2, 3, 4, 5];
      const sample = randomSample(data, 0);

      expect(sample).toEqual([]);
    });

    test('should return full array if sample size >= array length', () => {
      const data = [1, 2, 3];
      const sample = randomSample(data, 5);

      expect(sample).toHaveLength(3);
      expect(sample.sort()).toEqual(data.sort());
    });

    test('should produce consistent results with same seed', () => {
      const data = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];
      const seed = 99999;

      const sample1 = randomSample(data, 4, seed);
      const sample2 = randomSample(data, 4, seed);

      expect(sample1).toEqual(sample2);
    });

    test('should produce different results with different seeds', () => {
      const data = Array.from({ length: 100 }, (_, i) => i + 1);

      const sample1 = randomSample(data, 20, 111);
      const sample2 = randomSample(data, 20, 222);

      expect(sample1).not.toEqual(sample2);
    });

    test('should handle negative sample size gracefully', () => {
      const data = [1, 2, 3, 4, 5];
      const sample = randomSample(data, -1);

      expect(sample).toEqual([]);
    });
  });
});
