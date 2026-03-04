/**
 * Dataset Paths - Testing
 * Tests for dataset path utilities including market database path
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  ensureDbExtension,
  getDatasetPath,
  getDatasetV2Path,
  getMarketDbPath,
  getPortfolioDbPath,
  normalizeDatasetPath,
  resolveDatasetPath,
} from './dataset-paths';

describe('Dataset Paths', () => {
  describe('getMarketDbPath', () => {
    let originalXdgDataHome: string | undefined;

    beforeEach(() => {
      // Save original environment variable
      originalXdgDataHome = process.env.XDG_DATA_HOME;
    });

    afterEach(() => {
      // Restore original environment variable
      if (originalXdgDataHome === undefined) {
        // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
        delete process.env.XDG_DATA_HOME;
      } else {
        process.env.XDG_DATA_HOME = originalXdgDataHome;
      }
    });

    test('should return default XDG path when XDG_DATA_HOME is not set', () => {
      // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
      delete process.env.XDG_DATA_HOME;

      const dbPath = getMarketDbPath();
      const expectedPath = path.join(os.homedir(), '.local', 'share', 'trading25', 'market.db');

      expect(dbPath).toBe(expectedPath);
    });

    test('should use XDG_DATA_HOME when set', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        const dbPath = getMarketDbPath();
        const expectedPath = path.join(customDataHome, 'trading25', 'market.db');

        expect(dbPath).toBe(expectedPath);
      } finally {
        // Clean up
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should create directory if it does not exist', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        // Directory should not exist initially
        expect(fs.existsSync(customDataHome)).toBe(false);

        // Calling getMarketDbPath should create it
        const dbPath = getMarketDbPath();

        // Directory should now exist
        const tradingDir = path.join(customDataHome, 'trading25');
        expect(fs.existsSync(tradingDir)).toBe(true);

        // Path should be correct
        const expectedPath = path.join(customDataHome, 'trading25', 'market.db');
        expect(dbPath).toBe(expectedPath);
      } finally {
        // Clean up
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should handle existing directory gracefully', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      const tradingDir = path.join(customDataHome, 'trading25');

      try {
        // Create directory beforehand
        fs.mkdirSync(tradingDir, { recursive: true });
        process.env.XDG_DATA_HOME = customDataHome;

        // Should not throw error when directory exists
        const dbPath = getMarketDbPath();

        // Path should be correct
        const expectedPath = path.join(customDataHome, 'trading25', 'market.db');
        expect(dbPath).toBe(expectedPath);
        expect(fs.existsSync(tradingDir)).toBe(true);
      } finally {
        // Clean up
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should return absolute path', () => {
      const dbPath = getMarketDbPath();

      expect(path.isAbsolute(dbPath)).toBe(true);
    });

    test('should always end with market.db', () => {
      const dbPath = getMarketDbPath();

      expect(dbPath.endsWith('market.db')).toBe(true);
    });

    test('should contain trading25 directory in path', () => {
      const dbPath = getMarketDbPath();

      expect(dbPath).toContain('trading25');
    });
  });

  describe('getDatasetV2Path', () => {
    let originalXdgDataHome: string | undefined;

    beforeEach(() => {
      originalXdgDataHome = process.env.XDG_DATA_HOME;
    });

    afterEach(() => {
      if (originalXdgDataHome === undefined) {
        // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
        delete process.env.XDG_DATA_HOME;
      } else {
        process.env.XDG_DATA_HOME = originalXdgDataHome;
      }
    });

    test('should return default XDG path when XDG_DATA_HOME not set', () => {
      // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
      delete process.env.XDG_DATA_HOME;
      const dbPath = getDatasetV2Path('prime.db');
      const expectedPath = path.join(os.homedir(), '.local', 'share', 'trading25', 'datasets', 'prime.db');
      expect(dbPath).toBe(expectedPath);
    });

    test('should use XDG_DATA_HOME when set', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        const dbPath = getDatasetV2Path('prime.db');
        const expectedPath = path.join(customDataHome, 'trading25', 'datasets', 'prime.db');
        expect(dbPath).toBe(expectedPath);
      } finally {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should create directory if it does not exist', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        expect(fs.existsSync(customDataHome)).toBe(false);
        const dbPath = getDatasetV2Path('prime.db');
        const datasetsDir = path.join(customDataHome, 'trading25', 'datasets');
        expect(fs.existsSync(datasetsDir)).toBe(true);
        expect(dbPath).toContain('datasets');
      } finally {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should handle subdirectories', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-test-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        const dbPath = getDatasetV2Path('markets/prime.db');
        const expectedPath = path.join(customDataHome, 'trading25', 'datasets', 'markets', 'prime.db');
        expect(dbPath).toBe(expectedPath);

        // Verify subdirectory was created
        const marketsDir = path.join(customDataHome, 'trading25', 'datasets', 'markets');
        expect(fs.existsSync(marketsDir)).toBe(true);
      } finally {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('should auto-add .db extension', () => {
      const dbPath = getDatasetV2Path('prime');
      expect(dbPath.endsWith('prime.db')).toBe(true);
    });

    test('should reject absolute paths', () => {
      expect(() => getDatasetV2Path('/absolute/path/prime.db')).toThrow('Absolute paths are not allowed');
    });

    test('should reject parent directory references', () => {
      expect(() => getDatasetV2Path('../outside/prime.db')).toThrow('Parent directory references (..) are not allowed');
    });

    test('should reject invalid filesystem characters', () => {
      expect(() => getDatasetV2Path('invalid<>:"|?*.db')).toThrow('Invalid characters in filename');
    });

    test('should reject paths exceeding 255 characters', () => {
      const longPath = `${'a'.repeat(256)}.db`;
      expect(() => getDatasetV2Path(longPath)).toThrow('Path too long');
    });

    test('should normalize leading ./ in paths', () => {
      const dbPath = getDatasetV2Path('./prime.db');
      expect(dbPath.endsWith('datasets/prime.db')).toBe(true);
    });

    test('should verify resolved path stays within datasets directory', () => {
      const dbPath = getDatasetV2Path('prime.db');
      expect(dbPath).toContain('trading25/datasets');
    });

    test('should throw error for empty filename', () => {
      expect(() => getDatasetV2Path('')).toThrow('Invalid filename provided');
    });
  });

  describe('ensureDbExtension', () => {
    test('adds .db extension when missing', () => {
      expect(ensureDbExtension('myfile')).toBe('myfile.db');
    });

    test('keeps .db extension when present', () => {
      expect(ensureDbExtension('myfile.db')).toBe('myfile.db');
    });

    test('throws on empty string', () => {
      expect(() => ensureDbExtension('')).toThrow('Invalid file path');
    });

    test('throws on non-string input', () => {
      expect(() => ensureDbExtension(null as unknown as string)).toThrow('Invalid file path');
    });
  });

  describe('resolveDatasetPath', () => {
    test('rejects empty input', () => {
      expect(() => resolveDatasetPath('')).toThrow('Invalid file path');
    });

    test('rejects absolute paths', () => {
      expect(() => resolveDatasetPath('/etc/passwd')).toThrow('Absolute paths are not allowed');
    });

    test('rejects path traversal', () => {
      expect(() => resolveDatasetPath('../../../etc/passwd')).toThrow('Parent directory references');
    });

    test('rejects paths longer than 255 chars', () => {
      expect(() => resolveDatasetPath('a'.repeat(256))).toThrow('Path too long');
    });

    test('rejects invalid filesystem characters', () => {
      expect(() => resolveDatasetPath('file<name')).toThrow('Invalid characters');
      expect(() => resolveDatasetPath('file>name')).toThrow('Invalid characters');
      expect(() => resolveDatasetPath('file|name')).toThrow('Invalid characters');
    });

    test('rejects control characters', () => {
      expect(() => resolveDatasetPath('file\x00name')).toThrow('Invalid characters');
    });

    test('resolves simple filename to dataset directory', () => {
      const result = resolveDatasetPath('prime.db');
      expect(result).toContain('dataset');
      expect(result).toContain('prime.db');
    });

    test('normalizes leading ./', () => {
      const result1 = resolveDatasetPath('./myfile.db');
      const result2 = resolveDatasetPath('myfile.db');
      expect(result1).toBe(result2);
    });
  });

  describe('normalizeDatasetPath', () => {
    test('resolves and adds .db extension', () => {
      const result = normalizeDatasetPath('prime');
      expect(result).toContain('prime.db');
      expect(result).toContain('dataset');
    });

    test('keeps existing .db extension', () => {
      const result = normalizeDatasetPath('prime.db');
      expect(result).toContain('prime.db');
      expect(result).not.toContain('prime.db.db');
    });
  });

  describe('getPortfolioDbPath', () => {
    let originalXdgDataHome: string | undefined;

    beforeEach(() => {
      originalXdgDataHome = process.env.XDG_DATA_HOME;
    });

    afterEach(() => {
      if (originalXdgDataHome === undefined) {
        // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
        delete process.env.XDG_DATA_HOME;
      } else {
        process.env.XDG_DATA_HOME = originalXdgDataHome;
      }
    });

    test('returns path ending in portfolio.db', () => {
      const result = getPortfolioDbPath();
      expect(result).toMatch(/portfolio\.db$/);
    });

    test('returns absolute path', () => {
      const result = getPortfolioDbPath();
      expect(path.isAbsolute(result)).toBe(true);
    });

    test('uses default XDG path when not set', () => {
      // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
      delete process.env.XDG_DATA_HOME;
      const result = getPortfolioDbPath();
      const expected = path.join(os.homedir(), '.local', 'share', 'trading25', 'portfolio.db');
      expect(result).toBe(expected);
    });

    test('respects XDG_DATA_HOME', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-portfolio-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        const result = getPortfolioDbPath();
        const expected = path.join(customDataHome, 'trading25', 'portfolio.db');
        expect(result).toBe(expected);
      } finally {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });

    test('creates directory if it does not exist', () => {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trading25-portfolio-'));
      const customDataHome = path.join(tempDir, 'custom-data');
      process.env.XDG_DATA_HOME = customDataHome;

      try {
        expect(fs.existsSync(customDataHome)).toBe(false);
        getPortfolioDbPath();
        const tradingDir = path.join(customDataHome, 'trading25');
        expect(fs.existsSync(tradingDir)).toBe(true);
      } finally {
        if (fs.existsSync(tempDir)) {
          fs.rmSync(tempDir, { recursive: true, force: true });
        }
      }
    });
  });

  describe('getDatasetPath', () => {
    let originalXdgDataHome: string | undefined;

    beforeEach(() => {
      originalXdgDataHome = process.env.XDG_DATA_HOME;
    });

    afterEach(() => {
      if (originalXdgDataHome === undefined) {
        // biome-ignore lint/performance/noDelete: Need to actually delete env var for testing
        delete process.env.XDG_DATA_HOME;
      } else {
        process.env.XDG_DATA_HOME = originalXdgDataHome;
      }
    });

    test('resolves simple filename', () => {
      const result = getDatasetPath('prime.db');
      expect(result).toMatch(/datasets.*prime\.db$/);
    });

    test('adds .db extension', () => {
      const result = getDatasetPath('prime');
      expect(result).toMatch(/prime\.db$/);
    });

    test('rejects empty filename', () => {
      expect(() => getDatasetPath('')).toThrow('Invalid filename');
    });

    test('rejects absolute paths', () => {
      expect(() => getDatasetPath('/etc/passwd')).toThrow('Absolute paths are not allowed');
    });

    test('rejects path traversal', () => {
      expect(() => getDatasetPath('../secret')).toThrow('Parent directory references');
    });

    test('normalizes leading ./', () => {
      const result1 = getDatasetPath('./prime');
      const result2 = getDatasetPath('prime');
      expect(result1).toBe(result2);
    });
  });
});
