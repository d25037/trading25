/**
 * Security tests for path validation functions
 */

import { describe, expect, test } from 'bun:test';
import { isFilePathSafe, validateFilePath } from '../validators';

describe('validateFilePath security', () => {
  describe('path traversal prevention', () => {
    test('should reject simple path traversal', () => {
      const errors = validateFilePath('../etc/passwd');
      expect(errors).toContain('File path contains directory traversal pattern (..)');
    });

    test('should reject nested path traversal', () => {
      const errors = validateFilePath('foo/../../etc/passwd');
      expect(errors).toContain('File path contains directory traversal pattern (..)');
    });

    test('should reject path traversal at end', () => {
      const errors = validateFilePath('foo/..');
      expect(errors).toContain('File path contains directory traversal pattern (..)');
    });

    test('should reject Windows-style path traversal', () => {
      const errors = validateFilePath('foo\\..\\etc\\passwd');
      expect(errors).toContain('File path contains directory traversal pattern (..)');
    });

    test('should reject mixed path traversal', () => {
      const errors = validateFilePath('foo\\..\\..\\etc/passwd');
      expect(errors).toContain('File path contains directory traversal pattern (..)');
    });
  });

  describe('null byte injection prevention', () => {
    test('should reject null bytes', () => {
      const errors = validateFilePath('file.db\x00.txt');
      expect(errors).toContain('File path contains null byte (potential security issue)');
    });

    test('should reject null bytes in middle of path', () => {
      const errors = validateFilePath('/path/\x00/file.db');
      expect(errors).toContain('File path contains null byte (potential security issue)');
    });
  });

  describe('invalid characters', () => {
    test('should reject < character', () => {
      const errors = validateFilePath('file<name.db');
      expect(errors).toContain('File path contains invalid characters');
    });

    test('should reject > character', () => {
      const errors = validateFilePath('file>name.db');
      expect(errors).toContain('File path contains invalid characters');
    });

    test('should reject | character', () => {
      const errors = validateFilePath('file|name.db');
      expect(errors).toContain('File path contains invalid characters');
    });

    test('should reject " character', () => {
      const errors = validateFilePath('file"name.db');
      expect(errors).toContain('File path contains invalid characters');
    });
  });

  describe('length validation', () => {
    test('should reject paths over 260 characters', () => {
      const longPath = 'a'.repeat(261);
      const errors = validateFilePath(longPath);
      expect(errors).toContain('File path is too long (max 260 characters)');
    });

    test('should accept paths at 260 characters', () => {
      const maxPath = 'a'.repeat(260);
      const errors = validateFilePath(maxPath);
      expect(errors).not.toContain('File path is too long (max 260 characters)');
    });
  });

  describe('valid paths', () => {
    test('should accept simple filename', () => {
      const errors = validateFilePath('dataset.db');
      expect(errors).toHaveLength(0);
    });

    test('should accept absolute Unix path', () => {
      const errors = validateFilePath('/home/user/data/dataset.db');
      expect(errors).toHaveLength(0);
    });

    test('should accept relative path without traversal', () => {
      const errors = validateFilePath('./data/dataset.db');
      expect(errors).toHaveLength(0);
    });

    test('should accept Windows-style path', () => {
      const errors = validateFilePath('C:\\Users\\data\\dataset.db');
      expect(errors).toHaveLength(0);
    });
  });
});

describe('isFilePathSafe', () => {
  test('should return true for valid paths', () => {
    expect(isFilePathSafe('dataset.db')).toBe(true);
    expect(isFilePathSafe('/home/user/dataset.db')).toBe(true);
  });

  test('should return false for path traversal', () => {
    expect(isFilePathSafe('../etc/passwd')).toBe(false);
    expect(isFilePathSafe('foo/../bar')).toBe(false);
  });

  test('should return false for null bytes', () => {
    expect(isFilePathSafe('file\x00.db')).toBe(false);
  });

  test('should return false for empty paths', () => {
    expect(isFilePathSafe('')).toBe(false);
  });

  test('should return false for too long paths', () => {
    expect(isFilePathSafe('a'.repeat(261))).toBe(false);
  });

  test('should return false for invalid characters', () => {
    expect(isFilePathSafe('file<name.db')).toBe(false);
    expect(isFilePathSafe('file>name.db')).toBe(false);
    expect(isFilePathSafe('file|name.db')).toBe(false);
    expect(isFilePathSafe('file"name.db')).toBe(false);
  });

  test('should be consistent with validateFilePath', () => {
    const testCases = ['valid.db', '../etc/passwd', 'file\x00.db', '', 'a'.repeat(261), 'file<name.db'];

    for (const path of testCases) {
      const safeResult = isFilePathSafe(path);
      const validateResult = validateFilePath(path).length === 0;
      expect(safeResult).toBe(validateResult);
    }
  });
});
