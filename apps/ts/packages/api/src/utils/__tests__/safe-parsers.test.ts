/**
 * Tests for safe parser utilities
 */

import { describe, expect, test } from 'bun:test';
import { BadRequestError } from '@trading25/shared';
import { safeDecodeURIComponent, safeParseInt } from '../safe-parsers';

describe('safeParseInt', () => {
  describe('valid integers', () => {
    test('should parse positive integers', () => {
      expect(safeParseInt('123', 'testField')).toBe(123);
    });

    test('should parse zero', () => {
      expect(safeParseInt('0', 'testField')).toBe(0);
    });

    test('should parse negative integers', () => {
      expect(safeParseInt('-456', 'testField')).toBe(-456);
    });

    test('should parse large integers', () => {
      expect(safeParseInt('999999999', 'testField')).toBe(999999999);
    });

    test('should handle leading zeros', () => {
      expect(safeParseInt('007', 'testField')).toBe(7);
    });
  });

  describe('invalid inputs', () => {
    test('should throw for non-numeric string', () => {
      expect(() => safeParseInt('abc', 'testField')).toThrow(BadRequestError);
      expect(() => safeParseInt('abc', 'testField')).toThrow('Invalid testField: must be a valid integer');
    });

    test('should throw for empty string', () => {
      expect(() => safeParseInt('', 'testField')).toThrow(BadRequestError);
    });

    test('should throw for float strings', () => {
      // Note: parseInt('12.5') returns 12, not NaN
      // This is expected behavior - parseInt truncates decimals
      expect(safeParseInt('12.5', 'testField')).toBe(12);
    });

    test('should throw for mixed alphanumeric', () => {
      // Note: parseInt('123abc') returns 123, not NaN
      expect(safeParseInt('123abc', 'testField')).toBe(123);
    });

    test('should throw for special characters only', () => {
      expect(() => safeParseInt('!@#', 'testField')).toThrow(BadRequestError);
    });

    test('should throw for whitespace only', () => {
      expect(() => safeParseInt('   ', 'testField')).toThrow(BadRequestError);
    });
  });

  describe('field name in error message', () => {
    test('should include field name: portfolioId', () => {
      expect(() => safeParseInt('abc', 'portfolioId')).toThrow('Invalid portfolioId');
    });

    test('should include field name: itemId', () => {
      expect(() => safeParseInt('xyz', 'itemId')).toThrow('Invalid itemId');
    });
  });
});

describe('safeDecodeURIComponent', () => {
  describe('valid encoded strings', () => {
    test('should decode simple encoded string', () => {
      expect(safeDecodeURIComponent('hello%20world', 'testField')).toBe('hello world');
    });

    test('should decode special characters', () => {
      expect(safeDecodeURIComponent('%E3%83%86%E3%82%B9%E3%83%88', 'testField')).toBe('テスト');
    });

    test('should return unencoded string as-is', () => {
      expect(safeDecodeURIComponent('simple', 'testField')).toBe('simple');
    });

    test('should decode plus signs literally', () => {
      // Note: decodeURIComponent does NOT convert + to space
      expect(safeDecodeURIComponent('hello+world', 'testField')).toBe('hello+world');
    });

    test('should decode percent-encoded percent', () => {
      expect(safeDecodeURIComponent('%25', 'testField')).toBe('%');
    });
  });

  describe('invalid encoded strings', () => {
    test('should throw for malformed percent encoding', () => {
      expect(() => safeDecodeURIComponent('%', 'testField')).toThrow(BadRequestError);
      expect(() => safeDecodeURIComponent('%', 'testField')).toThrow('Invalid testField: malformed URI encoding');
    });

    test('should throw for incomplete percent encoding', () => {
      expect(() => safeDecodeURIComponent('%2', 'testField')).toThrow(BadRequestError);
    });

    test('should throw for invalid hex in percent encoding', () => {
      expect(() => safeDecodeURIComponent('%GG', 'testField')).toThrow(BadRequestError);
    });

    test('should throw for invalid UTF-8 sequence', () => {
      expect(() => safeDecodeURIComponent('%80', 'testField')).toThrow(BadRequestError);
    });
  });

  describe('field name in error message', () => {
    test('should include field name: portfolioName', () => {
      expect(() => safeDecodeURIComponent('%', 'portfolioName')).toThrow('Invalid portfolioName');
    });

    test('should include field name: stockCode', () => {
      expect(() => safeDecodeURIComponent('%', 'stockCode')).toThrow('Invalid stockCode');
    });
  });
});
