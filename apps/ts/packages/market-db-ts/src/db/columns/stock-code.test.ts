/**
 * Tests for stock code normalization utilities
 */

import { describe, expect, test } from 'bun:test';
import { expandStockCode, isValidStockCode, normalizeStockCode } from './stock-code';

describe('normalizeStockCode', () => {
  test('converts 5-digit code to 4-digit by removing trailing 0', () => {
    expect(normalizeStockCode('72030')).toBe('7203');
    expect(normalizeStockCode('13010')).toBe('1301');
    expect(normalizeStockCode('86970')).toBe('8697');
  });

  test('handles alphanumeric codes correctly', () => {
    expect(normalizeStockCode('285A0')).toBe('285A');
    expect(normalizeStockCode('1A2B0')).toBe('1A2B');
  });

  test('returns 4-digit codes unchanged', () => {
    expect(normalizeStockCode('7203')).toBe('7203');
    expect(normalizeStockCode('1301')).toBe('1301');
    expect(normalizeStockCode('285A')).toBe('285A');
  });

  test('handles edge cases', () => {
    // 5-digit code not ending in 0 (should be unchanged)
    expect(normalizeStockCode('12345')).toBe('12345');

    // Empty string
    expect(normalizeStockCode('')).toBe('');

    // 3-digit code
    expect(normalizeStockCode('123')).toBe('123');

    // 6-digit code
    expect(normalizeStockCode('123456')).toBe('123456');
  });

  test('handles real JQuants stock codes', () => {
    // Major Japanese stocks
    expect(normalizeStockCode('72030')).toBe('7203'); // Toyota Motor
    expect(normalizeStockCode('67580')).toBe('6758'); // Sony Group
    expect(normalizeStockCode('99840')).toBe('9984'); // SoftBank Group
    expect(normalizeStockCode('84110')).toBe('8411'); // Mizuho Financial
    expect(normalizeStockCode('98610')).toBe('9861'); // Yoshinoya
  });
});

describe('expandStockCode', () => {
  test('converts 4-digit code to 5-digit by adding trailing 0', () => {
    expect(expandStockCode('7203')).toBe('72030');
    expect(expandStockCode('1301')).toBe('13010');
    expect(expandStockCode('8697')).toBe('86970');
  });

  test('handles alphanumeric codes correctly', () => {
    expect(expandStockCode('285A')).toBe('285A0');
    expect(expandStockCode('1A2B')).toBe('1A2B0');
  });

  test('returns 5-digit codes unchanged', () => {
    expect(expandStockCode('72030')).toBe('72030');
    expect(expandStockCode('13010')).toBe('13010');
    expect(expandStockCode('285A0')).toBe('285A0');
  });

  test('handles edge cases', () => {
    // 3-digit code (adds 0 only if length is exactly 4)
    expect(expandStockCode('123')).toBe('123');

    // 6-digit code
    expect(expandStockCode('123456')).toBe('123456');
  });
});

describe('isValidStockCode', () => {
  test('validates correct 4-digit stock codes', () => {
    expect(isValidStockCode('7203')).toBe(true);
    expect(isValidStockCode('1301')).toBe(true);
    expect(isValidStockCode('285A')).toBe(true);
    expect(isValidStockCode('1A2B')).toBe(true);
    expect(isValidStockCode('9999')).toBe(true);
  });

  test('rejects invalid stock codes', () => {
    // 5-digit codes (JQuants format)
    expect(isValidStockCode('72030')).toBe(false);

    // 3-digit codes
    expect(isValidStockCode('123')).toBe(false);

    // Empty string
    expect(isValidStockCode('')).toBe(false);

    // Lowercase letters
    expect(isValidStockCode('285a')).toBe(false);

    // Special characters
    expect(isValidStockCode('123!')).toBe(false);

    // Spaces
    expect(isValidStockCode('123 ')).toBe(false);
  });
});

describe('roundtrip conversion', () => {
  test('normalize -> expand -> normalize returns original 4-digit code', () => {
    const original = '7203';
    const expanded = expandStockCode(original);
    const normalized = normalizeStockCode(expanded);
    expect(normalized).toBe(original);
  });

  test('expand -> normalize -> expand returns original 5-digit code', () => {
    const original = '72030';
    const normalized = normalizeStockCode(original);
    const expanded = expandStockCode(normalized);
    expect(expanded).toBe(original);
  });

  test('handles multiple roundtrips correctly', () => {
    const codes = ['7203', '6758', '9984', '8411', '285A'];
    for (const code of codes) {
      const expanded = expandStockCode(code);
      const normalized = normalizeStockCode(expanded);
      const reExpanded = expandStockCode(normalized);
      const reNormalized = normalizeStockCode(reExpanded);
      expect(normalized).toBe(code);
      expect(reNormalized).toBe(code);
    }
  });
});
