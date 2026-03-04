import { describe, expect, it } from 'bun:test';
import {
  assertArrayLength,
  assertMinArrayLength,
  getElement,
  getElementOrFail,
  getFirstElementOrFail,
  getLastElementOrFail,
  hasElementAt,
  isNonEmptyArray,
} from './array-helpers';

describe('hasElementAt', () => {
  it('returns true for valid index', () => {
    expect(hasElementAt([1, 2, 3], 0)).toBe(true);
    expect(hasElementAt([1, 2, 3], 2)).toBe(true);
  });

  it('returns false for out-of-range', () => {
    expect(hasElementAt([1, 2], 3)).toBe(false);
    expect(hasElementAt([1, 2], -1)).toBe(false);
  });
});

describe('isNonEmptyArray', () => {
  it('returns true for non-empty', () => {
    expect(isNonEmptyArray([1])).toBe(true);
  });

  it('returns false for empty', () => {
    expect(isNonEmptyArray([])).toBe(false);
  });
});

describe('getElement', () => {
  it('returns element at index', () => {
    expect(getElement([10, 20, 30], 1)).toBe(20);
  });

  it('returns undefined for invalid index', () => {
    expect(getElement([10], 5)).toBeUndefined();
  });
});

describe('getElementOrFail', () => {
  it('returns element at index', () => {
    expect(getElementOrFail([10, 20], 1)).toBe(20);
  });

  it('throws for invalid index', () => {
    expect(() => getElementOrFail([], 0)).toThrow();
  });

  it('throws with custom message', () => {
    expect(() => getElementOrFail([], 0, 'custom error')).toThrow('custom error');
  });
});

describe('getFirstElementOrFail', () => {
  it('returns first element', () => {
    expect(getFirstElementOrFail([42, 99])).toBe(42);
  });

  it('throws for empty array', () => {
    expect(() => getFirstElementOrFail([])).toThrow();
  });
});

describe('getLastElementOrFail', () => {
  it('returns last element', () => {
    expect(getLastElementOrFail([42, 99])).toBe(99);
  });

  it('throws for empty array', () => {
    expect(() => getLastElementOrFail([])).toThrow();
  });
});

describe('assertArrayLength', () => {
  it('returns array when length matches', () => {
    const arr = assertArrayLength([1, 2, 3], 3);
    expect(arr).toEqual([1, 2, 3]);
  });

  it('throws when length does not match', () => {
    expect(() => assertArrayLength([1, 2], 3)).toThrow();
  });
});

describe('assertMinArrayLength', () => {
  it('returns array when length >= min', () => {
    const arr = assertMinArrayLength([1, 2, 3], 2);
    expect(arr).toEqual([1, 2, 3]);
  });

  it('throws when length < min', () => {
    expect(() => assertMinArrayLength([1], 3)).toThrow();
  });
});
