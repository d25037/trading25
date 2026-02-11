import { describe, expect, it } from 'vitest';
import { formatConstraints } from './signalConstraints';

describe('formatConstraints', () => {
  it('formats only numeric constraints', () => {
    const result = formatConstraints({
      gt: 0,
      ge: 1,
      lt: null as unknown as number,
      le: undefined,
    });

    expect(result).toEqual(['>0', '>=1']);
  });

  it('returns empty array when constraints are absent', () => {
    expect(formatConstraints(undefined)).toEqual([]);
  });

  it('formats upper-bound numeric constraints', () => {
    const result = formatConstraints({
      gt: null as unknown as number,
      ge: undefined,
      lt: 9,
      le: 10,
    });

    expect(result).toEqual(['<9', '<=10']);
  });
});
