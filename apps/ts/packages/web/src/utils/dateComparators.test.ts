import { describe, expect, it } from 'vitest';
import { compareTimestampDesc } from './dateComparators';

describe('compareTimestampDesc', () => {
  it('sorts newest timestamps first', () => {
    const values = ['2026-02-10T08:00:00Z', '2026-02-11T08:00:00Z', '2026-02-09T08:00:00Z'];

    expect([...values].sort(compareTimestampDesc)).toEqual([
      '2026-02-11T08:00:00Z',
      '2026-02-10T08:00:00Z',
      '2026-02-09T08:00:00Z',
    ]);
  });
});
