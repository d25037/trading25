import { describe, expect, it } from 'vitest';
import { compareOptionalTimestampDesc, compareTimestampDesc } from './dateComparators';

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

describe('compareOptionalTimestampDesc', () => {
  it('sorts missing timestamps after real timestamps', () => {
    const values = ['2026-02-10T08:00:00Z', null, '2026-02-11T08:00:00Z', undefined];

    expect([...values].sort(compareOptionalTimestampDesc)).toEqual([
      '2026-02-11T08:00:00Z',
      '2026-02-10T08:00:00Z',
      null,
      undefined,
    ]);
  });
});
