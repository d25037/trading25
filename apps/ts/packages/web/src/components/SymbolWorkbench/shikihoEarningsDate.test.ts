import { describe, expect, test } from 'vitest';
import { getShikihoEarningsDateState } from './shikihoEarningsDate';

describe('getShikihoEarningsDateState', () => {
  const now = new Date('2026-07-15T03:00:00.000Z');

  test.each([
    ['2026-07-30', 'neutral', 15, 'あと15日'],
    ['2026-07-29', 'yellow', 14, 'あと14日'],
    ['2026-07-23', 'yellow', 8, 'あと8日'],
    ['2026-07-22', 'orange', 7, 'あと7日'],
    ['2026-07-19', 'orange', 4, 'あと4日'],
    ['2026-07-18', 'red', 3, 'あと3日'],
    ['2026-07-15', 'red', 0, '本日'],
    ['2026-07-14', 'past', -1, '予定日を過ぎています'],
  ] as const)('classifies %s as %s', (date, state, daysRemaining, remainingDayText) => {
    expect(getShikihoEarningsDateState(date, now)).toEqual({ state, daysRemaining, remainingDayText });
  });

  test('uses the JST calendar day when UTC is still on the previous date', () => {
    const afterJstMidnight = new Date('2026-07-15T15:30:00.000Z');

    expect(getShikihoEarningsDateState('2026-07-31', afterJstMidnight)).toMatchObject({
      state: 'neutral',
      daysRemaining: 15,
      remainingDayText: 'あと15日',
    });
  });
});
