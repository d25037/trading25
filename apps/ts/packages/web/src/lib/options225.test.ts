import { describe, expect, it } from 'vitest';
import type { N225OptionItem } from '@/types/options225';
import {
  buildOptionsComparator,
  filterOptionsItems,
  formatOptionsNumber,
  formatOptionsRange,
  getOptionRowKey,
  parseOptionsNumericInput,
  resolveSelectedOptionRowKey,
  summarizeFilteredOptions,
} from './options225';

function createItem(overrides: Partial<N225OptionItem>): N225OptionItem {
  return {
    date: '2026-03-18',
    code: '130060018',
    wholeDayOpen: null,
    wholeDayHigh: null,
    wholeDayLow: null,
    wholeDayClose: null,
    nightSessionOpen: null,
    nightSessionHigh: null,
    nightSessionLow: null,
    nightSessionClose: null,
    daySessionOpen: null,
    daySessionHigh: null,
    daySessionLow: null,
    daySessionClose: null,
    volume: null,
    openInterest: null,
    turnoverValue: null,
    contractMonth: null,
    strikePrice: null,
    onlyAuctionVolume: null,
    emergencyMarginTriggerDivision: null,
    emergencyMarginTriggerLabel: null,
    putCallDivision: null,
    putCallLabel: null,
    lastTradingDay: null,
    specialQuotationDay: null,
    settlementPrice: null,
    theoreticalPrice: null,
    baseVolatility: null,
    underlyingPrice: null,
    impliedVolatility: null,
    interestRate: null,
    ...overrides,
  };
}

describe('options225 helpers', () => {
  it('formats nullable numbers and ranges', () => {
    expect(formatOptionsNumber(null)).toBe('-');
    expect(formatOptionsNumber(1234.567, 1)).toBe('1,234.6');
    expect(formatOptionsRange(null, null)).toBe('-');
    expect(formatOptionsRange(34000, 35000, 0)).toBe('34,000 - 35,000');
  });

  it('parses numeric input with empty and invalid values', () => {
    expect(parseOptionsNumericInput('')).toBeNull();
    expect(parseOptionsNumericInput('   ')).toBeNull();
    expect(parseOptionsNumericInput('abc')).toBeNull();
    expect(parseOptionsNumericInput('34500.5')).toBe(34500.5);
  });

  it('builds a stable row key', () => {
    expect(getOptionRowKey(createItem({ code: '130060018', emergencyMarginTriggerDivision: '002' }))).toBe(
      '130060018:002'
    );
    expect(getOptionRowKey(createItem({ code: '130060018', emergencyMarginTriggerDivision: null }))).toBe(
      '130060018:none'
    );
  });

  it('sorts by each supported metric and handles null values', () => {
    const baseLeft = createItem({
      code: 'A',
      contractMonth: '2026-04',
      openInterest: 100,
      volume: 10,
      strikePrice: 34000,
      impliedVolatility: 20,
      wholeDayClose: 12,
    });
    const baseRight = createItem({
      code: 'B',
      contractMonth: '2026-05',
      openInterest: 200,
      volume: 20,
      strikePrice: 35000,
      impliedVolatility: 22,
      wholeDayClose: 14,
    });

    expect(buildOptionsComparator('openInterest', 'desc')(baseLeft, baseRight)).toBeGreaterThan(0);
    expect(buildOptionsComparator('volume', 'asc')(baseLeft, baseRight)).toBeLessThan(0);
    expect(buildOptionsComparator('strikePrice', 'asc')(baseLeft, baseRight)).toBeLessThan(0);
    expect(buildOptionsComparator('impliedVolatility', 'asc')(baseLeft, baseRight)).toBeLessThan(0);
    expect(buildOptionsComparator('wholeDayClose', 'asc')(baseLeft, baseRight)).toBeLessThan(0);

    const nullLeft = createItem({
      code: 'C',
      openInterest: null,
      volume: null,
      strikePrice: null,
      contractMonth: null,
    });
    const nullRight = createItem({ code: 'D', openInterest: 1, volume: 1, strikePrice: 1, contractMonth: '2026-04' });
    expect(buildOptionsComparator('openInterest', 'asc')(nullLeft, nullRight)).toBeGreaterThan(0);
    expect(buildOptionsComparator('volume', 'asc')(nullLeft, nullRight)).toBeGreaterThan(0);
    expect(buildOptionsComparator('strikePrice', 'asc')(nullLeft, nullRight)).toBeGreaterThan(0);
  });

  it('uses contract month, strike, and code as tie breakers', () => {
    const itemA = createItem({
      code: '130060018',
      contractMonth: '2026-04',
      openInterest: 100,
      strikePrice: 34000,
    });
    const itemB = createItem({
      code: '130060019',
      contractMonth: '2026-05',
      openInterest: 100,
      strikePrice: 34000,
    });
    const itemC = createItem({
      code: '130060020',
      contractMonth: '2026-05',
      openInterest: 100,
      strikePrice: 35000,
    });

    expect(buildOptionsComparator('openInterest', 'asc')(itemA, itemB)).toBeLessThan(0);
    expect(buildOptionsComparator('openInterest', 'asc')(itemB, itemC)).toBeLessThan(0);
    expect(buildOptionsComparator('openInterest', 'asc')(itemB, { ...itemB, code: '130060021' })).toBeLessThan(0);
  });

  it('filters, sorts, and summarizes option rows', () => {
    const items = [
      createItem({
        code: 'put-row',
        putCallDivision: '1',
        contractMonth: '2026-04',
        strikePrice: 34000,
        openInterest: 90,
      }),
      createItem({
        code: 'call-row-low',
        putCallDivision: '2',
        contractMonth: '2026-05',
        strikePrice: 35000,
        openInterest: 30,
      }),
      createItem({
        code: 'call-row-high',
        putCallDivision: '2',
        contractMonth: '2026-05',
        strikePrice: 35100,
        openInterest: 120,
      }),
    ];

    const filtered = filterOptionsItems(items, {
      putCall: 'call',
      contractMonth: '2026-05',
      strikeMin: 34900,
      strikeMax: 35050,
      sortBy: 'openInterest',
      order: 'desc',
    });

    expect(filtered.map((item) => item.code)).toEqual(['call-row-low']);
    expect(summarizeFilteredOptions(filtered)).toEqual({
      filteredCount: 1,
      putCount: 0,
      callCount: 1,
      totalOpenInterest: 30,
    });
  });

  it('resolves selected row keys against the filtered list', () => {
    const items = [
      createItem({ code: 'first', emergencyMarginTriggerDivision: '001' }),
      createItem({ code: 'second', emergencyMarginTriggerDivision: '002' }),
    ];

    expect(resolveSelectedOptionRowKey([], null)).toBeNull();
    expect(resolveSelectedOptionRowKey(items, null)).toBe('first:001');
    expect(resolveSelectedOptionRowKey(items, 'second:002')).toBe('second:002');
    expect(resolveSelectedOptionRowKey(items, 'missing')).toBe('first:001');
  });
});
