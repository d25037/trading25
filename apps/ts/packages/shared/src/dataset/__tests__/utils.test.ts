import { describe, expect, it } from 'bun:test';
import type { StockInfo } from '../types';
import {
  chunkArray,
  createCustomDateRange,
  createDateRange,
  createErrorSummary,
  filterStocksByMarkets,
  filterStocksByScaleCategories,
  filterStocksBySector33Requirements,
  filterStocksBySectors,
  filterStocksExcludingScaleCategories,
  filterStocksExcludingSectorCodes,
  formatDateForApi,
  formatFileSize,
  getDateRangeStrings,
  getDaysInRange,
  getErrorMessage,
  getMarketCode,
  getMarketCodes,
  getMarketType,
  getUniqueValues,
  groupStocksByMarket,
  groupStocksBySector,
  isDateInRange,
  isDefined,
  isNonEmptyArray,
  isNonEmptyString,
  isValidDateRange,
  isValidSectorCode,
  randomSample,
  removeDuplicatesBy,
  safeJsonStringify,
  sanitizeFilePath,
  shuffleArray,
} from '../utils';

const makeStock = (overrides: Partial<StockInfo> = {}): StockInfo => ({
  code: '7203',
  companyName: 'Toyota',
  companyNameEnglish: 'Toyota Motor',
  marketCode: '0111',
  marketName: 'Prime',
  sector17Code: '0050',
  sector17Name: 'Automobiles',
  sector33Code: '3700',
  sector33Name: 'Transportation Equipment',
  scaleCategory: 'TOPIX Large70',
  listedDate: new Date('1949-05-16'),
  ...overrides,
});

describe('date utilities', () => {
  it('createDateRange creates range for N years', () => {
    const range = createDateRange(5);
    const diffYears = (range.to.getTime() - range.from.getTime()) / (365.25 * 24 * 60 * 60 * 1000);
    expect(diffYears).toBeCloseTo(5, 0);
  });

  it('createCustomDateRange from strings', () => {
    const range = createCustomDateRange('2024-01-01', '2024-12-31');
    expect(range.from.getFullYear()).toBe(2024);
    expect(range.to.getMonth()).toBe(11);
  });

  it('createCustomDateRange from Dates', () => {
    const from = new Date('2024-01-01');
    const to = new Date('2024-12-31');
    const range = createCustomDateRange(from, to);
    expect(range.from).toBe(from);
    expect(range.to).toBe(to);
  });

  it('formatDateForApi returns YYYY-MM-DD', () => {
    const date = new Date('2024-06-15T12:00:00Z');
    expect(formatDateForApi(date)).toBe('2024-06-15');
  });

  it('getDateRangeStrings returns formatted strings', () => {
    const range = createCustomDateRange('2024-01-15', '2024-06-30');
    const strings = getDateRangeStrings(range);
    expect(strings.from).toBe('2024-01-15');
    expect(strings.to).toBe('2024-06-30');
  });

  it('isDateInRange checks correctly', () => {
    const range = createCustomDateRange('2024-01-01', '2024-12-31');
    expect(isDateInRange(new Date('2024-06-15'), range)).toBe(true);
    expect(isDateInRange(new Date('2023-06-15'), range)).toBe(false);
  });

  it('getDaysInRange calculates days', () => {
    const range = createCustomDateRange('2024-01-01', '2024-01-11');
    expect(getDaysInRange(range)).toBe(10);
  });

  it('isValidDateRange checks from <= to', () => {
    const valid = createCustomDateRange('2024-01-01', '2024-12-31');
    expect(isValidDateRange(valid)).toBe(true);
    const invalid = createCustomDateRange('2024-12-31', '2024-01-01');
    expect(isValidDateRange(invalid)).toBe(false);
  });
});

describe('market utilities', () => {
  it('getMarketCode maps types', () => {
    expect(getMarketCode('prime')).toBe('0111');
    expect(getMarketCode('standard')).toBe('0112');
    expect(getMarketCode('growth')).toBe('0113');
  });

  it('getMarketType maps codes', () => {
    expect(getMarketType('0111')).toBe('prime');
    expect(getMarketType('0112')).toBe('standard');
    expect(getMarketType('9999')).toBeNull();
  });

  it('getMarketCodes maps array', () => {
    expect(getMarketCodes(['prime', 'growth'])).toEqual(['0111', '0113']);
  });
});

describe('stock filter utilities', () => {
  const stocks = [
    makeStock({ code: '7203', marketCode: '0111', sector33Code: '3700', scaleCategory: 'TOPIX Large70' }),
    makeStock({ code: '6758', marketCode: '0112', sector33Code: '3650', scaleCategory: 'TOPIX Mid400' }),
    makeStock({ code: '4755', marketCode: '0113', sector33Code: '5250', scaleCategory: '' }),
  ];

  it('filterStocksByMarkets filters by market type', () => {
    // marketCode is '0111' etc, but filter expects 'prime' etc as MarketType
    const primeStocks = filterStocksByMarkets(
      [makeStock({ marketCode: 'prime' }), makeStock({ code: '6758', marketCode: 'growth' })],
      ['prime']
    );
    expect(primeStocks).toHaveLength(1);
  });

  it('filterStocksBySectors filters by sector codes', () => {
    const result = filterStocksBySectors(stocks, ['3700']);
    expect(result).toHaveLength(1);
    expect(result[0]?.code).toBe('7203');
  });

  it('filterStocksBySector33Requirements excludes empty sectors', () => {
    const withEmpty = [makeStock({ sector33Code: '3700' }), makeStock({ code: '9999', sector33Code: '' })];
    expect(filterStocksBySector33Requirements(withEmpty, true)).toHaveLength(1);
    expect(filterStocksBySector33Requirements(withEmpty, false)).toHaveLength(2);
  });

  it('filterStocksExcludingSectorCodes excludes specified sectors', () => {
    const result = filterStocksExcludingSectorCodes(stocks, ['3700']);
    expect(result).toHaveLength(2);
  });

  it('filterStocksExcludingSectorCodes returns all when empty exclude', () => {
    expect(filterStocksExcludingSectorCodes(stocks, [])).toHaveLength(3);
  });

  it('filterStocksByScaleCategories filters by scale', () => {
    const result = filterStocksByScaleCategories(stocks, ['TOPIX Large70']);
    expect(result).toHaveLength(1);
  });

  it('filterStocksByScaleCategories returns all when empty', () => {
    expect(filterStocksByScaleCategories(stocks, [])).toHaveLength(3);
  });

  it('filterStocksExcludingScaleCategories excludes categories', () => {
    const result = filterStocksExcludingScaleCategories(stocks, ['TOPIX Large70']);
    expect(result).toHaveLength(2);
  });

  it('filterStocksExcludingScaleCategories returns all when empty', () => {
    expect(filterStocksExcludingScaleCategories(stocks, [])).toHaveLength(3);
  });
});

describe('groupStocksByMarket', () => {
  it('groups by market type', () => {
    const stocks = [
      makeStock({ marketCode: '0111' }),
      makeStock({ code: '6758', marketCode: '0112' }),
      makeStock({ code: '4755', marketCode: '0111' }),
      makeStock({ code: '9999', marketCode: '9999' }),
    ];
    const groups = groupStocksByMarket(stocks);
    expect(groups.prime).toHaveLength(2);
    expect(groups.standard).toHaveLength(1);
    expect(groups.growth).toHaveLength(0);
  });
});

describe('groupStocksBySector', () => {
  it('groups by sector code', () => {
    const stocks = [
      makeStock({ sector33Code: '3700' }),
      makeStock({ code: '6758', sector33Code: '3650' }),
      makeStock({ code: '7267', sector33Code: '3700' }),
    ];
    const groups = groupStocksBySector(stocks);
    expect(groups['3700']).toHaveLength(2);
    expect(groups['3650']).toHaveLength(1);
  });
});

describe('validation utilities', () => {
  it('isValidSectorCode validates 4-digit codes', () => {
    expect(isValidSectorCode('3700')).toBe(true);
    expect(isValidSectorCode('abc')).toBe(false);
    expect(isValidSectorCode('12345')).toBe(false);
  });
});

describe('string utilities', () => {
  it('sanitizeFilePath removes invalid chars', () => {
    expect(sanitizeFilePath('file<name>:test')).toBe('file_name__test');
    expect(sanitizeFilePath('path\\to\\file')).toBe('path/to/file');
    expect(sanitizeFilePath('path//to///file')).toBe('path/to/file');
  });

  it('formatFileSize formats bytes', () => {
    expect(formatFileSize(0)).toBe('0.0 B');
    expect(formatFileSize(1024)).toBe('1.0 KB');
    expect(formatFileSize(1024 * 1024)).toBe('1.0 MB');
    expect(formatFileSize(1536)).toBe('1.5 KB');
  });
});

describe('array utilities', () => {
  it('chunkArray splits array', () => {
    expect(chunkArray([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
    expect(chunkArray([], 3)).toEqual([]);
  });

  it('getUniqueValues removes duplicates', () => {
    expect(getUniqueValues([1, 2, 2, 3, 3, 3])).toEqual([1, 2, 3]);
  });

  it('removeDuplicatesBy uses key function', () => {
    const items = [
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
      { id: 1, name: 'c' },
    ];
    const result = removeDuplicatesBy(items, (item) => item.id);
    expect(result).toHaveLength(2);
    expect(result[0]?.name).toBe('a');
  });
});

describe('error utilities', () => {
  it('safeJsonStringify handles circular refs', () => {
    const obj: Record<string, unknown> = { a: 1 };
    obj.self = obj;
    const result = safeJsonStringify(obj);
    expect(result).toContain('[Circular]');
  });

  it('safeJsonStringify works for normal objects', () => {
    expect(safeJsonStringify({ a: 1 })).toBe('{"a":1}');
  });

  it('getErrorMessage extracts from various types', () => {
    expect(getErrorMessage(new Error('test'))).toBe('test');
    expect(getErrorMessage('string')).toBe('string');
    expect(getErrorMessage(42)).toBe('42');
  });

  it('createErrorSummary formats errors', () => {
    expect(createErrorSummary([])).toBe('No errors');
    expect(createErrorSummary(['one'])).toBe('one');
    expect(createErrorSummary(['one', 'two'])).toContain('2 errors');
  });
});

describe('type guards', () => {
  it('isDefined checks null/undefined', () => {
    expect(isDefined(0)).toBe(true);
    expect(isDefined('')).toBe(true);
    expect(isDefined(null)).toBe(false);
    expect(isDefined(undefined)).toBe(false);
  });

  it('isNonEmptyString checks strings', () => {
    expect(isNonEmptyString('hello')).toBe(true);
    expect(isNonEmptyString('')).toBe(false);
    expect(isNonEmptyString(null)).toBe(false);
  });

  it('isNonEmptyArray checks arrays', () => {
    expect(isNonEmptyArray([1])).toBe(true);
    expect(isNonEmptyArray([])).toBe(false);
    expect(isNonEmptyArray(null)).toBe(false);
  });
});

describe('sampling utilities', () => {
  it('shuffleArray returns same elements', () => {
    const arr = [1, 2, 3, 4, 5];
    const shuffled = shuffleArray(arr, 42);
    expect(shuffled).toHaveLength(5);
    expect(shuffled.sort()).toEqual([1, 2, 3, 4, 5]);
  });

  it('shuffleArray with seed is deterministic', () => {
    const arr = [1, 2, 3, 4, 5];
    const a = shuffleArray(arr, 42);
    const b = shuffleArray(arr, 42);
    expect(a).toEqual(b);
  });

  it('randomSample returns subset', () => {
    const arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const sample = randomSample(arr, 3, 42);
    expect(sample).toHaveLength(3);
  });

  it('randomSample handles edge cases', () => {
    expect(randomSample([1, 2, 3], 0)).toEqual([]);
    expect(randomSample([1, 2, 3], 5)).toEqual([1, 2, 3]);
  });
});
