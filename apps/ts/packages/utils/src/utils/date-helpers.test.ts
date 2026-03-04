/**
 * Tests for date-helpers
 */

import { describe, expect, it } from 'bun:test';
import { dateRangeToISO, toISODateString, toISODateStringOrDefault, toISODateStringOrNull } from './date-helpers';

describe('date-helpers', () => {
  describe('toISODateString', () => {
    it('should convert valid date to YYYY-MM-DD format', () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      const result = toISODateString(date);
      expect(result).toBe('2024-01-15');
    });

    it('should handle dates with different times correctly', () => {
      const date1 = new Date('2024-12-31T23:59:59.999Z');
      const date2 = new Date('2024-12-31T00:00:00.000Z');

      expect(toISODateString(date1)).toBe('2024-12-31');
      expect(toISODateString(date2)).toBe('2024-12-31');
    });

    it('should handle edge dates correctly', () => {
      const date1 = new Date('1970-01-01T00:00:00.000Z');
      const date2 = new Date('2099-12-31T23:59:59.999Z');

      expect(toISODateString(date1)).toBe('1970-01-01');
      expect(toISODateString(date2)).toBe('2099-12-31');
    });

    it('should handle leap year dates', () => {
      const date = new Date('2024-02-29T12:00:00.000Z');
      expect(toISODateString(date)).toBe('2024-02-29');
    });

    it('should throw error for invalid date', () => {
      const invalidDate = new Date('invalid');
      expect(() => toISODateString(invalidDate)).toThrow('Invalid date');
    });

    it('should handle dates across timezone boundaries', () => {
      // Date created in different timezone context
      const date = new Date(2024, 0, 15, 0, 0, 0); // Local time
      const result = toISODateString(date);

      // Result should be in YYYY-MM-DD format (exact value depends on timezone)
      expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    });
  });

  describe('toISODateStringOrDefault', () => {
    it('should convert valid date to YYYY-MM-DD format', () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      const result = toISODateStringOrDefault(date);
      expect(result).toBe('2024-01-15');
    });

    it('should return empty string for null date by default', () => {
      const result = toISODateStringOrDefault(null);
      expect(result).toBe('');
    });

    it('should return empty string for undefined date by default', () => {
      const result = toISODateStringOrDefault(undefined);
      expect(result).toBe('');
    });

    it('should return custom default for null date', () => {
      const result = toISODateStringOrDefault(null, 'N/A');
      expect(result).toBe('N/A');
    });

    it('should return custom default for undefined date', () => {
      const result = toISODateStringOrDefault(undefined, 'Unknown');
      expect(result).toBe('Unknown');
    });

    it('should throw error for invalid date (not null/undefined)', () => {
      const invalidDate = new Date('invalid');
      expect(() => toISODateStringOrDefault(invalidDate, 'N/A')).toThrow('Invalid date');
    });

    it('should handle explicit empty string as default', () => {
      const result = toISODateStringOrDefault(null, '');
      expect(result).toBe('');
    });
  });

  describe('toISODateStringOrNull', () => {
    it('should convert valid date to YYYY-MM-DD format', () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      const result = toISODateStringOrNull(date);
      expect(result).toBe('2024-01-15');
    });

    it('should return null for null date', () => {
      const result = toISODateStringOrNull(null);
      expect(result).toBeNull();
    });

    it('should return null for undefined date', () => {
      const result = toISODateStringOrNull(undefined);
      expect(result).toBeNull();
    });

    it('should throw error for invalid date (not null/undefined)', () => {
      const invalidDate = new Date('invalid');
      expect(() => toISODateStringOrNull(invalidDate)).toThrow('Invalid date');
    });

    it('should preserve type narrowing', () => {
      const date: Date | null = new Date('2024-01-15T10:30:00.000Z');
      const result = toISODateStringOrNull(date);

      // TypeScript should infer result as string | null
      if (result !== null) {
        expect(typeof result).toBe('string');
      }
    });
  });

  describe('dateRangeToISO', () => {
    it('should convert valid date range', () => {
      const range = {
        from: new Date('2024-01-01T00:00:00.000Z'),
        to: new Date('2024-12-31T23:59:59.999Z'),
      };

      const result = dateRangeToISO(range);

      expect(result.from).toBe('2024-01-01');
      expect(result.to).toBe('2024-12-31');
    });

    it('should allow same from and to dates', () => {
      const range = {
        from: new Date('2024-06-15T00:00:00.000Z'),
        to: new Date('2024-06-15T23:59:59.999Z'),
      };

      const result = dateRangeToISO(range);

      expect(result.from).toBe('2024-06-15');
      expect(result.to).toBe('2024-06-15');
    });

    it('should throw error if from > to', () => {
      const range = {
        from: new Date('2024-12-31T00:00:00.000Z'),
        to: new Date('2024-01-01T00:00:00.000Z'),
      };

      expect(() => dateRangeToISO(range)).toThrow('Invalid date range: from (2024-12-31) > to (2024-01-01)');
    });

    it('should throw error for invalid from date', () => {
      const range = {
        from: new Date('invalid'),
        to: new Date('2024-12-31T00:00:00.000Z'),
      };

      expect(() => dateRangeToISO(range)).toThrow('Invalid date');
    });

    it('should throw error for invalid to date', () => {
      const range = {
        from: new Date('2024-01-01T00:00:00.000Z'),
        to: new Date('invalid'),
      };

      expect(() => dateRangeToISO(range)).toThrow('Invalid date');
    });

    it('should handle year boundary ranges', () => {
      const range = {
        from: new Date('2023-12-31T00:00:00.000Z'),
        to: new Date('2024-01-01T00:00:00.000Z'),
      };

      const result = dateRangeToISO(range);

      expect(result.from).toBe('2023-12-31');
      expect(result.to).toBe('2024-01-01');
    });

    it('should handle multi-year ranges', () => {
      const range = {
        from: new Date('2020-01-01T00:00:00.000Z'),
        to: new Date('2024-12-31T00:00:00.000Z'),
      };

      const result = dateRangeToISO(range);

      expect(result.from).toBe('2020-01-01');
      expect(result.to).toBe('2024-12-31');
    });
  });

  describe('Integration scenarios', () => {
    it('should handle typical database insertion pattern', () => {
      const stockData = {
        code: '7203',
        date: new Date('2024-01-15T00:00:00.000Z'),
        price: 1500,
      };

      const dateStr = toISODateString(stockData.date);

      expect(dateStr).toBe('2024-01-15');
      expect(typeof dateStr).toBe('string');
    });

    it('should handle optional financial statement dates', () => {
      const statement = {
        code: '7203',
        disclosedDate: null as Date | null,
      };

      const dateStr = toISODateStringOrDefault(statement.disclosedDate, 'Not Disclosed');

      expect(dateStr).toBe('Not Disclosed');
    });

    it('should handle SQL WHERE clause parameters', () => {
      const dateRange = {
        from: new Date('2024-01-01T00:00:00.000Z'),
        to: new Date('2024-03-31T23:59:59.999Z'),
      };

      const { from, to } = dateRangeToISO(dateRange);

      // Simulate SQL parameter substitution
      const params = [from, to];

      expect(params[0]).toBe('2024-01-01');
      expect(params[1]).toBe('2024-03-31');
    });

    it('should handle functional array mapping', () => {
      const dates = [
        new Date('2024-01-01T00:00:00.000Z'),
        new Date('2024-01-02T00:00:00.000Z'),
        new Date('2024-01-03T00:00:00.000Z'),
      ];

      const dateStrings = dates.map(toISODateString);

      expect(dateStrings).toEqual(['2024-01-01', '2024-01-02', '2024-01-03']);
    });

    it('should handle Set operations with dates', () => {
      const tradingDays = [
        new Date('2024-01-01T00:00:00.000Z'),
        new Date('2024-01-02T00:00:00.000Z'),
        new Date('2024-01-01T00:00:00.000Z'), // Duplicate
      ];

      const uniqueDates = new Set(tradingDays.map(toISODateString));

      expect(uniqueDates.size).toBe(2);
      expect(uniqueDates.has('2024-01-01')).toBe(true);
      expect(uniqueDates.has('2024-01-02')).toBe(true);
    });

    it('should handle filter operations with dates', () => {
      const targetDate = '2024-01-15';
      const dates = [
        new Date('2024-01-14T00:00:00.000Z'),
        new Date('2024-01-15T00:00:00.000Z'),
        new Date('2024-01-16T00:00:00.000Z'),
      ];

      const filtered = dates.filter((d) => toISODateString(d) !== targetDate);

      expect(filtered).toHaveLength(2);
      expect(toISODateString(filtered[0] as Date)).toBe('2024-01-14');
      expect(toISODateString(filtered[1] as Date)).toBe('2024-01-16');
    });
  });
});
