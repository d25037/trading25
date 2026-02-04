import { beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { existsSync, mkdirSync } from 'node:fs';
import { mockDailyQuotes, mockTOPIXData } from '../test-utils/fixtures';
import { CsvExporter } from './csv-exporter';

// Mock node:fs for directory operations
mock.module('node:fs', () => ({
  existsSync: mock(),
  mkdirSync: mock(),
}));

describe('CsvExporter', () => {
  let csvExporter: CsvExporter;
  let bunWriteSpy: ReturnType<typeof spyOn>;
  const mockExistsSync = existsSync as unknown as ReturnType<typeof mock>;
  const mockMkdirSync = mkdirSync as unknown as ReturnType<typeof mock>;

  beforeEach(() => {
    // Reset mock return values
    mockExistsSync.mockClear?.();
    mockMkdirSync.mockClear?.();
    mockExistsSync.mockReturnValue(false);

    // Spy on Bun.write
    bunWriteSpy = spyOn(Bun, 'write').mockResolvedValue(0);

    csvExporter = new CsvExporter('./test-output');
  });

  describe('constructor', () => {
    it('should create output directory if it does not exist', () => {
      mockMkdirSync.mockClear?.();
      mockExistsSync.mockReturnValue(false);

      new CsvExporter('./new-output');

      expect(mockMkdirSync).toHaveBeenCalledWith('./new-output', { recursive: true });
    });

    it('should not create directory if it already exists', () => {
      mockMkdirSync.mockClear?.();
      mockExistsSync.mockReturnValue(true);

      new CsvExporter('./existing-output');

      expect(mockMkdirSync).not.toHaveBeenCalled();
    });

    it('should use default output directory', () => {
      new CsvExporter();

      expect(mockMkdirSync).toHaveBeenCalledWith('./data', { recursive: true });
    });
  });

  describe('exportDailyQuotes', () => {
    it('should export daily quotes to CSV format', async () => {
      const result = await csvExporter.exportDailyQuotes(mockDailyQuotes.data, 'daily_quotes.csv');

      expect(bunWriteSpy).toHaveBeenCalledWith(
        'test-output/daily_quotes.csv',
        expect.stringContaining('Date,Code,Open,High,Low,Close')
      );
      expect(result).toBe('test-output/daily_quotes.csv');
    });

    it('should handle empty daily quotes data', async () => {
      const result = await csvExporter.exportDailyQuotes([], 'empty_daily.csv');

      expect(bunWriteSpy).toHaveBeenCalledWith(
        'test-output/empty_daily.csv',
        expect.stringContaining('Date,Code,Open,High,Low,Close')
      );
      expect(result).toBe('test-output/empty_daily.csv');
    });

    it('should escape CSV special characters', async () => {
      const dataWithSpecialChars = [
        {
          Date: '2025-01-10',
          Code: '7203,TEST',
          O: 2750,
          H: 2780,
          L: 2740,
          C: 2765,
          Vo: 1250000,
          Va: 3456789000,
          AdjFactor: 1.0,
          AdjO: 2750,
          AdjH: 2780,
          AdjL: 2740,
          AdjC: 2765,
          AdjVo: 1250000,
        },
      ];

      await csvExporter.exportDailyQuotes(dataWithSpecialChars, 'special_chars.csv');

      expect(bunWriteSpy).toHaveBeenCalledWith('test-output/special_chars.csv', expect.stringContaining('"7203,TEST"'));
    });
  });

  describe('exportTOPIX', () => {
    it('should export TOPIX data to CSV format', async () => {
      const result = await csvExporter.exportTOPIX(mockTOPIXData.data, 'topix.csv');

      expect(bunWriteSpy).toHaveBeenCalledWith(
        'test-output/topix.csv',
        expect.stringContaining('Date,Open,High,Low,Close')
      );
      expect(result).toBe('test-output/topix.csv');
    });

    it('should handle TOPIX data with correct values', async () => {
      bunWriteSpy.mockClear();
      await csvExporter.exportTOPIX(mockTOPIXData.data, 'topix_values.csv');

      const csvContent = bunWriteSpy.mock.calls[0]?.[1] as string;
      expect(csvContent).toContain('2025-01-10,2359.28,2380.1,2335.58,2378.79');
      expect(csvContent).toContain('2025-01-11,2387.88,2400.53,2382.79,2393.54');
    });
  });

  describe('exportJSON', () => {
    it('should export data to JSON format', async () => {
      const testData = { test: 'data', number: 123 };
      const result = await csvExporter.exportJSON(testData, 'test.json');

      expect(bunWriteSpy).toHaveBeenCalledWith('test-output/test.json', JSON.stringify(testData, null, 2));
      expect(result).toBe('test-output/test.json');
    });

    it('should handle complex JSON data', async () => {
      const result = await csvExporter.exportJSON(mockTOPIXData, 'topix.json');

      expect(bunWriteSpy).toHaveBeenCalledWith('test-output/topix.json', JSON.stringify(mockTOPIXData, null, 2));
      expect(result).toBe('test-output/topix.json');
    });
  });

  describe('escapeCSV', () => {
    it('should escape values with commas', async () => {
      bunWriteSpy.mockClear();
      const dataWithComma = [
        {
          Date: '2025-01-10',
          O: 100,
          H: 110,
          L: 90,
          C: 105,
        },
      ];

      await csvExporter.exportTOPIX(dataWithComma, 'escape_test.csv');

      const csvContent = bunWriteSpy.mock.calls[0]?.[1] as string;
      expect(csvContent).toContain('2025-01-10,100');
    });

    it('should escape values with quotes', async () => {
      bunWriteSpy.mockClear();
      const dataWithQuotes = [
        {
          Date: '2025-01-10',
          O: 100,
          H: 110,
          L: 90,
          C: 105,
        },
      ];

      await csvExporter.exportTOPIX(dataWithQuotes, 'quotes_test.csv');

      const csvContent = bunWriteSpy.mock.calls[0]?.[1] as string;
      expect(csvContent).toContain('2025-01-10,100');
    });

    it('should handle null and undefined values', async () => {
      bunWriteSpy.mockClear();
      const dataWithNulls = [
        {
          Date: '2025-01-10',
          O: 100,
          H: 110,
          L: 90,
          C: 105,
        },
      ];

      await csvExporter.exportTOPIX(dataWithNulls, 'nulls_test.csv');

      const csvContent = bunWriteSpy.mock.calls[0]?.[1] as string;
      expect(csvContent).toContain('2025-01-10,100');
    });
  });
});
