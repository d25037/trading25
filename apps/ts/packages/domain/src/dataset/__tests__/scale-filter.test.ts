/**
 * Test for scaleCategories filtering functionality
 */
import { describe, expect, test } from 'bun:test';
import type { StockInfo } from '../types';
import { filterStocksByScaleCategories } from '../utils';

describe('filterStocksByScaleCategories', () => {
  const testStocks: StockInfo[] = [
    {
      code: '7203',
      companyName: 'Toyota',
      companyNameEnglish: 'Toyota Motor Corporation',
      marketCode: 'prime',
      marketName: 'Prime Market',
      sector17Code: '050',
      sector17Name: 'Manufacturing',
      sector33Code: '5010',
      sector33Name: 'Automotive',
      scaleCategory: 'TOPIX Core30',
      listedDate: new Date('1949-05-16'),
    },
    {
      code: '8306',
      companyName: 'MUFG',
      companyNameEnglish: 'Mitsubishi UFJ Financial Group',
      marketCode: 'prime',
      marketName: 'Prime Market',
      sector17Code: '070',
      sector17Name: 'Finance',
      sector33Code: '7050',
      sector33Name: 'Banking',
      scaleCategory: 'TOPIX Large70',
      listedDate: new Date('2005-10-01'),
    },
    {
      code: '2432',
      companyName: 'DeNA',
      companyNameEnglish: 'DeNA Co., Ltd.',
      marketCode: 'prime',
      marketName: 'Prime Market',
      sector17Code: '055',
      sector17Name: 'Information & Communication',
      sector33Code: '5250',
      sector33Name: 'Information Services',
      scaleCategory: 'TOPIX Mid400',
      listedDate: new Date('2005-02-15'),
    },
    {
      code: '1234',
      companyName: 'Test Small',
      companyNameEnglish: 'Test Small Company',
      marketCode: 'standard',
      marketName: 'Standard Market',
      sector17Code: '055',
      sector17Name: 'Information & Communication',
      sector33Code: '5250',
      sector33Name: 'Information Services',
      scaleCategory: 'TOPIX Small 2',
      listedDate: new Date('2010-01-01'),
    },
    {
      code: '5678',
      companyName: 'Test No Scale',
      companyNameEnglish: 'Test No Scale Company',
      marketCode: 'growth',
      marketName: 'Growth Market',
      sector17Code: '055',
      sector17Name: 'Information & Communication',
      sector33Code: '5250',
      sector33Name: 'Information Services',
      scaleCategory: '',
      listedDate: new Date('2020-01-01'),
    },
  ];

  test('should filter for single scale category', () => {
    const result = filterStocksByScaleCategories(testStocks, ['TOPIX Core30']);

    expect(result).toHaveLength(1);
    expect(result[0]?.code).toBe('7203');
    expect(result[0]?.scaleCategory).toBe('TOPIX Core30');
  });

  test('should filter for multiple scale categories', () => {
    const result = filterStocksByScaleCategories(testStocks, ['TOPIX Core30', 'TOPIX Large70']);

    expect(result).toHaveLength(2);
    expect(result.map((s) => s.code)).toEqual(['7203', '8306']);
    expect(result.map((s) => s.scaleCategory)).toEqual(['TOPIX Core30', 'TOPIX Large70']);
  });

  test('should filter for TOPIX Mid400', () => {
    const result = filterStocksByScaleCategories(testStocks, ['TOPIX Mid400']);

    expect(result).toHaveLength(1);
    expect(result[0]?.code).toBe('2432');
    expect(result[0]?.scaleCategory).toBe('TOPIX Mid400');
  });

  test('should filter for TOPIX Small 2', () => {
    const result = filterStocksByScaleCategories(testStocks, ['TOPIX Small 2']);

    expect(result).toHaveLength(1);
    expect(result[0]?.code).toBe('1234');
    expect(result[0]?.scaleCategory).toBe('TOPIX Small 2');
  });

  test('should return all stocks when no categories specified', () => {
    const result = filterStocksByScaleCategories(testStocks, []);

    expect(result).toHaveLength(5);
    expect(result).toEqual(testStocks);
  });

  test('should return empty array for non-existent category', () => {
    const result = filterStocksByScaleCategories(testStocks, ['Non-existent Category']);

    expect(result).toHaveLength(0);
  });

  test('should exclude stocks with empty scaleCategory', () => {
    const result = filterStocksByScaleCategories(testStocks, ['']);

    expect(result).toHaveLength(0);
  });

  test('should handle undefined/null categories array', () => {
    const result1 = filterStocksByScaleCategories(testStocks, undefined as unknown as string[]);
    const result2 = filterStocksByScaleCategories(testStocks, null as unknown as string[]);

    expect(result1).toEqual(testStocks);
    expect(result2).toEqual(testStocks);
  });

  test('should handle real preset configurations', () => {
    // Test topix100 preset (Core30 + Large70)
    const topix100 = filterStocksByScaleCategories(testStocks, ['TOPIX Core30', 'TOPIX Large70']);
    expect(topix100).toHaveLength(2);
    expect(topix100.map((s) => s.code)).toEqual(['7203', '8306']);

    // Test mid400 preset
    const mid400 = filterStocksByScaleCategories(testStocks, ['TOPIX Mid400']);
    expect(mid400).toHaveLength(1);
    expect(mid400[0]?.code).toBe('2432');

    // Test sample400TS2 preset
    const sample400TS2 = filterStocksByScaleCategories(testStocks, ['TOPIX Small 2']);
    expect(sample400TS2).toHaveLength(1);
    expect(sample400TS2[0]?.code).toBe('1234');
  });
});
