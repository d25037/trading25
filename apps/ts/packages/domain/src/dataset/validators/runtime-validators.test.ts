import { describe, expect, it } from 'bun:test';
import {
  safeValidateStockDataArray,
  safeValidateStockInfo,
  validateMarketType,
  validateStockData,
  validateStockInfo,
} from './runtime-validators';

const validStockData = {
  code: '7203',
  date: new Date('2024-01-15'),
  open: 2500,
  high: 2600,
  low: 2400,
  close: 2550,
  volume: 1000000,
};

const validStockInfo = {
  code: '7203',
  companyName: 'Toyota',
  companyNameEnglish: 'Toyota Motor',
  marketCode: 'prime',
  marketName: 'Prime',
  sector17Code: '0050',
  sector17Name: 'Automobiles',
  sector33Code: '3700',
  sector33Name: 'Transport',
  scaleCategory: 'Large70',
  listedDate: new Date('1949-05-16'),
};

describe('validateStockData', () => {
  it('validates valid data', () => {
    const result = validateStockData(validStockData);
    expect(result.code).toBe('7203');
  });

  it('throws for invalid data', () => {
    expect(() => validateStockData({ ...validStockData, open: -1 })).toThrow('validation failed');
  });
});

describe('validateStockInfo', () => {
  it('validates valid info', () => {
    const result = validateStockInfo(validStockInfo);
    expect(result.code).toBe('7203');
  });

  it('throws for invalid info', () => {
    expect(() => validateStockInfo({ ...validStockInfo, code: '' })).toThrow('validation failed');
  });
});

describe('validateMarketType', () => {
  it('validates valid market types', () => {
    expect(validateMarketType('prime')).toBe('prime');
    expect(validateMarketType('standard')).toBe('standard');
    expect(validateMarketType('growth')).toBe('growth');
  });

  it('throws for invalid market type', () => {
    expect(() => validateMarketType('invalid')).toThrow('Invalid market type');
  });
});

describe('safeValidateStockInfo', () => {
  it('returns success for valid data', () => {
    const result = safeValidateStockInfo(validStockInfo);
    expect(result.success).toBe(true);
  });

  it('returns failure for invalid data', () => {
    const result = safeValidateStockInfo({ ...validStockInfo, code: '' });
    expect(result.success).toBe(false);
  });
});

describe('safeValidateStockDataArray', () => {
  it('separates valid and invalid entries', () => {
    const data = [validStockData, { ...validStockData, open: -1 }];
    const result = safeValidateStockDataArray(data);
    expect(result.valid).toHaveLength(1);
    expect(result.invalid).toHaveLength(1);
    expect(result.invalid[0]?.index).toBe(1);
  });
});
