import { describe, expect, it } from 'bun:test';
import type { MarginData, SectorData, StatementsData, StockData, StockInfo, TopixData } from '../types';
import {
  isFilePathSafe,
  validateDataArray,
  validateDatasetConfig,
  validateDatasetConsistency,
  validateDateRange,
  validateFilePath,
  validateMarginData,
  validateSectorCode,
  validateSectorData,
  validateStatementsData,
  validateStockCode,
  validateStockData,
  validateStockInfo,
  validateTopixData,
} from '../validators';

const validStock: StockInfo = {
  code: '7203',
  companyName: 'Toyota',
  companyNameEnglish: 'Toyota Motor',
  marketCode: '0111',
  marketName: 'Prime',
  sector17Code: '0050',
  sector17Name: 'Automobiles',
  sector33Code: '3700',
  sector33Name: 'Transport Equipment',
  scaleCategory: 'Large70',
  listedDate: new Date('1949-05-16'),
};

const validStockData: StockData = {
  code: '7203',
  date: new Date('2024-01-15'),
  open: 2500,
  high: 2600,
  low: 2400,
  close: 2550,
  volume: 1000000,
};

describe('validateDatasetConfig', () => {
  it('passes for valid config', () => {
    const errors = validateDatasetConfig({
      outputPath: '/path/to/output.db',
      markets: ['prime'],
    } as never);
    expect(errors).toHaveLength(0);
  });

  it('catches empty outputPath', () => {
    const errors = validateDatasetConfig({
      outputPath: '',
      markets: ['prime'],
    } as never);
    expect(errors).toContain('outputPath is required');
  });

  it('catches empty markets', () => {
    const errors = validateDatasetConfig({
      outputPath: '/path',
      markets: [],
    } as never);
    expect(errors.some((e) => e.includes('market'))).toBe(true);
  });

  it('catches invalid markets', () => {
    const errors = validateDatasetConfig({
      outputPath: '/path',
      markets: ['invalid'],
    } as never);
    expect(errors.some((e) => e.includes('Invalid markets'))).toBe(true);
  });

  it('catches startDate >= endDate', () => {
    const errors = validateDatasetConfig({
      outputPath: '/path',
      markets: ['prime'],
      startDate: new Date('2025-01-01'),
      endDate: new Date('2024-01-01'),
    } as never);
    expect(errors.some((e) => e.includes('startDate must be before'))).toBe(true);
  });

  it('catches negative maxStocks', () => {
    const errors = validateDatasetConfig({
      outputPath: '/path',
      markets: ['prime'],
      maxStocks: -1,
    } as never);
    expect(errors.some((e) => e.includes('maxStocks'))).toBe(true);
  });
});

describe('validateStockInfo', () => {
  it('passes for valid stock', () => {
    expect(validateStockInfo(validStock)).toHaveLength(0);
  });

  it('catches invalid code', () => {
    const errors = validateStockInfo({ ...validStock, code: 'abc' });
    expect(errors.some((e) => e.includes('code'))).toBe(true);
  });

  it('catches empty company name', () => {
    const errors = validateStockInfo({ ...validStock, companyName: '' });
    expect(errors.some((e) => e.includes('name'))).toBe(true);
  });

  it('catches invalid market code', () => {
    const errors = validateStockInfo({ ...validStock, marketCode: 'ab' });
    expect(errors.some((e) => e.includes('Market code'))).toBe(true);
  });

  it('catches future listed date', () => {
    const future = new Date();
    future.setFullYear(future.getFullYear() + 1);
    const errors = validateStockInfo({ ...validStock, listedDate: future });
    expect(errors.some((e) => e.includes('future'))).toBe(true);
  });
});

describe('validateStockData', () => {
  it('passes for valid data', () => {
    expect(validateStockData(validStockData)).toHaveLength(0);
  });

  it('catches negative open', () => {
    const errors = validateStockData({ ...validStockData, open: -1 });
    expect(errors.some((e) => e.includes('Open'))).toBe(true);
  });

  it('catches high < low', () => {
    const errors = validateStockData({ ...validStockData, high: 2300, low: 2400 });
    expect(errors.some((e) => e.includes('High'))).toBe(true);
  });

  it('catches invalid adjustment factor', () => {
    const errors = validateStockData({ ...validStockData, adjustmentFactor: -1 });
    expect(errors.some((e) => e.includes('Adjustment'))).toBe(true);
  });

  it('passes with valid adjustment factor', () => {
    expect(validateStockData({ ...validStockData, adjustmentFactor: 1.0 })).toHaveLength(0);
  });
});

describe('validateMarginData', () => {
  it('passes for valid data', () => {
    const data: MarginData = {
      code: '7203',
      date: new Date('2024-01-15'),
      longMarginVolume: 1000,
      shortMarginVolume: 500,
    };
    expect(validateMarginData(data)).toHaveLength(0);
  });

  it('passes with null volumes', () => {
    const data: MarginData = {
      code: '7203',
      date: new Date('2024-01-15'),
      longMarginVolume: null,
      shortMarginVolume: null,
    };
    expect(validateMarginData(data)).toHaveLength(0);
  });

  it('catches negative volume', () => {
    const data: MarginData = {
      code: '7203',
      date: new Date('2024-01-15'),
      longMarginVolume: -100,
      shortMarginVolume: null,
    };
    expect(validateMarginData(data).length).toBeGreaterThan(0);
  });
});

describe('validateTopixData', () => {
  const validTopix: TopixData = {
    date: new Date('2024-01-15'),
    open: 2500,
    high: 2600,
    low: 2400,
    close: 2550,
  };

  it('passes for valid data', () => {
    expect(validateTopixData(validTopix)).toHaveLength(0);
  });

  it('catches high < low', () => {
    const errors = validateTopixData({ ...validTopix, high: 2300 });
    expect(errors.some((e) => e.includes('High'))).toBe(true);
  });

  it('catches negative open', () => {
    const errors = validateTopixData({ ...validTopix, open: -1 });
    expect(errors.length).toBeGreaterThan(0);
  });
});

describe('validateSectorData', () => {
  const validSector: SectorData = {
    sectorCode: '3700',
    sectorName: 'Transport',
    date: new Date('2024-01-15'),
    open: 100,
    high: 110,
    low: 90,
    close: 105,
  };

  it('passes for valid data', () => {
    expect(validateSectorData(validSector)).toHaveLength(0);
  });

  it('catches invalid sector code', () => {
    const errors = validateSectorData({ ...validSector, sectorCode: 'ab' });
    expect(errors.some((e) => e.includes('Sector code'))).toBe(true);
  });

  it('catches empty sector name', () => {
    const errors = validateSectorData({ ...validSector, sectorName: '' });
    expect(errors.some((e) => e.includes('Sector name'))).toBe(true);
  });

  it('catches high < low', () => {
    const errors = validateSectorData({ ...validSector, high: 80 });
    expect(errors.some((e) => e.includes('High'))).toBe(true);
  });
});

describe('validateStatementsData', () => {
  const validStatements: StatementsData = {
    code: '7203',
    disclosedDate: new Date('2024-01-15'),
    earningsPerShare: 250.5,
    profit: 150000,
    equity: 2000000,
    typeOfCurrentPeriod: 'FY',
    typeOfDocument: 'AnnualSecuritiesReport',
    nextYearForecastEarningsPerShare: null,
    bps: 1200,
    sales: 1000000,
    operatingProfit: 200000,
    ordinaryProfit: 210000,
    operatingCashFlow: null,
    dividendFY: 110,
    forecastEps: null,
    investingCashFlow: null,
    financingCashFlow: null,
    cashAndEquivalents: null,
    totalAssets: 5000000,
    sharesOutstanding: 1000000,
    treasuryShares: null,
  };

  it('passes for valid data', () => {
    expect(validateStatementsData(validStatements)).toHaveLength(0);
  });

  it('catches invalid code', () => {
    const errors = validateStatementsData({ ...validStatements, code: 'abc' });
    expect(errors.some((e) => e.includes('code'))).toBe(true);
  });

  it('catches empty typeOfCurrentPeriod', () => {
    const errors = validateStatementsData({ ...validStatements, typeOfCurrentPeriod: '' });
    expect(errors.some((e) => e.includes('period'))).toBe(true);
  });
});

describe('validateDataArray', () => {
  it('validates all valid items', () => {
    const result = validateDataArray([validStockData, validStockData], validateStockData, 'quote');
    expect(result.isValid).toBe(true);
    expect(result.validCount).toBe(2);
    expect(result.invalidCount).toBe(0);
  });

  it('reports invalid items', () => {
    const bad = { ...validStockData, open: -1 };
    const result = validateDataArray([validStockData, bad], validateStockData, 'quote');
    expect(result.isValid).toBe(false);
    expect(result.validCount).toBe(1);
    expect(result.invalidCount).toBe(1);
    expect(result.errors.length).toBeGreaterThan(0);
  });
});

describe('validateDateRange', () => {
  it('passes for valid range', () => {
    expect(validateDateRange({ from: new Date('2024-01-01'), to: new Date('2024-12-31') })).toHaveLength(0);
  });

  it('catches from >= to', () => {
    const errors = validateDateRange({
      from: new Date('2024-12-31'),
      to: new Date('2024-01-01'),
    });
    expect(errors.some((e) => e.includes('before'))).toBe(true);
  });
});

describe('validateFilePath', () => {
  it('passes for valid path', () => {
    expect(validateFilePath('/path/to/file.db')).toHaveLength(0);
  });

  it('catches empty path', () => {
    expect(validateFilePath('')).toContain('File path is required');
  });

  it('catches null bytes', () => {
    const errors = validateFilePath('/path/to/file\0.db');
    expect(errors.some((e) => e.includes('null byte'))).toBe(true);
  });

  it('catches path traversal', () => {
    const errors = validateFilePath('/path/../etc/passwd');
    expect(errors.some((e) => e.includes('traversal'))).toBe(true);
  });

  it('catches invalid characters', () => {
    const errors = validateFilePath('/path/to/<file>');
    expect(errors.some((e) => e.includes('invalid characters'))).toBe(true);
  });

  it('catches too long path', () => {
    const errors = validateFilePath('a'.repeat(261));
    expect(errors.some((e) => e.includes('too long'))).toBe(true);
  });
});

describe('isFilePathSafe', () => {
  it('returns true for valid path', () => {
    expect(isFilePathSafe('/path/to/file.db')).toBe(true);
  });

  it('returns false for dangerous path', () => {
    expect(isFilePathSafe('/path/../etc/passwd')).toBe(false);
  });
});

describe('validateStockCode', () => {
  it('passes for valid code', () => {
    expect(validateStockCode('7203')).toHaveLength(0);
    expect(validateStockCode('285A')).toHaveLength(0);
  });

  it('catches empty code', () => {
    expect(validateStockCode('')).toContain('Stock code is required');
  });

  it('catches invalid format', () => {
    const errors = validateStockCode('abc');
    expect(errors.some((e) => e.includes('4 characters'))).toBe(true);
  });
});

describe('validateSectorCode', () => {
  it('passes for valid code', () => {
    expect(validateSectorCode('3700')).toHaveLength(0);
  });

  it('catches empty code', () => {
    expect(validateSectorCode('')).toContain('Sector code is required');
  });

  it('catches invalid format', () => {
    const errors = validateSectorCode('abc');
    expect(errors.some((e) => e.includes('4 digits'))).toBe(true);
  });
});

describe('validateDatasetConsistency', () => {
  it('passes for consistent data', () => {
    const stocks = [validStock];
    const quotes = new Map([['7203', [validStockData]]]);
    const result = validateDatasetConsistency({ stocks, quotes });
    expect(result.isValid).toBe(true);
  });

  it('catches quote data for unknown stock', () => {
    const stocks = [validStock];
    const quotes = new Map([
      ['7203', [validStockData]],
      ['9999', [{ ...validStockData, code: '9999' }]],
    ]);
    const result = validateDatasetConsistency({ stocks, quotes });
    expect(result.isValid).toBe(false);
    expect(result.errors.some((e) => e.includes('9999'))).toBe(true);
  });

  it('warns about stocks without quotes', () => {
    const stocks = [validStock, { ...validStock, code: '6758' }];
    const quotes = new Map([['7203', [validStockData]]]);
    const result = validateDatasetConsistency({ stocks, quotes });
    expect(result.warnings.length).toBeGreaterThan(0);
  });

  it('catches margin data for unknown stock', () => {
    const stocks = [validStock];
    const quotes = new Map([['7203', [validStockData]]]);
    const margin = new Map([
      [
        '9999',
        [
          {
            code: '9999',
            date: new Date('2024-01-15'),
            longMarginVolume: 100,
            shortMarginVolume: 50,
          },
        ],
      ],
    ]);
    const result = validateDatasetConsistency({ stocks, quotes, margin });
    expect(result.isValid).toBe(false);
  });
});
