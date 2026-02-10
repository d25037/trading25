/**
 * Dataset V2 - Data Validators
 * Data validation functions for dataset operations
 */

import type {
  DatasetConfig,
  DateRange,
  MarginData,
  SectorData,
  StatementsData,
  StockData,
  StockInfo,
  TopixData,
} from './types';

// ===== CONFIGURATION VALIDATION =====

/**
 * Validate dataset configuration
 */
export function validateDatasetConfig(config: DatasetConfig): string[] {
  const errors: string[] = [];

  // Required fields
  if (!config.outputPath || config.outputPath.trim().length === 0) {
    errors.push('outputPath is required');
  }

  if (!config.markets || config.markets.length === 0) {
    errors.push('At least one market must be specified');
  } else {
    const validMarkets = ['prime', 'standard', 'growth'];
    const invalidMarkets = config.markets.filter((m) => !validMarkets.includes(m));
    if (invalidMarkets.length > 0) {
      errors.push(`Invalid markets: ${invalidMarkets.join(', ')}`);
    }
  }

  // Date validation
  if (config.startDate && config.endDate && config.startDate >= config.endDate) {
    errors.push('startDate must be before endDate');
  }

  if (config.startDate && config.startDate > new Date()) {
    errors.push('startDate cannot be in the future');
  }

  // Numeric validation
  if (config.maxStocks !== undefined && config.maxStocks <= 0) {
    errors.push('maxStocks must be positive');
  }

  return errors;
}

// ===== DATA VALIDATION =====

/**
 * Validate stock information
 */
export function validateStockInfo(stock: StockInfo): string[] {
  const errors: string[] = [];

  if (!stock.code || !/^\d[0-9A-Z]\d[0-9A-Z]$/.test(stock.code)) {
    errors.push('Stock code must be 4 characters (e.g., 7203 or 285A)');
  }

  if (!stock.companyName || stock.companyName.trim().length === 0) {
    errors.push('Company name is required');
  }

  if (!stock.marketCode || !/^\d{4}$/.test(stock.marketCode)) {
    errors.push('Market code must be 4 digits');
  }

  if (!stock.marketName || stock.marketName.trim().length === 0) {
    errors.push('Market name is required');
  }

  if (!stock.sector33Code || !/^\d{4}$/.test(stock.sector33Code)) {
    errors.push('Sector33 code must be 4 digits');
  }

  if (!stock.sector33Name || stock.sector33Name.trim().length === 0) {
    errors.push('Sector33 name is required');
  }

  if (!stock.listedDate || Number.isNaN(stock.listedDate.getTime())) {
    errors.push('Valid listed date is required');
  } else if (stock.listedDate > new Date()) {
    errors.push('Listed date cannot be in the future');
  }

  return errors;
}

/**
 * Validate stock price data
 */
export function validateStockData(data: StockData): string[] {
  const errors: string[] = [];

  errors.push(...validateStockBasicFields(data));
  errors.push(...validateStockPriceFields(data));
  errors.push(...validateStockLogicalConstraints(data));
  errors.push(...validateStockOptionalFields(data));

  return errors;
}

function validateStockBasicFields(data: StockData): string[] {
  const errors: string[] = [];

  if (!data.code || !/^\d[0-9A-Z]\d[0-9A-Z]$/.test(data.code)) {
    errors.push('Stock code must be 4 characters');
  }

  if (!data.date || Number.isNaN(data.date.getTime())) {
    errors.push('Valid date is required');
  }

  return errors;
}

function validateStockPriceFields(data: StockData): string[] {
  const errors: string[] = [];

  if (typeof data.open !== 'number' || data.open < 0) {
    errors.push('Open price must be a non-negative number');
  }

  if (typeof data.high !== 'number' || data.high < 0) {
    errors.push('High price must be a non-negative number');
  }

  if (typeof data.low !== 'number' || data.low < 0) {
    errors.push('Low price must be a non-negative number');
  }

  if (typeof data.close !== 'number' || data.close < 0) {
    errors.push('Close price must be a non-negative number');
  }

  if (typeof data.volume !== 'number' || data.volume < 0) {
    errors.push('Volume must be a non-negative number');
  }

  return errors;
}

function validateStockLogicalConstraints(data: StockData): string[] {
  const errors: string[] = [];

  if (data.high < data.low) {
    errors.push('High price must be greater than or equal to low price');
  }

  if (data.high < data.open || data.high < data.close) {
    errors.push('High price must be greater than or equal to open and close prices');
  }

  if (data.low > data.open || data.low > data.close) {
    errors.push('Low price must be less than or equal to open and close prices');
  }

  return errors;
}

function validateStockOptionalFields(data: StockData): string[] {
  const errors: string[] = [];

  if (data.adjustmentFactor !== undefined) {
    if (typeof data.adjustmentFactor !== 'number' || data.adjustmentFactor <= 0) {
      errors.push('Adjustment factor must be a positive number');
    }
  }

  return errors;
}

/**
 * Validate margin data
 */
export function validateMarginData(data: MarginData): string[] {
  const errors: string[] = [];

  if (!data.code || !/^\d[0-9A-Z]\d[0-9A-Z]$/.test(data.code)) {
    errors.push('Stock code must be 4 characters');
  }

  if (!data.date || Number.isNaN(data.date.getTime())) {
    errors.push('Valid date is required');
  }

  if (data.longMarginVolume !== null && (typeof data.longMarginVolume !== 'number' || data.longMarginVolume < 0)) {
    errors.push('Long margin volume must be a non-negative number or null');
  }

  if (data.shortMarginVolume !== null && (typeof data.shortMarginVolume !== 'number' || data.shortMarginVolume < 0)) {
    errors.push('Short margin volume must be a non-negative number or null');
  }

  return errors;
}

/**
 * Validate TOPIX data
 */
export function validateTopixData(data: TopixData): string[] {
  const errors: string[] = [];

  errors.push(...validateTopixBasicFields(data));
  errors.push(...validateTopixValueFields(data));
  errors.push(...validateTopixLogicalConstraints(data));

  return errors;
}

function validateTopixBasicFields(data: TopixData): string[] {
  const errors: string[] = [];

  if (!data.date || Number.isNaN(data.date.getTime())) {
    errors.push('Valid date is required');
  }

  return errors;
}

function validateTopixValueFields(data: TopixData): string[] {
  const errors: string[] = [];

  if (typeof data.open !== 'number' || data.open < 0) {
    errors.push('Open value must be a non-negative number');
  }

  if (typeof data.high !== 'number' || data.high < 0) {
    errors.push('High value must be a non-negative number');
  }

  if (typeof data.low !== 'number' || data.low < 0) {
    errors.push('Low value must be a non-negative number');
  }

  if (typeof data.close !== 'number' || data.close < 0) {
    errors.push('Close value must be a non-negative number');
  }

  // Note: JQuants API does not provide volume for TOPIX data

  return errors;
}

function validateTopixLogicalConstraints(data: TopixData): string[] {
  const errors: string[] = [];

  if (data.high < data.low) {
    errors.push('High value must be greater than or equal to low value');
  }

  if (data.high < data.open || data.high < data.close) {
    errors.push('High value must be greater than or equal to open and close values');
  }

  if (data.low > data.open || data.low > data.close) {
    errors.push('Low value must be less than or equal to open and close values');
  }

  return errors;
}

/**
 * Validate sector data
 */
export function validateSectorData(data: SectorData): string[] {
  const errors: string[] = [];

  errors.push(...validateSectorBasicFields(data));
  errors.push(...validateSectorValueFields(data));
  errors.push(...validateSectorLogicalConstraints(data));

  return errors;
}

function validateSectorBasicFields(data: SectorData): string[] {
  const errors: string[] = [];

  if (!data.sectorCode || !/^\d{4}$/.test(data.sectorCode)) {
    errors.push('Sector code must be 4 digits');
  }

  if (!data.sectorName || data.sectorName.trim().length === 0) {
    errors.push('Sector name is required');
  }

  if (!data.date || Number.isNaN(data.date.getTime())) {
    errors.push('Valid date is required');
  }

  return errors;
}

function validateSectorValueFields(data: SectorData): string[] {
  const errors: string[] = [];

  if (typeof data.open !== 'number' || data.open < 0) {
    errors.push('Open value must be a non-negative number');
  }

  if (typeof data.high !== 'number' || data.high < 0) {
    errors.push('High value must be a non-negative number');
  }

  if (typeof data.low !== 'number' || data.low < 0) {
    errors.push('Low value must be a non-negative number');
  }

  if (typeof data.close !== 'number' || data.close < 0) {
    errors.push('Close value must be a non-negative number');
  }

  // Note: JQuants API does not provide volume for sector indices

  return errors;
}

function validateSectorLogicalConstraints(data: SectorData): string[] {
  const errors: string[] = [];

  if (data.high < data.low) {
    errors.push('High value must be greater than or equal to low value');
  }

  return errors;
}

/**
 * Validate statements data
 */
export function validateStatementsData(data: StatementsData): string[] {
  const errors: string[] = [];

  if (!data.code || !/^\d[0-9A-Z]\d[0-9A-Z]$/.test(data.code)) {
    errors.push('Stock code must be 4 characters');
  }

  if (!data.disclosedDate || Number.isNaN(data.disclosedDate.getTime())) {
    errors.push('Valid disclosed date is required');
  }

  if (data.earningsPerShare !== null && typeof data.earningsPerShare !== 'number') {
    errors.push('Earnings per share must be a number or null');
  }

  if (!data.typeOfCurrentPeriod || data.typeOfCurrentPeriod.trim().length === 0) {
    errors.push('Type of current period is required');
  }

  if (!data.typeOfDocument || data.typeOfDocument.trim().length === 0) {
    errors.push('Type of document is required');
  }

  return errors;
}

// ===== BATCH VALIDATION =====

/**
 * Validate array of data with detailed error reporting
 */
export function validateDataArray<T>(
  data: T[],
  validator: (item: T) => string[],
  itemName: string = 'item'
): { isValid: boolean; errors: string[]; validCount: number; invalidCount: number } {
  const allErrors: string[] = [];
  let validCount = 0;
  let invalidCount = 0;

  data.forEach((item, index) => {
    const itemErrors = validator(item);
    if (itemErrors.length === 0) {
      validCount++;
    } else {
      invalidCount++;
      itemErrors.forEach((error) => {
        allErrors.push(`${itemName} ${index + 1}: ${error}`);
      });
    }
  });

  return {
    isValid: invalidCount === 0,
    errors: allErrors,
    validCount,
    invalidCount,
  };
}

// ===== UTILITY VALIDATORS =====

/**
 * Validate date range
 */
export function validateDateRange(range: DateRange): string[] {
  const errors: string[] = [];

  if (!range.from || Number.isNaN(range.from.getTime())) {
    errors.push('Valid from date is required');
  }

  if (!range.to || Number.isNaN(range.to.getTime())) {
    errors.push('Valid to date is required');
  }

  if (range.from && range.to && range.from >= range.to) {
    errors.push('From date must be before to date');
  }

  if (range.from && range.from > new Date()) {
    errors.push('From date cannot be in the future');
  }

  if (range.to && range.to > new Date()) {
    errors.push('To date cannot be in the future');
  }

  return errors;
}

/**
 * Validate file path with security checks
 */
export function validateFilePath(path: string): string[] {
  const errors: string[] = [];

  if (!path || path.trim().length === 0) {
    errors.push('File path is required');
    return errors;
  }

  // Check for reasonable length (max 260 for Windows compatibility)
  if (path.length > 260) {
    errors.push('File path is too long (max 260 characters)');
  }

  // Security: Check for null bytes (potential path injection)
  if (path.includes('\0')) {
    errors.push('File path contains null byte (potential security issue)');
  }

  // Security: Check for path traversal attempts
  // Normalize backslashes first, then check for any ".." pattern
  const normalized = path.replace(/\\/g, '/');
  if (normalized.includes('..')) {
    errors.push('File path contains directory traversal pattern (..)');
  }

  // Check for invalid characters (basic validation)
  if (/[<>|"]/.test(path)) {
    errors.push('File path contains invalid characters');
  }

  return errors;
}

/**
 * Check if a file path is safe (returns boolean)
 * Convenience function that uses validateFilePath for consistency
 */
export function isFilePathSafe(path: string): boolean {
  return validateFilePath(path).length === 0;
}

/**
 * Validate stock code format
 */
export function validateStockCode(code: string): string[] {
  const errors: string[] = [];

  if (!code || code.trim().length === 0) {
    errors.push('Stock code is required');
  } else if (!/^\d[0-9A-Z]\d[0-9A-Z]$/.test(code.trim())) {
    errors.push('Stock code must be 4 characters (e.g., 7203 or 285A)');
  }

  return errors;
}

/**
 * Validate sector code format
 */
export function validateSectorCode(code: string): string[] {
  const errors: string[] = [];

  if (!code || code.trim().length === 0) {
    errors.push('Sector code is required');
  } else if (!/^\d{4}$/.test(code.trim())) {
    errors.push('Sector code must be exactly 4 digits');
  }

  return errors;
}

// ===== COMPREHENSIVE VALIDATION =====

/**
 * Validate complete dataset for consistency
 */
export function validateDatasetConsistency(data: {
  stocks: StockInfo[];
  quotes: Map<string, StockData[]>;
  margin?: Map<string, MarginData[]>;
  topix?: TopixData[];
  sectors?: SectorData[];
}): { isValid: boolean; errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];

  errors.push(...validateStockDataConsistency(data.stocks, data.quotes));
  errors.push(...validateMarginDataConsistency(data.stocks, data.margin));
  warnings.push(...validateQuoteDataCoverage(data.stocks, data.quotes));
  warnings.push(...validateDateConsistency(data.quotes, data.topix));

  return {
    isValid: errors.length === 0,
    errors,
    warnings,
  };
}

function validateStockDataConsistency(stocks: StockInfo[], quotes: Map<string, StockData[]>): string[] {
  const errors: string[] = [];

  for (const stockCode of quotes.keys()) {
    const stockExists = stocks.some((s) => s.code === stockCode);
    if (!stockExists) {
      errors.push(`Quote data exists for stock ${stockCode} but stock info is missing`);
    }
  }

  return errors;
}

function validateMarginDataConsistency(stocks: StockInfo[], margin?: Map<string, MarginData[]>): string[] {
  const errors: string[] = [];

  if (margin) {
    for (const stockCode of margin.keys()) {
      const stockExists = stocks.some((s) => s.code === stockCode);
      if (!stockExists) {
        errors.push(`Margin data exists for stock ${stockCode} but stock info is missing`);
      }
    }
  }

  return errors;
}

function validateQuoteDataCoverage(stocks: StockInfo[], quotes: Map<string, StockData[]>): string[] {
  const warnings: string[] = [];

  const stocksWithoutQuotes = stocks.filter((stock) => !quotes.has(stock.code) || quotes.get(stock.code)?.length === 0);

  if (stocksWithoutQuotes.length > 0) {
    warnings.push(`${stocksWithoutQuotes.length} stocks have no quote data`);
  }

  return warnings;
}

function validateDateConsistency(quotes: Map<string, StockData[]>, topix?: TopixData[]): string[] {
  const warnings: string[] = [];

  const allDates = extractTradingDates(quotes);

  if (topix && topix.length > 0) {
    const topixDates = new Set(topix.map((t) => t.date.toISOString().split('T')[0]));
    const missingTopixDates = Array.from(allDates).filter((date) => !topixDates.has(date));

    if (missingTopixDates.length > 0 && missingTopixDates.length < allDates.size * 0.1) {
      warnings.push(`TOPIX data missing for ${missingTopixDates.length} trading days`);
    }
  }

  return warnings;
}

function extractTradingDates(quotes: Map<string, StockData[]>): Set<string> {
  const allDates = new Set<string>();

  for (const quoteArray of quotes.values()) {
    quoteArray.forEach((quote) => {
      const dateStr = quote.date.toISOString().split('T')[0];
      if (dateStr) {
        allDates.add(dateStr);
      }
    });
  }

  return allDates;
}
