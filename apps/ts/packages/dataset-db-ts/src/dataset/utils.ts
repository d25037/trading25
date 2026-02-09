/**
 * Dataset V2 - Utility Functions
 * Consolidated utility functions for dataset operations
 */

import type { DateRange, MarketType, StockInfo } from './types';

// ===== DATE UTILITIES =====

/**
 * Create a date range for the last N years
 */
export function createDateRange(years: number = 10): DateRange {
  const to = new Date();
  const from = new Date();
  from.setFullYear(from.getFullYear() - years);

  return { from, to };
}

/**
 * Create a date range from specific dates
 */
export function createCustomDateRange(from: Date | string, to: Date | string): DateRange {
  return {
    from: typeof from === 'string' ? new Date(from) : from,
    to: typeof to === 'string' ? new Date(to) : to,
  };
}

/**
 * Format date for API requests (YYYY-MM-DD)
 */
export function formatDateForApi(date: Date): string {
  return date.toISOString().split('T')[0] || '';
}

/**
 * Get the current date range as strings
 */
export function getDateRangeStrings(dateRange: DateRange): { from: string; to: string } {
  return {
    from: formatDateForApi(dateRange.from),
    to: formatDateForApi(dateRange.to),
  };
}

/**
 * Check if a date is within a date range
 */
export function isDateInRange(date: Date, range: DateRange): boolean {
  return date >= range.from && date <= range.to;
}

/**
 * Get the number of days in a date range
 */
export function getDaysInRange(range: DateRange): number {
  const diffTime = Math.abs(range.to.getTime() - range.from.getTime());
  return Math.ceil(diffTime / (1000 * 60 * 60 * 24));
}

// ===== MARKET UTILITIES =====

/**
 * Convert market type to market code
 */
export function getMarketCode(market: MarketType): string {
  const marketMap: Record<MarketType, string> = {
    prime: '0111',
    standard: '0112',
    growth: '0113',
  };
  return marketMap[market];
}

/**
 * Convert market code to market type
 */
export function getMarketType(code: string): MarketType | null {
  const codeMap: Record<string, MarketType> = {
    '0111': 'prime',
    '0112': 'standard',
    '0113': 'growth',
  };
  return codeMap[code] || null;
}

/**
 * Get market codes for multiple market types
 */
export function getMarketCodes(markets: MarketType[]): string[] {
  return markets.map(getMarketCode);
}

/**
 * Filter stocks by market types
 */
export function filterStocksByMarkets(stocks: StockInfo[], markets: MarketType[]): StockInfo[] {
  // stock.marketCode contains MarketType values ('prime', 'standard', 'growth')
  // so we compare directly with the markets array
  return stocks.filter((stock) => markets.includes(stock.marketCode as MarketType));
}

/**
 * Filter stocks by sector codes (include only specified sectors)
 */
export function filterStocksBySectors(stocks: StockInfo[], sectorCodes: string[]): StockInfo[] {
  return stocks.filter((stock) => sectorCodes.includes(stock.sector33Code));
}

/**
 * Filter stocks requiring valid sector33Code
 */
export function filterStocksBySector33Requirements(
  stocks: StockInfo[],
  excludeEmptySector33: boolean = true
): StockInfo[] {
  if (!excludeEmptySector33) {
    return stocks;
  }

  return stocks.filter((stock) => {
    // Exclude stocks without sector33Code or with empty/whitespace-only sector33Code
    return stock.sector33Code && stock.sector33Code.trim() !== '';
  });
}

/**
 * Filter stocks excluding specific sector codes (exclude specified sectors)
 */
export function filterStocksExcludingSectorCodes(stocks: StockInfo[], excludeSectorCodes: string[] = []): StockInfo[] {
  if (excludeSectorCodes.length === 0) {
    return stocks;
  }

  return stocks.filter((stock) => {
    // Exclude stocks with sector codes in the exclude list
    return !excludeSectorCodes.includes(stock.sector33Code);
  });
}

/**
 * Filter stocks by TOPIX scale categories (include only specified categories)
 */
export function filterStocksByScaleCategories(stocks: StockInfo[], scaleCategories: string[]): StockInfo[] {
  if (!scaleCategories || scaleCategories.length === 0) {
    return stocks;
  }

  return stocks.filter((stock) => {
    // Include stocks whose scaleCategory matches any of the specified categories
    // Handle cases where scaleCategory might be empty or null
    return stock.scaleCategory && scaleCategories.includes(stock.scaleCategory);
  });
}

/**
 * Filter stocks excluding specific TOPIX scale categories (exclude specified categories)
 */
export function filterStocksExcludingScaleCategories(
  stocks: StockInfo[],
  excludeCategories: string[] = []
): StockInfo[] {
  if (excludeCategories.length === 0) {
    return stocks;
  }

  return stocks.filter((stock) => {
    // Exclude stocks whose scaleCategory is in the exclude list
    // Include stocks without scaleCategory (null/undefined/empty)
    return !stock.scaleCategory || !excludeCategories.includes(stock.scaleCategory);
  });
}

/**
 * Group stocks by market type
 */
export function groupStocksByMarket(stocks: StockInfo[]): Record<MarketType, StockInfo[]> {
  const groups: Record<MarketType, StockInfo[]> = {
    prime: [],
    standard: [],
    growth: [],
  };

  for (const stock of stocks) {
    const marketType = getMarketType(stock.marketCode);
    if (marketType) {
      groups[marketType].push(stock);
    }
  }

  return groups;
}

/**
 * Group stocks by sector
 */
export function groupStocksBySector(stocks: StockInfo[]): Record<string, StockInfo[]> {
  const groups: Record<string, StockInfo[]> = {};

  for (const stock of stocks) {
    const sectorCode = stock.sector33Code;
    if (!groups[sectorCode]) {
      groups[sectorCode] = [];
    }
    groups[sectorCode].push(stock);
  }

  return groups;
}

// ===== VALIDATION UTILITIES =====

// Re-export isValidStockCode from db/columns for consistency
export { isValidStockCode } from '../db/columns/stock-code';

/**
 * Validate sector code format
 */
export function isValidSectorCode(code: string): boolean {
  return /^\d{4}$/.test(code);
}

/**
 * Validate date range
 */
export function isValidDateRange(range: DateRange): boolean {
  return range.from <= range.to;
}

// ===== STRING UTILITIES =====

/**
 * Sanitize file path for cross-platform compatibility
 */
export function sanitizeFilePath(path: string): string {
  // Remove invalid characters and normalize path
  return path
    .replace(/[<>:"|?*]/g, '_')
    .replace(/\\/g, '/')
    .replace(/\/+/g, '/');
}

/**
 * Generate a unique filename with timestamp
 */
export function generateUniqueFilename(base: string, extension: string): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
  return `${base}_${timestamp}.${extension}`;
}

/**
 * Format file size in human readable format
 */
export function formatFileSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(1)} ${units[unitIndex]}`;
}

// ===== ARRAY UTILITIES =====

/**
 * Split array into chunks of specified size
 */
export function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

/**
 * Get unique values from array
 */
export function getUniqueValues<T>(array: T[]): T[] {
  return Array.from(new Set(array));
}

/**
 * Remove duplicates from array based on key function
 */
export function removeDuplicatesBy<T, K>(array: T[], keyFn: (item: T) => K): T[] {
  const seen = new Set<K>();
  return array.filter((item) => {
    const key = keyFn(item);
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

// ===== PERFORMANCE UTILITIES =====

/**
 * Sleep for specified milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Measure execution time of a function
 */
export async function measureTime<T>(
  operation: () => Promise<T>,
  label?: string
): Promise<{ result: T; timeMs: number }> {
  const startTime = Date.now();
  const result = await operation();
  const timeMs = Date.now() - startTime;

  if (label) {
    console.log(`${label}: ${timeMs}ms`);
  }

  return { result, timeMs };
}

/**
 * Create a debounced function
 */
export function debounce<T extends (...args: unknown[]) => void>(func: T, waitMs: number): T {
  let timeoutId: NodeJS.Timeout | null = null;

  return ((...args: Parameters<T>) => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }

    timeoutId = setTimeout(() => {
      func(...args);
      timeoutId = null;
    }, waitMs);
  }) as T;
}

// ===== ERROR UTILITIES =====

/**
 * Safe JSON stringify that handles circular references
 */
export function safeJsonStringify(obj: unknown, indent?: number): string {
  const seen = new WeakSet();
  return JSON.stringify(
    obj,
    (_key, value) => {
      if (typeof value === 'object' && value !== null) {
        if (seen.has(value)) {
          return '[Circular]';
        }
        seen.add(value);
      }
      return value;
    },
    indent
  );
}

/**
 * Extract error message from various error types
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return String(error);
}

/**
 * Create a detailed error summary
 */
export function createErrorSummary(errors: string[]): string {
  if (errors.length === 0) return 'No errors';
  if (errors.length === 1) return errors[0] || 'Unknown error';

  return `${errors.length} errors:\n${errors.map((err, i) => `  ${i + 1}. ${err}`).join('\n')}`;
}

// ===== TYPE GUARDS =====

/**
 * Check if value is not null or undefined
 */
export function isDefined<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}

/**
 * Check if string is not empty
 */
export function isNonEmptyString(value: string | null | undefined): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Check if array is not empty
 */
export function isNonEmptyArray<T>(value: T[] | null | undefined): value is T[] {
  return Array.isArray(value) && value.length > 0;
}

// ===== SAMPLING UTILITIES =====

/**
 * Fisher-Yates shuffle algorithm for random array shuffling
 */
export function shuffleArray<T>(array: T[], seed?: number): T[] {
  const result = [...array];
  const rng = seed !== undefined ? createSeededRandom(seed) : Math.random;

  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    const itemI = result[i];
    const itemJ = result[j];

    if (itemI !== undefined && itemJ !== undefined) {
      result[i] = itemJ;
      result[j] = itemI;
    }
  }

  return result;
}

/**
 * Simple seeded random number generator (Linear Congruential Generator)
 * Provides reproducible random numbers for testing
 */
function createSeededRandom(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state * 1664525 + 1013904223) % 4294967296;
    return state / 4294967296;
  };
}

/**
 * Random sample from array using Fisher-Yates algorithm
 * @param array - Array to sample from
 * @param size - Number of items to sample
 * @param seed - Optional seed for reproducible sampling
 * @returns Random sample of specified size
 */
export function randomSample<T>(array: T[], size: number, seed?: number): T[] {
  if (size <= 0) return [];
  if (size >= array.length) return [...array];

  const shuffled = shuffleArray(array, seed);
  return shuffled.slice(0, size);
}
