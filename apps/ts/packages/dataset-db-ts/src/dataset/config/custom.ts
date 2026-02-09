/**
 * Dataset V2 - Custom Configuration Functions
 * Functions for creating custom configurations (V1 migration)
 */

import type { CustomConfigOptions, DatasetConfig } from '../types';
import { createConfig } from './builder';

/**
 * Create custom configuration with flexible options - V1 equivalent: createCustomConfig
 */
export function createCustomConfig(
  outputPath: string,
  startDate: Date,
  endDate: Date,
  options?: CustomConfigOptions
): DatasetConfig {
  if (!outputPath || typeof outputPath !== 'string') {
    throw new Error('Output path is required and must be a string');
  }

  const config = createConfig({
    outputPath,
    startDate,
    endDate,
    includeMargin: options?.includeMargin ?? false,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    markets: options?.markets ?? ['prime', 'standard', 'growth'],
    marketCapFilter: options?.minMarketCap,
    maxStocks: options?.maxStockCount,
  });

  return config;
}

/**
 * Create configuration for specific date range with preset options - V1 equivalent: createForDateRangeConfig
 */
export function createForDateRangeConfig(
  outputPath: string,
  startDate: Date,
  endDate: Date,
  preset: 'full' | 'prime' | 'testing' = 'full'
): DatasetConfig {
  if (!outputPath || typeof outputPath !== 'string') {
    throw new Error('Output path is required and must be a string');
  }

  const baseConfig: Partial<DatasetConfig> = {
    outputPath,
    startDate,
    endDate,
    includeMargin: false,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
  };

  switch (preset) {
    case 'prime':
      return createConfig({
        ...baseConfig,
        markets: ['prime'],
        marketCapFilter: 10_000_000_000, // 100億円以上
      });
    case 'testing':
      return createConfig({
        ...baseConfig,
        markets: ['standard'],
        maxStocks: 1,
      });
    default: // 'full'
      return createConfig({
        ...baseConfig,
        markets: ['prime', 'standard', 'growth'],
      });
  }
}
