/**
 * Dataset - Basic Market Coverage Presets
 * fullMarket, primeMarket, standardMarket, and growthMarket presets
 *
 * Note: Date range is not specified - JQuants API automatically returns
 * the maximum available data based on subscription plan (e.g., 10 years for Standard).
 */

import type { DatasetConfig } from '../../types';
import { createConfig } from '../builder';

/**
 * Full market dataset - V1 equivalent: fullMarket
 * Complete market coverage for comprehensive analysis
 */
export function fullMarket(outputPath = './full-market.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime', 'standard', 'growth'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
  });
}

/**
 * Prime market only - V1 equivalent: primeMarket
 * High-quality large-cap stocks only
 */
export function primeMarket(outputPath = './prime-market.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['prime'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
    marketCapFilter: 10_000_000_000, // 100億円以上
  });
}

/**
 * Standard market only
 * Mid-cap stocks listed on TSE Standard market
 */
export function standardMarket(outputPath = './standard-market.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['standard'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
  });
}

/**
 * Growth market only
 * Emerging/growth stocks listed on TSE Growth market
 */
export function growthMarket(outputPath = './growth-market.db'): DatasetConfig {
  return createConfig({
    outputPath,
    markets: ['growth'],
    includeMargin: true,
    includeStatements: true,
    includeTOPIX: true,
    includeSectorIndices: true,
  });
}
